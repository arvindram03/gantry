# SPDX-License-Identifier: Apache-2.0
"""PostgreSQL target adapter.

One write path, and it is not the bulk one.

`write_batch` is the correctness path: one `INSERT ... ON CONFLICT DO UPDATE`
with a `WHERE` clause that skips rows already identical, so a replayed batch
reports every row unchanged and touches nothing. It is used where the rows are
already in hand — repair, verification evidence, small corrections.

Bulk movement is not here and is deliberately not reachable from here. It is a
generated SQL script run as a job (`gantry.movement.sqljob`), which connects to
both databases itself. This process starts that job and reads what it reports;
the bytes never pass through it. An earlier version relayed COPY streams
through Python buffers, which kept the orchestration layer topologically on the
data path — technically streaming, but still the thing the design forbids.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.commit import CommitResult
from gantry.core.dataset import DatasetManifest
from gantry.state.database import transaction

ADAPTER = "postgres"

# The column carrying the source position. A snapshot stamps it with the
# position the snapshot represents; CDC stamps it with each change's own LSN.
SNAPSHOT_LSN_COLUMN = "source_lsn"

# Catalog-derived, but validated anyway: nothing interpolated into SQL text
# should be able to carry a surprise, however it got there.
_SAFE_TYPE = re.compile(r"^[a-z][a-z0-9 _]*(\(\d+(,\s*\d+)?\))?(\[\])?$")
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class UnpreparedTargetError(Exception):
    """Raised when a dataset cannot be written because it lacks a usable key."""


class PostgresTargetAdapter:
    """Writes to a PostgreSQL target."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def prepare(self, manifest: DatasetManifest, *, target: str) -> None:
        """Create the target table if it does not exist.

        Secondary indexes are deliberately not created. Every index has to be
        maintained on each of a hundred million inserts, which is the single
        largest avoidable cost in a bulk load. The primary key is the exception:
        idempotent upserts need it to detect a conflict, so it has to exist
        before the first row lands.

        The deferred indexes are the caller's to create after the snapshot, and
        before cutover - the plan knows about them because discovery recorded
        them on the manifest.
        """
        schema = manifest.dataset_schema
        if not schema.fields:
            raise UnpreparedTargetError(
                f"dataset {manifest.name!r} has no discovered fields; discover the source first"
            )
        if not schema.keys:
            raise UnpreparedTargetError(
                f"dataset {manifest.name!r} has no key; idempotent writes need a stable key"
            )

        columns = ", ".join(
            f"{_quote(field.name)} {_checked_type(field.type)}"
            + ("" if field.nullable else " NOT NULL")
            for field in schema.fields
        )
        key = ", ".join(_quote(column) for column in schema.keys)
        table = _qualified(target)

        async with transaction(self._engine) as connection:
            await connection.execute(
                text(f"CREATE TABLE IF NOT EXISTS {table} ({columns}, PRIMARY KEY ({key}))")
            )

    async def write_batch(
        self,
        manifest: DatasetManifest,
        *,
        target: str,
        rows: Sequence[Sequence[object]],
    ) -> CommitResult:
        """Upsert rows, reporting exactly what changed."""
        if not rows:
            return CommitResult(committed_at=datetime.now(UTC))

        schema = manifest.dataset_schema
        names = [field.name for field in schema.fields]
        types = [field.type for field in schema.fields]

        # One statement over column arrays rather than one statement per row.
        # executemany cannot return rows, and a row-at-a-time loop would put
        # Python back on the data path.
        columns = list(zip(*rows, strict=True))
        payload = {f"v{index}": list(column) for index, column in enumerate(columns)}

        async with transaction(self._engine) as connection:
            result = await connection.execute(
                text(_upsert_sql(target, names, types, list(schema.keys))), payload
            )
            outcomes = [row[0] for row in result.all()]

        inserted = sum(1 for outcome in outcomes if outcome)
        # Rows the statement declined to touch never come back from RETURNING,
        # which is precisely what makes a replay observable: it returns nothing.
        touched = len(outcomes)
        return CommitResult(
            rows_inserted=inserted,
            rows_updated=touched - inserted,
            rows_unchanged=len(rows) - touched,
            committed_at=datetime.now(UTC),
        )


def _upsert_sql(
    target: str, names: Sequence[str], types: Sequence[str], keys: Sequence[str]
) -> str:
    columns = ", ".join(_quote(name) for name in names)
    arrays = ", ".join(
        f"CAST(:v{index} AS {_checked_type(declared)}[])" for index, declared in enumerate(types)
    )
    return (
        f"INSERT INTO {_qualified(target)} ({columns}) "
        f"SELECT * FROM unnest({arrays}) "
        f"ON CONFLICT ({', '.join(_quote(key) for key in keys)}) "
        f"{_conflict_action(target, names, keys)} "
        f"RETURNING (xmax = 0) AS inserted"
    )


def _conflict_action(
    target: str, names: Sequence[str], keys: Sequence[str], *, extra_guard: str | None = None
) -> str:
    """What to do when a row already exists.

    The WHERE clause is what makes a replay a no-op. Without it, ON CONFLICT DO
    UPDATE rewrites every row with identical values, reporting work that did
    not happen and producing WAL that need not exist.
    """
    updatable = [name for name in names if name not in keys]
    if not updatable:
        return "DO NOTHING"
    assignments = ", ".join(f"{_quote(n)} = EXCLUDED.{_quote(n)}" for n in updatable)
    distinct = " OR ".join(
        f"{_qualified(target)}.{_quote(n)} IS DISTINCT FROM EXCLUDED.{_quote(n)}" for n in updatable
    )
    condition = distinct if extra_guard is None else f"({distinct}) AND {extra_guard}"
    return f"DO UPDATE SET {assignments} WHERE {condition}"


def _merge_sql(
    target: str,
    staging: str,
    names: Sequence[str],
    keys: Sequence[str],
    *,
    snapshot_lsn: int | None = None,
) -> str:
    """Merge staged rows into the target.

    When a snapshot position is given, every row is stamped with it and the
    merge refuses to overwrite anything newer. That is what makes the snapshot
    and the change stream safe to run at the same time: a snapshot represents
    the source as of one position, so a change that happened after that
    position must win, even if the snapshot writes it second.

    Without the guard, a partition copied slowly enough would silently undo
    changes CDC had already applied - and nothing downstream could tell,
    because the row would look consistent.
    """
    columns = ", ".join(_quote(name) for name in names)
    if snapshot_lsn is None:
        projection = columns
        guard = None
    else:
        projection = ", ".join(
            f"{snapshot_lsn}::bigint AS {_quote(name)}"
            if name == SNAPSHOT_LSN_COLUMN
            else _quote(name)
            for name in names
        )
        guard = (
            f"{_qualified(target)}.{_quote(SNAPSHOT_LSN_COLUMN)} < "
            f"EXCLUDED.{_quote(SNAPSHOT_LSN_COLUMN)}"
        )

    return (
        f"INSERT INTO {_qualified(target)} ({columns}) "
        f"SELECT {projection} FROM {staging} "
        f"ON CONFLICT ({', '.join(_quote(key) for key in keys)}) "
        f"{_conflict_action(target, names, keys, extra_guard=guard)} "
        f"RETURNING (xmax = 0) AS inserted"
    )


def _quote(identifier: str) -> str:
    if not _SAFE_IDENT.match(identifier):
        raise ValueError(f"unsafe identifier: {identifier!r}")
    return f'"{identifier}"'


def _qualified(reference: str) -> str:
    return ".".join(_quote(part) for part in reference.split("."))


def _staging_name(target: str) -> str:
    suffix = target.replace(".", "_")
    if not _SAFE_IDENT.match(suffix):
        raise ValueError(f"unsafe target name: {target!r}")
    return f"gantry_staging_{suffix}"


def _checked_type(declared: str) -> str:
    if not _SAFE_TYPE.match(declared):
        raise ValueError(f"unsupported column type: {declared!r}")
    return declared
