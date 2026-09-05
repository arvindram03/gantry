"""Applying change events to a target.

Two guarantees live here, and both are enforced in SQL rather than by careful
sequencing in Python:

**Stale writes are rejected, not applied.** Every write carries the source LSN
it came from, and the target only accepts a change newer than what it holds.
Delivery order therefore stops mattering: an event that arrives late is
compared against the row it would overwrite and declined.

**Deleted rows stay deleted.** A delete removes the row, which removes the LSN
the guard compares against - so an out-of-order insert would resurrect it. A
tombstone records the position at which a key was deleted, and a change older
than the tombstone is refused.

Both are last-writer-wins by source position. Neither depends on events
arriving in order, and neither depends on them arriving once.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from gantry.core.changes import ChangeEvent, ChangeOperation
from gantry.core.commit import CommitResult
from gantry.core.dataset import DatasetManifest
from gantry.movement.routing import order_by_key
from gantry.state.database import transaction
from gantry.verification.sql import column_type, qualified, quote

# The column carrying the source position on the target. Stale-write rejection
# needs somewhere to compare against, so a target without it cannot be
# protected - see ApplyError below.
LSN_COLUMN = "source_lsn"

# Tombstones live in the target database, not the metadata store. They have to
# be written in the same transaction as the delete they describe: a crash
# between deleting a row and recording that it was deleted leaves nothing for a
# later out-of-order insert to be checked against, and the row comes back. That
# atomicity is worth a Gantry-owned side table in the target, which already
# carries a Gantry-owned source_lsn column.
TOMBSTONE_TABLE = "gantry_tombstones"


class ApplyError(Exception):
    """A change could not be applied for a reason retrying will not fix."""


@dataclass
class ApplyReport:
    """What an apply pass did, and what it refused to do."""

    applied: int = 0
    deleted: int = 0
    # Events declined because the target already held something newer. This is
    # the number that shows delivery order is not being trusted.
    rejected_stale: int = 0
    dead_lettered: int = 0
    dead_letters: list[tuple[ChangeEvent, str]] = field(default_factory=list)
    committed_at: datetime | None = None

    @property
    def seen(self) -> int:
        return self.applied + self.deleted + self.rejected_stale + self.dead_lettered

    def to_commit(self) -> CommitResult:
        return CommitResult(
            rows_inserted=self.applied,
            rows_unchanged=0,
            rows_rejected_stale=self.rejected_stale,
            committed_at=self.committed_at or datetime.now(UTC),
        )

    def describe(self) -> str:
        return (
            f"{self.applied} applied, {self.deleted} deleted, "
            f"{self.rejected_stale} stale rejected, {self.dead_lettered} dead-lettered"
        )


class CDCApplier:
    """Applies change events to one dataset's target."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        operation: str,
        manifest: DatasetManifest,
        target: str,
    ) -> None:
        self._engine = engine
        self._operation = operation
        self._manifest = manifest
        self._target = target
        self._require_lsn_column()

    def _require_lsn_column(self) -> None:
        if self._manifest.dataset_schema.field(LSN_COLUMN) is None:
            raise ApplyError(
                f"dataset {self._manifest.name!r} has no {LSN_COLUMN!r} column; "
                f"stale-write rejection has nothing to compare against"
            )

    async def ensure_tombstones(self) -> None:
        """Create the tombstone table in the target if it is not there.

        Part of Prepare, and idempotent, because Prepare runs again on every
        restart.
        """
        schema = self._target.rsplit(".", 1)[0] if "." in self._target else "public"
        async with transaction(self._engine) as connection:
            await connection.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {quote(schema)}.{quote(TOMBSTONE_TABLE)} ("
                    f"  operation text NOT NULL,"
                    f"  dataset text NOT NULL,"
                    f"  key_text text NOT NULL,"
                    f"  source_lsn bigint NOT NULL,"
                    f"  deleted_at timestamptz NOT NULL,"
                    f"  PRIMARY KEY (operation, dataset, key_text))"
                )
            )

    @property
    def _tombstones(self) -> str:
        schema = self._target.rsplit(".", 1)[0] if "." in self._target else "public"
        return f"{quote(schema)}.{quote(TOMBSTONE_TABLE)}"

    async def apply(self, events: Sequence[ChangeEvent]) -> ApplyReport:
        """Apply a batch, in source order within each key.

        The whole batch commits together. A partial batch would leave the
        target holding some of a transaction's changes and not others, which no
        later replay can distinguish from the source having looked that way.
        """
        report = ApplyReport()
        if not events:
            report.committed_at = datetime.now(UTC)
            return report

        async with transaction(self._engine) as connection:
            for event in order_by_key(events):
                try:
                    await self._apply_one(connection, event, report)
                except ApplyError as error:
                    report.dead_lettered += 1
                    report.dead_letters.append((event, str(error)))
            report.committed_at = datetime.now(UTC)
        return report

    async def _apply_one(
        self, connection: AsyncConnection, event: ChangeEvent, report: ApplyReport
    ) -> None:
        if event.dataset != self._manifest.name:
            raise ApplyError(
                f"event for {event.dataset!r} routed to applier for {self._manifest.name!r}"
            )
        if event.operation is ChangeOperation.TRUNCATE:
            raise ApplyError("truncate is not applied automatically; it needs an operator")

        if event.operation is ChangeOperation.DELETE:
            await self._delete(connection, event, report)
            return
        await self._upsert(connection, event, report)

    async def _upsert(
        self, connection: AsyncConnection, event: ChangeEvent, report: ApplyReport
    ) -> None:
        if await self._is_tombstoned(connection, event):
            # The row was deleted at a position at or after this change.
            report.rejected_stale += 1
            return

        after = event.after or {}
        names = [field.name for field in self._manifest.dataset_schema.fields]
        missing = [name for name in names if name not in after and name != LSN_COLUMN]
        if missing:
            raise ApplyError(f"event is missing columns the target requires: {missing}")

        values = {name: after.get(name) for name in names}
        values[LSN_COLUMN] = event.source_lsn

        keys = list(self._manifest.dataset_schema.keys)
        updatable = [name for name in names if name not in keys]
        columns = ", ".join(quote(name) for name in names)
        placeholders = ", ".join(f":{name}" for name in names)
        assignments = ", ".join(f"{quote(n)} = EXCLUDED.{quote(n)}" for n in updatable)
        table = qualified(self._target)

        # The guard is the whole point: a change is applied only if it is newer
        # than what the target already holds. An equal LSN is a duplicate
        # delivery and is also declined.
        statement = text(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT ({', '.join(quote(key) for key in keys)}) DO UPDATE SET {assignments} "
            f"WHERE {table}.{quote(LSN_COLUMN)} < EXCLUDED.{quote(LSN_COLUMN)} "
            f"RETURNING 1"
        )
        result = await connection.execute(statement, values)
        if result.rowcount:
            report.applied += 1
        else:
            report.rejected_stale += 1

    async def _delete(
        self, connection: AsyncConnection, event: ChangeEvent, report: ApplyReport
    ) -> None:
        keys = list(self._manifest.dataset_schema.keys)
        table = qualified(self._target)
        # Event keys are type-agnostic strings, so they are re-typed from the
        # schema and bound as text first - the driver infers a parameter's type
        # from its cast target, so casting straight to bigint would demand an
        # int it has not been given.
        predicate = " AND ".join(
            f"{quote(key)} = CAST(CAST(:{key} AS text) AS {column_type(self._manifest, key)})"
            for key in keys
        )
        params: dict[str, object] = {key: event.key.get(key) for key in keys}
        params["lsn"] = event.source_lsn

        result = await connection.execute(
            text(f"DELETE FROM {table} WHERE {predicate} AND {quote(LSN_COLUMN)} <= :lsn"),
            params,
        )

        # The tombstone is recorded whether or not a row was there to delete:
        # the delete may simply have arrived before the insert it supersedes.
        await self._record_tombstone(connection, event)
        if result.rowcount:
            report.deleted += 1
        else:
            report.rejected_stale += 1

    async def _is_tombstoned(self, connection: AsyncConnection, event: ChangeEvent) -> bool:
        row = (
            await connection.execute(
                text(
                    f"SELECT source_lsn FROM {self._tombstones} "
                    f"WHERE operation = :operation AND dataset = :dataset AND key_text = :key"
                ),
                {
                    "operation": self._operation,
                    "dataset": self._manifest.name,
                    "key": event.key_text,
                },
            )
        ).one_or_none()
        return row is not None and int(row.source_lsn) >= event.source_lsn

    async def _record_tombstone(self, connection: AsyncConnection, event: ChangeEvent) -> None:
        await connection.execute(
            text(
                f"INSERT INTO {self._tombstones} "
                f"  (operation, dataset, key_text, source_lsn, deleted_at)"
                f" VALUES (:operation, :dataset, :key, :lsn, :at)"
                f" ON CONFLICT (operation, dataset, key_text) DO UPDATE"
                f" SET source_lsn = GREATEST("
                f"       {self._tombstones}.source_lsn, EXCLUDED.source_lsn),"
                f"     deleted_at = EXCLUDED.deleted_at"
                f" WHERE {self._tombstones}.source_lsn < EXCLUDED.source_lsn"
            ),
            {
                "operation": self._operation,
                "dataset": self._manifest.name,
                "key": event.key_text,
                "lsn": event.source_lsn,
                "at": datetime.now(UTC),
            },
        )


def event_payload(event: ChangeEvent) -> dict[str, object]:
    """A dead-letter payload complete enough to replay from."""
    decoded: dict[str, object] = json.loads(event.model_dump_json())
    return decoded
