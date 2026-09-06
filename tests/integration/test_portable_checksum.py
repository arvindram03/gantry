# SPDX-License-Identifier: Apache-2.0
"""Two implementations of one checksum, checked against each other.

`verification.checksum` renders SQL and lets PostgreSQL compute it.
`verification.portable` computes the same thing in Python, for targets that have
no engine to send SQL to.

They must agree exactly, and "exactly" is not something to establish by reading
the PostgreSQL manual. Every value below is one I expected to disagree — the
scale of a numeric, a timestamp's microseconds, a float's last bits, a null, a
byte string, a unicode character that is several bytes long.

If this file fails, verification against a non-SQL target is reporting
corruption that is not there, or agreeing about data that differs.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.state.database import create_engine, transaction
from gantry.verification.checksum import compute_checksum
from gantry.verification.portable import checksum as portable_checksum
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TABLE = "public.portable_probe"

COLUMNS = (
    "id bigint PRIMARY KEY",
    "label text",
    "amount numeric(20,6)",
    "ratio double precision",
    "flag boolean",
    "seen_at timestamp with time zone",
    "day date",
    "payload bytea",
)


@pytest.fixture
async def source() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(SOURCE_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await connection.execute(text(f"CREATE TABLE {TABLE} ({', '.join(COLUMNS)})"))
        yield engine
    finally:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
        await engine.dispose()


ROWS = [
    # Trailing zeroes: 1.50 and 1.5 are the same number, different strings.
    "(1, 'plain', 1.50, 0.5, true, '2026-09-05T12:34:56.123456Z', '2026-09-05', '\\x00ff')",
    # An integral numeric, where trim_scale gives `2` and not `2.` or `2E+0`.
    "(2, 'integral', 2.000000, 1.0, false, '2026-01-01T00:00:00Z', '2026-01-01', '\\x')",
    # Nulls in every nullable column at once.
    "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    # Microseconds that must not be truncated, and a non-UTC offset.
    "(4, 'micros', 0.000001, 1e-7, true, "
    "'2026-06-30T23:59:59.000009+05:30', '2026-06-30', '\\xdeadbeef')",
    # Multi-byte text, and the separator's neighbours.
    "(5, 'héllo — ünicode', -12345.678901, -0.25, false, "
    "'2026-12-31T00:00:00Z', '2026-12-31', '\\x1f')",
    # A large numeric that still fits, negative zero, and an empty string.
    "(6, '', 99999999999999.999999, -0.0, true, "
    "'2027-02-28T06:00:00Z', '2027-02-28', '\\x0102030405')",
]


async def seed(engine: AsyncEngine) -> None:
    async with transaction(engine) as connection:
        await connection.execute(text(f"INSERT INTO {TABLE} VALUES {', '.join(ROWS)}"))


async def manifest_of(engine: AsyncEngine):  # type: ignore[no-untyped-def]
    return {m.name: m for m in await PostgresSourceAdapter(engine).discover()}[TABLE]


async def read_rows(engine: AsyncEngine, manifest) -> list[dict[str, object]]:  # type: ignore[no-untyped-def]
    names = ", ".join(f'"{f.name}"' for f in manifest.dataset_schema.fields)
    async with transaction(engine) as connection:
        result = await connection.execute(text(f"SELECT {names} FROM {TABLE}"))
        return [dict(row._mapping) for row in result]


async def test_python_and_sql_agree_on_awkward_values(source: AsyncEngine) -> None:
    """The whole basis of verifying a target that has no engine."""
    await seed(source)
    manifest = await manifest_of(source)

    in_engine = await compute_checksum(source, manifest, TABLE, predicate="TRUE", params={})
    in_python = portable_checksum(manifest, await read_rows(source, manifest))

    assert in_python.rows == in_engine.rows
    assert in_python.checksum == in_engine.checksum, (
        "the two implementations of one checksum disagree; "
        "verification against a non-SQL target cannot be trusted"
    )


async def test_they_disagree_when_the_data_differs(source: AsyncEngine) -> None:
    """A checksum that always agrees proves nothing."""
    await seed(source)
    manifest = await manifest_of(source)
    rows = await read_rows(source, manifest)

    in_engine = await compute_checksum(source, manifest, TABLE, predicate="TRUE", params={})
    rows[0] = {**rows[0], "label": "tampered"}
    assert portable_checksum(manifest, rows).checksum != in_engine.checksum


async def test_order_does_not_matter(source: AsyncEngine) -> None:
    """Rows in an Iceberg table have no order worth speaking of, so the
    checksum must not depend on one."""
    await seed(source)
    manifest = await manifest_of(source)
    rows = await read_rows(source, manifest)
    assert (
        portable_checksum(manifest, rows).checksum
        == portable_checksum(manifest, list(reversed(rows))).checksum
    )
