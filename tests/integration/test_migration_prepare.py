# SPDX-License-Identifier: Apache-2.0
"""Prepare refuses before anything moves.

The claim under test is about *timing* as much as correctness. A schema
mismatch found in Prepare costs a minute; the same mismatch found at cutover
costs the snapshot, the catch-up, and whatever was scheduled around them. So
the assertion that matters is not only "it refused" but "the target has exactly
as many rows afterwards as before".

Requires: make dev-up && uv run alembic upgrade head.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.lifecycle.migration import MigrationState
from gantry.migration.model import Migration
from gantry.migration.prepare import PrepareCheck, PrepareReport, prepare
from gantry.migration.service import MigrationService, PrepareRefusedError
from gantry.state.database import create_engine, transaction
from gantry.state.migrations import MigrationStore
from gantry.state.operations import OperationStore
from gantry.state.tables import migration_transitions, migrations
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncEngine

from .conftest import clear_operation, ensure_source_scale

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)

NAME = "prepare-test-migration"
MOVEMENT = "prepare-test-movement"
SOURCE_TABLE = "public.prepare_src"
TARGET_TABLE = "public.prepare_dst"


@pytest.fixture
async def engines() -> AsyncIterator[tuple[AsyncEngine, AsyncEngine, AsyncEngine]]:
    source = create_engine(SOURCE_URL)
    target = create_engine(TARGET_URL)
    meta = create_engine(META_URL)

    async def clear() -> None:
        for engine, table in ((source, SOURCE_TABLE), (target, TARGET_TABLE)):
            async with transaction(engine) as connection:
                await connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
        async with transaction(meta) as connection:
            await connection.execute(
                delete(migration_transitions).where(migration_transitions.c.migration == NAME)
            )
            await connection.execute(delete(migrations).where(migrations.c.name == NAME))
        await clear_operation(meta, MOVEMENT)

    try:
        await clear()
        async with transaction(source) as connection:
            await connection.execute(
                text(
                    f"CREATE TABLE {SOURCE_TABLE} ("
                    "  id bigint PRIMARY KEY,"
                    "  amount numeric(12,2) NOT NULL,"
                    "  region text,"
                    "  note text)"
                )
            )
            await connection.execute(
                text(
                    f"INSERT INTO {SOURCE_TABLE} "
                    f"SELECT g, g, 'eu', 'n' FROM generate_series(1,50) g"
                )
            )
            await connection.execute(text(f"ANALYZE {SOURCE_TABLE}"))
        yield source, target, meta
    finally:
        await clear()
        for engine in (source, target, meta):
            await engine.dispose()


async def create_target(target: AsyncEngine, ddl: str) -> None:
    async with transaction(target) as connection:
        await connection.execute(text(f"CREATE TABLE {TARGET_TABLE} ({ddl})"))


async def run_prepare(
    engines: tuple[AsyncEngine, AsyncEngine, AsyncEngine], *, needs_replication: bool = False
) -> PrepareReport:
    source, target, _ = engines
    manifests = {m.name: m for m in await PostgresSourceAdapter(source).discover()}
    existing = {m.name: m for m in await PostgresSourceAdapter(target).discover()}
    return await prepare(
        NAME,
        source=source,
        target=target,
        manifests={SOURCE_TABLE: manifests[SOURCE_TABLE]},
        targets={SOURCE_TABLE: TARGET_TABLE},
        target_manifests=existing,
        needs_replication=needs_replication,
    )


async def test_a_matching_target_is_ready(engines: tuple[AsyncEngine, ...]) -> None:
    await create_target(
        engines[1],
        "id bigint PRIMARY KEY, amount numeric(12,2) NOT NULL, region text, note text",
    )
    report = await run_prepare(engines)  # type: ignore[arg-type]
    assert report.ready, report.describe()


async def test_a_narrower_column_is_refused_by_name(engines: tuple[AsyncEngine, ...]) -> None:
    await create_target(
        engines[1],
        "id bigint PRIMARY KEY, amount numeric(6,2) NOT NULL, region text, note text",
    )
    report = await run_prepare(engines)  # type: ignore[arg-type]

    assert not report.ready
    named = " ".join(failure.describe() for failure in report.failures)
    assert "amount" in named, "the refusal must name the column"
    assert "truncate" in named, "and say what would happen to it"


async def test_every_offending_column_is_named_in_one_pass(
    engines: tuple[AsyncEngine, ...],
) -> None:
    """An operator fixing one column per round trip is what this avoids."""
    await create_target(
        engines[1], "id bigint PRIMARY KEY, amount numeric(6,2) NOT NULL, region text NOT NULL"
    )
    report = await run_prepare(engines)  # type: ignore[arg-type]

    named = " ".join(f.describe() for f in report.failures)
    assert "amount" in named, "narrowed column"
    assert "region" in named, "nullability"
    assert "note" in named, "missing column"


async def test_a_missing_target_is_reported_as_something_to_create(
    engines: tuple[AsyncEngine, ...],
) -> None:
    """Not a refusal — the adapter creates it. Said out loud because 'about to
    create a table' is something an operator may want to stop."""
    report = await run_prepare(engines)  # type: ignore[arg-type]
    assert report.ready
    assert report.to_create == (TARGET_TABLE,)


async def test_a_snapshot_migration_does_not_demand_logical_decoding(
    engines: tuple[AsyncEngine, ...],
) -> None:
    """Refusing a snapshot for want of a replication slot would refuse
    migrations that are perfectly fine."""
    await create_target(
        engines[1],
        "id bigint PRIMARY KEY, amount numeric(12,2) NOT NULL, region text, note text",
    )
    report = await run_prepare(engines, needs_replication=False)  # type: ignore[arg-type]
    assert not any(
        f.check in (PrepareCheck.REPLICATION_CONFIGURED, PrepareCheck.REPLICATION_SLOTS_AVAILABLE)
        for f in report.failures
    )


async def test_an_unreachable_database_stops_before_comparing_schemas(
    engines: tuple[AsyncEngine, ...],
) -> None:
    """There is nothing to say about a schema on a database that will not
    answer, and saying it anyway buries the real problem."""
    source, _target, _meta = engines
    unreachable = create_engine("postgresql+asyncpg://gantry:gantry@localhost:1/nope")
    try:
        report = await prepare(
            NAME,
            source=source,
            target=unreachable,
            manifests={},
            targets={},
            needs_replication=False,
        )
        assert not report.ready
        assert [f.check for f in report.failures] == [PrepareCheck.TARGET_REACHABLE]
    finally:
        await unreachable.dispose()


async def test_a_refused_migration_moves_no_rows_and_returns_to_planned(
    engines: tuple[AsyncEngine, ...],
) -> None:
    """The exit criterion for the phase, stated as an assertion."""
    source, target, meta = engines
    await ensure_source_scale(source, orders=1_000, customers=1_000)
    await create_target(target, "id bigint PRIMARY KEY, amount numeric(6,2) NOT NULL")

    async with transaction(target) as connection:
        before = (
            await connection.execute(text(f"SELECT count(*) FROM {TARGET_TABLE}"))
        ).scalar_one()

    service = MigrationService(migrations=MigrationStore(meta), operations=OperationStore(meta))
    migration = Migration(name=NAME, movements=(MOVEMENT,), movement_specs={MOVEMENT: "unused"})

    ran: list[str] = []

    async def runner(name: str, /) -> object:
        ran.append(name)
        return None

    async def preparer(_: Migration, /) -> PrepareReport:
        return await run_prepare(engines)  # type: ignore[arg-type]

    with pytest.raises(PrepareRefusedError) as caught:
        await service.run(migration, runner=runner, preparer=preparer)

    assert ran == [], "nothing may run once prepare has refused"
    assert not caught.value.report.ready

    async with transaction(target) as connection:
        after = (
            await connection.execute(text(f"SELECT count(*) FROM {TARGET_TABLE}"))
        ).scalar_one()
    assert after == before, "prepare refused, so no row may have moved"

    status = await service.status(NAME)
    assert status.state is MigrationState.PLANNED, "repairable input, not a dead end"

    last = (await MigrationStore(meta).history(NAME))[-1]
    assert "prepare refused" in last.reason
    assert last.evidence is not None, "the refusal must survive into the trail"
