# SPDX-License-Identifier: Apache-2.0
"""A Migration drives Movements it does not reimplement.

This is the plan's actual hypothesis under test. RFC 0 §5.2 says migration is a
workflow composed from Movements rather than a fundamental abstraction. If that
is right, the Migration below should be able to run a real Movement while
knowing nothing about partitions, checkpoints, engines or plans — and its own
state should be *derived* from what the Movement reports rather than tracked
alongside it.

Requires: make dev-up && uv run alembic upgrade head.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from gantry.core.operation import OperationState, OperationType
from gantry.lifecycle.migration import MigrationState
from gantry.lifecycle.states import ActorKind
from gantry.migration.model import Migration
from gantry.migration.reconcile import ReconciliationReport
from gantry.migration.service import MigrationService
from gantry.state.database import create_engine, transaction
from gantry.state.migrations import MigrationStore
from gantry.state.operations import OperationStore
from gantry.state.tables import migration_transitions, migrations
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine

from .conftest import clear_operation

pytestmark = pytest.mark.integration

META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
NAME = "composition-test-migration"
MOVEMENT = "composition-test-movement"


@pytest.fixture
async def meta() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(META_URL)

    async def clear() -> None:
        async with transaction(engine) as connection:
            await connection.execute(
                delete(migration_transitions).where(migration_transitions.c.migration == NAME)
            )
            await connection.execute(delete(migrations).where(migrations.c.name == NAME))
        await clear_operation(engine, MOVEMENT)

    try:
        await clear()
        yield engine
    finally:
        await clear()
        await engine.dispose()


def service(meta: AsyncEngine) -> MigrationService:
    return MigrationService(migrations=MigrationStore(meta), operations=OperationStore(meta))


def migration() -> Migration:
    return Migration(
        name=NAME,
        movements=(MOVEMENT,),
        movement_specs={MOVEMENT: "examples/postgres-to-postgres/movement.yaml"},
    )


async def test_planning_walks_the_workflow_to_planned(meta: AsyncEngine) -> None:
    record = await service(meta).plan(migration())
    assert record.state is MigrationState.PLANNED

    history = await MigrationStore(meta).history(NAME)
    assert [entry.to_state for entry in history] == [
        MigrationState.DISCOVERING,
        MigrationState.PLANNED,
    ]


async def test_planning_twice_is_idempotent(meta: AsyncEngine) -> None:
    """Re-applying a spec must not rewind a workflow already underway."""
    migrations_service = service(meta)
    await migrations_service.plan(migration())
    again = await migrations_service.plan(migration())

    assert again.state is MigrationState.PLANNED
    assert len(await MigrationStore(meta).history(NAME)) == 2, "no second walk through DRAFT"


async def test_the_workflow_reaches_verifying_by_driving_a_movement(
    meta: AsyncEngine,
) -> None:
    """The composition claim, exercised.

    The runner stands in for whatever knows how to run a Movement — here it
    advances the Operation the way a real run does. What matters is that the
    Migration never touches a plan, a partition or a checkpoint to get there.
    """
    operations = OperationStore(meta)
    ran: list[str] = []

    async def runner(name: str) -> object:
        ran.append(name)
        await operations.ensure(name, OperationType.MOVEMENT, plan_version=1)
        for state in (
            OperationState.PLANNED,
            OperationState.GENERATED,
            OperationState.VALIDATED,
            OperationState.EXECUTING,
            OperationState.VERIFYING,
            OperationState.COMPLETED,
        ):
            await operations.transition(
                name, state, actor=ActorKind.RUNTIME, reason="test movement", plan_version=1
            )
        return None

    status = await service(meta).run(migration(), runner=runner)

    assert ran == [MOVEMENT], "the Migration must drive the Movement, not run it itself"
    assert status.state is MigrationState.VERIFYING
    assert status.all_movements_complete

    trail = [entry.to_state for entry in await MigrationStore(meta).history(NAME)]
    assert trail == [
        MigrationState.DISCOVERING,
        MigrationState.PLANNED,
        MigrationState.PREPARING,
        MigrationState.SNAPSHOTTING,
        MigrationState.CATCHING_UP,
        MigrationState.VERIFYING,
    ]


async def test_movement_state_is_read_not_mirrored(meta: AsyncEngine) -> None:
    """A Movement that finishes while nobody is watching shows up the moment
    anyone asks. Two records of one fact drift, and the one an operator
    happens to read decides what they believe."""
    operations = OperationStore(meta)
    migration_service = service(meta)
    await migration_service.plan(migration())

    before = await migration_service.status(NAME)
    assert before.movements[0].state is None, "not started yet"

    await operations.ensure(MOVEMENT, OperationType.MOVEMENT, plan_version=7)
    await operations.transition(
        MOVEMENT,
        OperationState.PLANNED,
        actor=ActorKind.RUNTIME,
        reason="elsewhere",
        plan_version=7,
    )

    after = await migration_service.status(NAME)
    assert after.movements[0].state is OperationState.PLANNED
    assert after.movements[0].plan_version == 7


async def test_a_failing_movement_fails_the_migration(meta: AsyncEngine) -> None:
    async def runner(name: str) -> object:
        raise RuntimeError("target refused the connection")

    migration_service = service(meta)
    with pytest.raises(RuntimeError, match="target refused"):
        await migration_service.run(migration(), runner=runner)

    status = await migration_service.status(NAME)
    assert status.state is MigrationState.FAILED

    last = (await MigrationStore(meta).history(NAME))[-1]
    assert "target refused" in last.reason, "the cause must survive into the trail"


async def test_a_movement_that_ends_badly_stops_the_workflow(meta: AsyncEngine) -> None:
    """The runner returning without raising is not proof of success — the
    Operation's own state is."""
    operations = OperationStore(meta)

    async def runner(name: str) -> object:
        await operations.ensure(name, OperationType.MOVEMENT, plan_version=1)
        await operations.transition(
            name, OperationState.FAILED, actor=ActorKind.RUNTIME, reason="it did not work"
        )
        return None

    status = await service(meta).run(migration(), runner=runner)
    assert status.any_movement_failed
    assert status.state is MigrationState.FAILED


async def test_reconciliation_agreeing_opens_the_door_to_cutover(
    meta: AsyncEngine,
) -> None:
    """VERIFYING is not a resting place. Agreement moves the workflow to
    READY_FOR_CUTOVER, which is the only state the gates can be evaluated from.
    """
    from gantry.migration.reconcile import LayerOutcome, LayerResult, ReconciliationLayer

    operations = OperationStore(meta)

    async def runner(name: str, /) -> object:
        await operations.ensure(name, OperationType.MOVEMENT, plan_version=1)
        for state in (
            OperationState.PLANNED,
            OperationState.GENERATED,
            OperationState.VALIDATED,
            OperationState.EXECUTING,
            OperationState.VERIFYING,
            OperationState.COMPLETED,
        ):
            await operations.transition(
                name, state, actor=ActorKind.RUNTIME, reason="test", plan_version=1
            )
        return None

    async def agreeing(_: Migration, /) -> list[ReconciliationReport]:
        report = ReconciliationReport(dataset="public.orders", target="public.orders")
        report.layers.append(
            LayerResult(
                layer=ReconciliationLayer.COUNT,
                outcome=LayerOutcome.AGREED,
                detail="10 rows on both sides",
            )
        )
        return [report]

    status = await service(meta).run(migration(), runner=runner, reconciler=agreeing)

    assert status.state is MigrationState.READY_FOR_CUTOVER
    assert status.reconciliation and status.reconciliation[0].agreed


async def test_reconciliation_disagreeing_returns_to_catching_up(
    meta: AsyncEngine,
) -> None:
    """Not FAILED. Under live writes a disagreement is more often a stream
    that has not finished applying than data that is wrong, and the runtime
    cannot tell those apart from one reading. Retrying is cheap; failing the
    migration is not reversible."""
    from gantry.migration.reconcile import LayerOutcome, LayerResult, ReconciliationLayer

    operations = OperationStore(meta)

    async def runner(name: str, /) -> object:
        await operations.ensure(name, OperationType.MOVEMENT, plan_version=1)
        for state in (
            OperationState.PLANNED,
            OperationState.GENERATED,
            OperationState.VALIDATED,
            OperationState.EXECUTING,
            OperationState.VERIFYING,
            OperationState.COMPLETED,
        ):
            await operations.transition(
                name, state, actor=ActorKind.RUNTIME, reason="test", plan_version=1
            )
        return None

    async def disagreeing(_: Migration, /) -> list[ReconciliationReport]:
        report = ReconciliationReport(dataset="public.orders", target="public.orders")
        report.layers.append(
            LayerResult(
                layer=ReconciliationLayer.COUNT,
                outcome=LayerOutcome.DISAGREED,
                detail="source 10, target 9 (+1)",
            )
        )
        return [report]

    status = await service(meta).run(migration(), runner=runner, reconciler=disagreeing)

    assert status.state is MigrationState.CATCHING_UP
    assert not status.reconciliation[0].agreed

    last = (await MigrationStore(meta).history(NAME))[-1]
    assert "disagreeing" in last.reason
    assert last.evidence is not None, "what disagreed must survive into the trail"
