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
from datetime import UTC, datetime, timedelta

import pytest
from gantry.core.operation import OperationState, OperationType
from gantry.core.positions import PositionKind, SourcePosition
from gantry.lifecycle.migration import (
    IllegalMigrationTransitionError,
    MigrationState,
)
from gantry.lifecycle.states import ActorKind
from gantry.migration.gates import GateFacts, GateName
from gantry.migration.model import Migration
from gantry.migration.prepare import PrepareReport
from gantry.migration.reconcile import ReconciliationReport
from gantry.migration.service import (
    CutoverIncompleteError,
    CutoverRefusedError,
    MigrationService,
    WindowOpenError,
)
from gantry.state.database import create_engine, transaction
from gantry.state.migrations import ApprovalRequiredError, MigrationStore
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


async def ready_for_cutover(meta: AsyncEngine) -> None:
    """Walk a Migration to the one state cutover is reachable from."""
    store = MigrationStore(meta)
    await store.ensure(migration())
    for state in (
        MigrationState.DISCOVERING,
        MigrationState.PLANNED,
        MigrationState.PREPARING,
        MigrationState.SNAPSHOTTING,
        MigrationState.CATCHING_UP,
        MigrationState.VERIFYING,
        MigrationState.READY_FOR_CUTOVER,
    ):
        await store.transition(NAME, state, actor=ActorKind.RUNTIME, reason="test setup")


def green_facts() -> GateFacts:
    return GateFacts(
        partitions_total=4,
        partitions_verified=4,
        streaming=False,
        verification=(),
        target_healthy=True,
        prepare=PrepareReport(migration=NAME),
    )


async def test_cutover_is_refused_with_a_report_naming_the_failing_gate(
    meta: AsyncEngine,
) -> None:
    await ready_for_cutover(meta)
    facts = GateFacts(**{**green_facts().__dict__, "partitions_verified": 2})

    with pytest.raises(CutoverRefusedError) as caught:
        await service(meta).cutover(
            migration(), approved_by="arvind", reason="release window", facts=facts
        )

    blocking = caught.value.report.blocking
    assert [result.gate for result in blocking] == [GateName.ALL_PARTITIONS_VERIFIED]
    assert blocking[0].measured == "2/4"

    status = await service(meta).status(NAME)
    assert status.state is MigrationState.CATCHING_UP, "refused, and said what to do next"

    last = (await MigrationStore(meta).history(NAME))[-1]
    assert "cutover refused" in last.reason
    assert last.evidence is not None, "a refused cutover is a decision worth keeping"


async def test_an_agent_cannot_reach_cutover_however_it_asks(meta: AsyncEngine) -> None:
    """The access-ladder principle applied to a state machine: the refusal is
    in the transition function, not in a policy that could be relaxed."""
    await ready_for_cutover(meta)

    with pytest.raises(ApprovalRequiredError, match="operator decision"):
        await MigrationStore(meta).transition(
            NAME,
            MigrationState.CUTTING_OVER,
            actor=ActorKind.AGENT,
            actor_id="planner-1",
            reason="all the gates look green to me",
        )

    status = await service(meta).status(NAME)
    assert status.state is MigrationState.READY_FOR_CUTOVER, "the workflow did not move"


async def test_an_approved_cutover_with_green_gates_proceeds_and_is_attributed(
    meta: AsyncEngine,
) -> None:
    await ready_for_cutover(meta)

    report = await service(meta).cutover(
        migration(),
        approved_by="arvind",
        reason="release window, gates green",
        facts=green_facts(),
    )

    assert report.passed
    status = await service(meta).status(NAME)
    assert status.state is MigrationState.CUTTING_OVER

    last = (await MigrationStore(meta).history(NAME))[-1]
    assert last.actor is ActorKind.OPERATOR
    assert last.actor_id == "arvind"
    assert last.evidence is not None
    gates = last.evidence["gates"]
    assert isinstance(gates, list)
    assert len(gates) == len(GateName), "every gate recorded, not only the blocking ones"


async def test_gates_are_re_evaluated_at_cutover_not_trusted_from_earlier(
    meta: AsyncEngine,
) -> None:
    """A report from ten minutes ago is a claim about ten minutes ago."""
    await ready_for_cutover(meta)
    migration_service = service(meta)

    # `gates` is read-only and takes no approver of its own, so the dry run
    # supplies one to see what a cutover *would* find.
    dry_run = GateFacts(**{**green_facts().__dict__, "approved_by": "arvind"})
    earlier = await migration_service.gates(migration(), facts=dry_run)
    assert earlier.passed

    degraded = GateFacts(**{**green_facts().__dict__, "target_healthy": False})
    with pytest.raises(CutoverRefusedError):
        await migration_service.cutover(
            migration(), approved_by="arvind", reason="release window", facts=degraded
        )


async def test_asking_why_you_cannot_cut_over_does_not_move_the_workflow(
    meta: AsyncEngine,
) -> None:
    """An operator will ask repeatedly while fixing whatever is wrong."""
    await ready_for_cutover(meta)
    migration_service = service(meta)

    before = len(await MigrationStore(meta).history(NAME))
    await migration_service.gates(migration(), facts=GateFacts())
    await migration_service.gates(migration(), facts=GateFacts())

    assert len(await MigrationStore(meta).history(NAME)) == before
    assert (await migration_service.status(NAME)).state is MigrationState.READY_FOR_CUTOVER


async def cut_over(meta: AsyncEngine) -> None:
    """Walk a Migration through an approved cutover into the window."""
    await ready_for_cutover(meta)
    migration_service = service(meta)
    report = await migration_service.cutover(
        migration(), approved_by="arvind", reason="release window", facts=green_facts()
    )
    await migration_service.complete_cutover(
        migration(),
        approved_by="arvind",
        reason="release window",
        gates=report,
        lag=None,
        reconciliation=[_agreed()],
        position=SourcePosition(kind=PositionKind.LSN, value="37450805752"),
    )


def _agreed(dataset: str = "public.orders", *, agreed: bool = True) -> ReconciliationReport:
    from gantry.migration.reconcile import LayerOutcome, LayerResult, ReconciliationLayer

    report = ReconciliationReport(dataset=dataset, target=dataset)
    report.layers.append(
        LayerResult(
            layer=ReconciliationLayer.COUNT,
            outcome=LayerOutcome.AGREED if agreed else LayerOutcome.DISAGREED,
            detail="test",
        )
    )
    return report


async def test_a_completed_cutover_opens_the_rollback_window(meta: AsyncEngine) -> None:
    await cut_over(meta)

    status = await service(meta).status(NAME)
    assert status.state is MigrationState.ROLLBACK_WINDOW

    last = (await MigrationStore(meta).history(NAME))[-1]
    assert last.evidence is not None
    assert last.evidence["position"] == "37450805752"
    assert last.evidence["position_kind"] == "lsn"


async def test_a_cutover_that_cannot_drain_lands_in_rolling_back(meta: AsyncEngine) -> None:
    """Not FAILED. Traffic may already be moving, and the only safe direction
    from a half-finished cutover is back — which is reachable from
    CUTTING_OVER and from nowhere earlier."""
    await ready_for_cutover(meta)
    migration_service = service(meta)
    report = await migration_service.cutover(
        migration(), approved_by="arvind", reason="release window", facts=green_facts()
    )

    with pytest.raises(CutoverIncompleteError):
        await migration_service.complete_cutover(
            migration(),
            approved_by="arvind",
            reason="release window",
            gates=report,
            lag=timedelta(seconds=30),
            reconciliation=[_agreed()],
            position=SourcePosition(kind=PositionKind.LSN, value="1"),
        )

    assert (await migration_service.status(NAME)).state is MigrationState.ROLLING_BACK


async def test_the_window_reports_divergence_without_acting_on_it(
    meta: AsyncEngine,
) -> None:
    """The temptation this resists. Divergence may mean the migration was
    wrong, or that the target is now correct and the source is stale by
    design. Nothing here can tell those apart, so it reports and waits."""
    await cut_over(meta)
    migration_service = service(meta)

    window = await migration_service.window(migration(), reconciliation=[_agreed(agreed=False)])
    assert window.diverged

    assert (await migration_service.status(NAME)).state is MigrationState.ROLLBACK_WINDOW
    assert "rolling back is your call" in window.describe(datetime.now(UTC))


async def test_rollback_recovers_the_cutover_position_from_the_trail(
    meta: AsyncEngine,
) -> None:
    """Rolling back means treating the source as authoritative from a known
    point. Asking the operator to retype it would invite getting it wrong."""
    await cut_over(meta)

    record = await service(meta).roll_back(
        migration(), decided_by="arvind", reason="checkout errors spiked"
    )

    assert record.cutover_position is not None
    assert record.cutover_position.value == "37450805752"
    assert record.as_evidence()["method"] == "traffic"
    assert (await service(meta).status(NAME)).state is MigrationState.ROLLED_BACK


async def test_finalize_refuses_while_the_window_is_still_useful(
    meta: AsyncEngine,
) -> None:
    await cut_over(meta)

    with pytest.raises(WindowOpenError, match="left before the window closes"):
        await service(meta).finalize(migration(), reconciliation=[_agreed()])

    assert (await service(meta).status(NAME)).state is MigrationState.ROLLBACK_WINDOW


async def test_finalize_refuses_while_the_sides_disagree(meta: AsyncEngine) -> None:
    """Closing the window on a divergence would discard the only way back."""
    await cut_over(meta)

    with pytest.raises(WindowOpenError, match="diverged"):
        await service(meta).finalize(
            migration(),
            reconciliation=[_agreed(agreed=False)],
            now=datetime.now(UTC) + timedelta(days=2),
        )


async def test_finalize_closes_an_elapsed_window(meta: AsyncEngine) -> None:
    await cut_over(meta)

    status = await service(meta).finalize(
        migration(), reconciliation=[_agreed()], now=datetime.now(UTC) + timedelta(days=2)
    )

    assert status.state is MigrationState.COMPLETED


async def test_a_completed_migration_cannot_be_rolled_back(meta: AsyncEngine) -> None:
    """The source has been released; there is nothing to roll back to."""
    await cut_over(meta)
    await service(meta).finalize(
        migration(), reconciliation=[_agreed()], now=datetime.now(UTC) + timedelta(days=2)
    )

    with pytest.raises(IllegalMigrationTransitionError):
        await service(meta).roll_back(migration(), decided_by="arvind", reason="too late")


async def test_the_audit_trail_reconstructs_every_decision_and_who_made_it(
    meta: AsyncEngine,
) -> None:
    """The exit criterion for the phase."""
    await cut_over(meta)
    await service(meta).roll_back(migration(), decided_by="arvind", reason="checkout errors spiked")

    history = await MigrationStore(meta).history(NAME)
    operator_decisions = [entry for entry in history if entry.actor is ActorKind.OPERATOR]

    assert [entry.to_state for entry in operator_decisions] == [
        MigrationState.CUTTING_OVER,
        MigrationState.ROLLING_BACK,
    ]
    assert all(entry.actor_id == "arvind" for entry in operator_decisions)
    assert all(entry.reason for entry in operator_decisions)
    assert all(entry.evidence for entry in operator_decisions)
