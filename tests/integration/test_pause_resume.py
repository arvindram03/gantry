"""Pause drains; it does not kill.

Pausing is enforced at the dispatch layer: a paused operation stops handing out
work, and whatever is already leased runs to its checkpoint and completes. A
pause that killed in-flight work would throw away a partition mid-copy for no
reason - the work is already idempotent, so there is nothing to gain by it.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest
from gantry.core.operation import LifecycleStage, OperationState, OperationType
from gantry.lifecycle.plan import NodeKind, PlanNode, PlanVersion, node_id
from gantry.lifecycle.states import ActorKind, IllegalTransitionError
from gantry.scheduler.postgres import PostgresWorkflowBackend
from gantry.state.database import create_engine
from gantry.state.operations import ConcurrentUpdateError, OperationStore
from sqlalchemy.ext.asyncio import AsyncEngine

from .conftest import clear_operation

pytestmark = pytest.mark.integration

META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
OPERATION = "pause-resume-test"
AT = datetime(2026, 9, 18, tzinfo=UTC)
LEASE = timedelta(seconds=30)


@pytest.fixture
async def meta() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(META_URL)
    try:
        await clear_operation(engine, OPERATION)
        yield engine
    finally:
        await engine.dispose()


def plan() -> PlanVersion:
    nodes = tuple(
        PlanNode(
            id=node_id(OPERATION, NodeKind.SNAPSHOT_PARTITION, f"p{index}"),
            kind=NodeKind.SNAPSHOT_PARTITION,
            stage=LifecycleStage.EXECUTE,
            scope=f"p{index}",
        )
        for index in range(4)
    )
    return PlanVersion(
        operation=OPERATION,
        operation_type=OperationType.MOVEMENT,
        version=1,
        nodes=nodes,
        guarantee_fingerprint="sha256:" + "0" * 64,
        created_at=AT,
    )


async def prepared(meta: AsyncEngine) -> tuple[OperationStore, PostgresWorkflowBackend]:
    store = OperationStore(meta)
    await store.ensure(OPERATION, OperationType.MOVEMENT, plan_version=1)
    await store.transition(
        OPERATION, OperationState.PLANNED, actor=ActorKind.OPERATOR, reason="test setup"
    )
    for step in (OperationState.GENERATED, OperationState.VALIDATED, OperationState.EXECUTING):
        await store.transition(OPERATION, step, actor=ActorKind.RUNTIME, reason="test setup")

    backend = PostgresWorkflowBackend(meta, OPERATION)
    await backend.submit(plan())
    return store, backend


async def test_a_running_operation_hands_out_work(meta: AsyncEngine) -> None:
    _, backend = await prepared(meta)
    assert await backend.lease("w1", LEASE, now=AT) is not None


async def test_pausing_stops_new_work(meta: AsyncEngine) -> None:
    store, backend = await prepared(meta)
    await store.transition(
        OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="operator paused"
    )
    assert await backend.lease("w1", LEASE, now=AT) is None


async def test_pausing_does_not_disturb_work_in_flight(meta: AsyncEngine) -> None:
    """The leased task keeps its lease and can still complete."""
    store, backend = await prepared(meta)
    in_flight = await backend.lease("w1", LEASE, now=AT)
    assert in_flight is not None

    await store.transition(
        OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="operator paused"
    )

    assert await backend.lease("w2", LEASE, now=AT) is None, "no new work while paused"
    await backend.complete(in_flight)

    states = {task.node_id: task.state.value for task in await backend.tasks(OPERATION)}
    assert states[in_flight.node_id] == "done", "in-flight work must be allowed to finish"


async def test_resuming_hands_out_work_again(meta: AsyncEngine) -> None:
    store, backend = await prepared(meta)
    await store.transition(
        OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="paused"
    )
    assert await backend.lease("w1", LEASE, now=AT) is None

    await store.transition(
        OPERATION, OperationState.EXECUTING, actor=ActorKind.OPERATOR, reason="resumed"
    )
    assert await backend.lease("w1", LEASE, now=AT) is not None


async def test_a_failed_operation_hands_out_nothing(meta: AsyncEngine) -> None:
    store, backend = await prepared(meta)
    await store.transition(
        OPERATION, OperationState.FAILED, actor=ActorKind.OPERATOR, reason="aborted"
    )
    assert await backend.lease("w1", LEASE, now=AT) is None


# --- the audit trail -------------------------------------------------------


async def test_every_transition_is_recorded(meta: AsyncEngine) -> None:
    store, _ = await prepared(meta)
    await store.transition(
        OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="lunch"
    )
    history = await store.history(OPERATION)

    assert [step.to_state.value for step in history][-1] == "paused"
    assert history[-1].reason == "lunch"
    assert history[-1].actor is ActorKind.OPERATOR
    assert all(step.occurred_at.tzinfo is not None for step in history)


async def test_transitions_form_a_connected_chain(meta: AsyncEngine) -> None:
    store, _ = await prepared(meta)
    history = await store.history(OPERATION)
    for earlier, later in pairwise(history):
        assert earlier.to_state is later.from_state


async def test_an_illegal_transition_never_reaches_the_database(meta: AsyncEngine) -> None:
    """A rejected move must leave no trace, in state or in the audit log."""
    store, _ = await prepared(meta)
    before = len(await store.history(OPERATION))

    with pytest.raises(IllegalTransitionError):
        await store.transition(
            OPERATION, OperationState.DRAFT, actor=ActorKind.AGENT, reason="agent overreach"
        )

    assert (await store.get(OPERATION)).state is OperationState.EXECUTING
    assert len(await store.history(OPERATION)) == before


async def test_concurrent_transitions_do_not_both_win(meta: AsyncEngine) -> None:
    """Optimistic concurrency: two workers cannot both advance one operation."""
    store, _ = await prepared(meta)
    stale = await store.get(OPERATION)

    await store.transition(
        OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="first"
    )

    class StaleStore(OperationStore):
        async def get(self, name: str) -> object:  # type: ignore[override]
            return stale

    with pytest.raises(ConcurrentUpdateError):
        await StaleStore(meta).transition(
            OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="second"
        )
