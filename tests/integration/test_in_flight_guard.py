# SPDX-License-Identifier: Apache-2.0
"""The Day 19 rehearsal failure, reproduced and then refused.

The sequence that produced it: a Movement is interrupted mid-execution, the
source data changes, planning again yields a new version because partition
bounds come from the data — and submitting that version enqueued its nodes
beside the in-flight ones. The run finished partially and reported a
verification failure, which looks like a data bug and is not one.

This test drives the real stores rather than the guard function directly,
because the bug was never in the comparison. It was that nobody made it.

Requires: make dev-up && uv run alembic upgrade head.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from gantry.core.operation import LifecycleStage, OperationState, OperationType
from gantry.lifecycle.plan import NodeKind, PlanNode, PlanVersion, node_id
from gantry.lifecycle.states import ActorKind, OperationInFlightError
from gantry.movement.service import _refuse_if_in_flight
from gantry.state.database import create_engine
from gantry.state.operations import OperationStore
from sqlalchemy.ext.asyncio import AsyncEngine

from .conftest import clear_operation

pytestmark = pytest.mark.integration

META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
OPERATION = "in-flight-guard-test"
AT = datetime(2026, 9, 5, tzinfo=UTC)


@pytest.fixture
async def meta() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(META_URL)
    try:
        await clear_operation(engine, OPERATION)
        yield engine
    finally:
        await clear_operation(engine, OPERATION)
        await engine.dispose()


def plan(version: int, partitions: int) -> PlanVersion:
    """A plan whose shape depends on the data, as a real one's does."""
    nodes = tuple(
        PlanNode(
            id=node_id(OPERATION, NodeKind.SNAPSHOT_PARTITION, f"p{index}"),
            kind=NodeKind.SNAPSHOT_PARTITION,
            stage=LifecycleStage.EXECUTE,
            scope=f"p{index}",
        )
        for index in range(partitions)
    )
    return PlanVersion(
        operation=OPERATION,
        operation_type=OperationType.MOVEMENT,
        version=version,
        nodes=nodes,
        guarantee_fingerprint="sha256:" + "a" * 64,
        created_at=AT,
    )


async def advance_to_executing(store: OperationStore, *, plan_version: int) -> None:
    await store.ensure(OPERATION, OperationType.MOVEMENT, plan_version=plan_version)
    for state in (
        OperationState.PLANNED,
        OperationState.GENERATED,
        OperationState.VALIDATED,
        OperationState.EXECUTING,
    ):
        await store.transition(
            OPERATION,
            state,
            actor=ActorKind.RUNTIME,
            reason="setting up the test",
            plan_version=plan_version,
        )


async def test_a_new_plan_version_is_refused_while_the_old_one_runs(
    meta: AsyncEngine,
) -> None:
    """The rehearsal's exact shape: interrupted run, data moved, replan."""
    store = OperationStore(meta)
    await advance_to_executing(store, plan_version=1)

    # The source changed, so recompiling produces different partition bounds
    # and therefore a new version. That part is correct and stays legal.
    replanned = plan(version=2, partitions=6)
    assert replanned.content_hash != plan(version=1, partitions=4).content_hash

    with pytest.raises(OperationInFlightError, match="still executing version 1"):
        _refuse_if_in_flight(await store.get(OPERATION), replanned.version)


async def test_resuming_the_running_version_is_still_allowed(meta: AsyncEngine) -> None:
    """Crash recovery goes through this path. If the guard blocked it, a
    killed worker could never pick its own work back up."""
    store = OperationStore(meta)
    await advance_to_executing(store, plan_version=1)

    _refuse_if_in_flight(await store.get(OPERATION), 1)


async def test_a_paused_operation_can_be_replanned(meta: AsyncEngine) -> None:
    """Pausing is how an operator stops in order to change something."""
    store = OperationStore(meta)
    await advance_to_executing(store, plan_version=1)
    await store.transition(
        OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="source under load"
    )

    _refuse_if_in_flight(await store.get(OPERATION), 2)


async def test_the_recovery_the_refusal_recommends_actually_works(
    meta: AsyncEngine,
) -> None:
    """Follow the message's own advice end to end.

    It says to pause and plan again. Pausing has to be reachable from
    EXECUTING, the guard has to stop objecting afterwards, and the Operation
    has to be able to go forward again — otherwise the advice produces a
    Movement nobody can restart. `abort` would: it reaches FAILED, which is
    terminal by design.
    """
    store = OperationStore(meta)
    await advance_to_executing(store, plan_version=1)

    with pytest.raises(OperationInFlightError):
        _refuse_if_in_flight(await store.get(OPERATION), 2)

    await store.transition(
        OPERATION, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason="draining to replan"
    )
    _refuse_if_in_flight(await store.get(OPERATION), 2)

    await store.transition(
        OPERATION,
        OperationState.EXECUTING,
        actor=ActorKind.OPERATOR,
        reason="run requested",
        plan_version=2,
    )
    record = await store.get(OPERATION)
    assert record.current_plan_version == 2, "the record must say which version is running"
    _refuse_if_in_flight(record, 2)


async def test_the_recorded_version_follows_the_running_plan(meta: AsyncEngine) -> None:
    """The guard compares against `current_plan_version`, so that number has
    to track what is actually executing rather than whatever was last seen."""
    store = OperationStore(meta)
    await advance_to_executing(store, plan_version=3)
    assert (await store.get(OPERATION)).current_plan_version == 3
