"""The leased queue against a real database.

Requires the local stack: make dev-up && alembic upgrade head.

The in-memory backend defines the leasing rules; these tests check Postgres
reproduces them, plus the guarantee only a database can make - that concurrent
workers never receive the same task.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from gantry.lifecycle.plan import NodeKind, PlanVersion, node_id
from gantry.movement.planner import compile_movement
from gantry.scheduler.backend import TaskState
from gantry.scheduler.postgres import PostgresWorkflowBackend
from gantry.spec import load_movement_spec
from gantry.state.database import create_engine, transaction
from gantry.state.tables import operations
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from .conftest import clear_all_operations

pytestmark = pytest.mark.integration

AT = datetime(2026, 9, 11, tzinfo=UTC)
LEASE = timedelta(seconds=30)
EXAMPLES = __import__("pathlib").Path(__file__).resolve().parents[2] / "spec" / "examples"


def plan() -> PlanVersion:
    domain = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml").to_movement()
    return compile_movement(domain, created_at=AT)


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    created = create_engine()
    try:
        await clear_all_operations(created)
        async with transaction(created) as connection:
            await connection.execute(
                insert(operations).values(
                    name=plan().operation,
                    operation_type="movement",
                    state="executing",
                    created_at=AT,
                    updated_at=AT,
                )
            )
        yield created
    finally:
        await created.dispose()


@pytest.fixture
def backend(engine: AsyncEngine) -> PostgresWorkflowBackend:
    return PostgresWorkflowBackend(engine, plan().operation)


async def test_submit_enqueues_every_node(backend: PostgresWorkflowBackend) -> None:
    compiled = plan()
    assert await backend.submit(compiled) == len(compiled.nodes)
    assert len(await backend.tasks(compiled.operation)) == len(compiled.nodes)


async def test_resubmission_does_not_reset_progress(backend: PostgresWorkflowBackend) -> None:
    """Restarts resubmit; that must not undo completed work."""
    compiled = plan()
    await backend.submit(compiled)
    leased = await backend.lease("w1", LEASE, now=AT)
    assert leased is not None
    await backend.complete(leased)

    assert await backend.submit(compiled) == 0
    states = {t.node_id: t.state for t in await backend.tasks(compiled.operation)}
    assert states[leased.node_id] is TaskState.DONE


async def test_lease_respects_dependencies(backend: PostgresWorkflowBackend) -> None:
    compiled = plan()
    await backend.submit(compiled)
    first = await backend.lease("w1", LEASE, now=AT)
    assert first is not None
    # discover has no dependencies, so it must be the first thing leasable.
    assert first.node_id == node_id(compiled.operation, NodeKind.DISCOVER)


async def test_a_task_is_never_leased_twice(backend: PostgresWorkflowBackend) -> None:
    compiled = plan()
    await backend.submit(compiled)
    first = await backend.lease("w1", LEASE, now=AT)
    second = await backend.lease("w2", LEASE, now=AT)
    assert first is not None
    assert second is None, "only discover is runnable, and w1 already holds it"


async def test_concurrent_workers_never_receive_the_same_task(engine: AsyncEngine) -> None:
    """SKIP LOCKED is what makes a worker pool safe."""
    compiled = plan()
    await PostgresWorkflowBackend(engine, compiled.operation).submit(compiled)

    backends = [PostgresWorkflowBackend(engine, compiled.operation) for _ in range(8)]
    leased = await asyncio.gather(
        *(b.lease(f"w{i}", LEASE, now=AT) for i, b in enumerate(backends))
    )
    claimed = [task for task in leased if task is not None]
    assert len(claimed) == 1, "only one node is runnable, so exactly one worker may win"
    assert len({task.node_id for task in claimed}) == len(claimed)


async def test_expired_lease_is_reclaimed(backend: PostgresWorkflowBackend) -> None:
    """A dead worker needs nobody to notice; the lease simply runs out."""
    compiled = plan()
    await backend.submit(compiled)
    leased = await backend.lease("doomed", LEASE, now=AT)
    assert leased is not None

    assert await backend.reclaim_expired(now=AT) == 0
    assert await backend.reclaim_expired(now=AT + LEASE + timedelta(seconds=1)) == 1

    retaken = await backend.lease("survivor", LEASE, now=AT + LEASE * 2)
    assert retaken is not None
    assert retaken.node_id == leased.node_id
    assert retaken.attempts == 2


async def test_release_returns_the_task_for_retry(backend: PostgresWorkflowBackend) -> None:
    compiled = plan()
    await backend.submit(compiled)
    leased = await backend.lease("w1", LEASE, now=AT)
    assert leased is not None

    state = await backend.release(leased, "transient failure", max_attempts=5)
    assert state is TaskState.PENDING
    again = await backend.lease("w2", LEASE, now=AT)
    assert again is not None
    assert again.node_id == leased.node_id


async def test_release_quarantines_after_max_attempts(backend: PostgresWorkflowBackend) -> None:
    compiled = plan()
    await backend.submit(compiled)
    leased = await backend.lease("w1", LEASE, now=AT)
    assert leased is not None

    state = await backend.release(leased, "poison", max_attempts=1)
    assert state is TaskState.QUARANTINED
    assert await backend.lease("w2", LEASE, now=AT) is None


async def test_full_drain_completes_every_node(backend: PostgresWorkflowBackend) -> None:
    """The whole plan, leased and completed in dependency order."""
    compiled = plan()
    await backend.submit(compiled)

    completed: list[str] = []
    while (task := await backend.lease("w1", LEASE, now=AT)) is not None:
        await backend.complete(task)
        completed.append(task.node_id)

    assert len(completed) == len(compiled.nodes)
    customers = node_id(compiled.operation, NodeKind.SNAPSHOT_PARTITION, "customers")
    orders = node_id(compiled.operation, NodeKind.SNAPSHOT_PARTITION, "orders")
    assert completed.index(customers) < completed.index(orders)
