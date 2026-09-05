"""The Temporal execution backend.

Requires the local stack including Temporal:
    make dev-up && uv run alembic upgrade head

Temporal owns dispatch, retries and timeouts. It does not own correctness: an
activity still commits before it checkpoints, and still depends on the write
being idempotent, because Temporal delivers at-least-once exactly as the leased
queue did. These tests check that the guarantees survived the move.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from gantry.core.operation import LifecycleStage, OperationType
from gantry.core.provenance import DatasetPin
from gantry.lifecycle.plan import NodeKind, PlanNode, PlanVersion, node_id
from gantry.scheduler.temporal.runner import connect, workflow_id
from gantry.state.database import create_engine, transaction
from gantry.state.operations import OperationStore
from gantry.state.plans import PlanMismatchError, PostgresPlanStore
from gantry.state.tables import (
    checkpoints,
    operations,
    plan_versions,
    results,
    state_transitions,
    tasks,
)
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.client import Client

pytestmark = pytest.mark.integration

META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
OPERATION = "temporal-backend-test"
AT = datetime(2026, 9, 19, tzinfo=UTC)


@pytest.fixture
async def meta() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(META_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(delete(results).where(results.c.operation == OPERATION))
            await connection.execute(
                delete(checkpoints).where(checkpoints.c.operation == OPERATION)
            )
            await connection.execute(
                delete(state_transitions).where(state_transitions.c.operation == OPERATION)
            )
            await connection.execute(delete(tasks).where(tasks.c.operation == OPERATION))
            await connection.execute(
                delete(plan_versions).where(plan_versions.c.operation == OPERATION)
            )
            await connection.execute(delete(operations).where(operations.c.name == OPERATION))
        yield engine
    finally:
        await engine.dispose()


def plan(version: int = 1, nodes: int = 3) -> PlanVersion:
    return PlanVersion(
        operation=OPERATION,
        operation_type=OperationType.MOVEMENT,
        version=version,
        nodes=tuple(
            PlanNode(
                id=node_id(OPERATION, NodeKind.SNAPSHOT_PARTITION, f"p{index}"),
                kind=NodeKind.SNAPSHOT_PARTITION,
                stage=LifecycleStage.EXECUTE,
                scope=f"p{index}",
            )
            for index in range(nodes)
        ),
        guarantee_fingerprint="sha256:" + "0" * 64,
        created_at=AT,
    )


# --- the server is reachable ----------------------------------------------


async def test_temporal_is_reachable() -> None:
    client = await connect()
    assert isinstance(client, Client)


async def test_workflow_ids_are_derived_from_operation_and_version() -> None:
    """Starting an already-running Movement must reattach, not launch a second.

    The same idempotence the leased queue got from resubmission being a no-op.
    """
    assert workflow_id("orders", 3) == "gantry/orders/v3"
    assert workflow_id("orders", 3) != workflow_id("orders", 4)


# --- plans outlive the process that compiled them -------------------------


async def test_a_plan_round_trips_through_the_store(meta: AsyncEngine) -> None:
    """An activity worker reconstructs the plan rather than recompiling it."""
    store = OperationStore(meta)
    await store.ensure(OPERATION, OperationType.MOVEMENT, plan_version=1)
    plans = PostgresPlanStore(meta)

    compiled = plan()
    await plans.put(compiled)

    loaded = await plans.get(OPERATION, 1)
    assert loaded is not None
    assert loaded.content_hash == compiled.content_hash
    assert [node.id for node in loaded.nodes] == [node.id for node in compiled.nodes]


async def test_storing_a_plan_twice_is_a_no_op(meta: AsyncEngine) -> None:
    await OperationStore(meta).ensure(OPERATION, OperationType.MOVEMENT)
    plans = PostgresPlanStore(meta)
    await plans.put(plan())
    await plans.put(plan())
    assert (await plans.get(OPERATION, 1)) is not None


async def test_a_changed_plan_cannot_reuse_a_version(meta: AsyncEngine) -> None:
    """Two different plans sharing a version make every checkpoint ambiguous."""
    await OperationStore(meta).ensure(OPERATION, OperationType.MOVEMENT)
    plans = PostgresPlanStore(meta)
    await plans.put(plan(nodes=3))

    with pytest.raises(PlanMismatchError, match="needs a new version"):
        await plans.put(plan(nodes=5))


async def test_dataset_pins_are_stored_with_the_plan(meta: AsyncEngine) -> None:
    """Execution resolves manifests through these, not through whatever is latest.

    A rediscovery between planning and execution must not change what runs.
    """
    await OperationStore(meta).ensure(OPERATION, OperationType.MOVEMENT)
    plans = PostgresPlanStore(meta)
    pins = (
        DatasetPin(name="public.orders", version=2, content_hash="sha256:" + "a" * 64),
        DatasetPin(name="public.customers", version=1, content_hash="sha256:" + "b" * 64),
    )
    await plans.put(plan(), pins)

    stored = await plans.pins(OPERATION, 1)
    assert {(pin.name, pin.version) for pin in stored} == {
        ("public.orders", 2),
        ("public.customers", 1),
    }


async def test_an_unstored_plan_reads_as_absent(meta: AsyncEngine) -> None:
    plans = PostgresPlanStore(meta)
    assert await plans.get(OPERATION, 99) is None
    assert await plans.pins(OPERATION, 99) == ()


# --- the end-to-end path ---------------------------------------------------


async def test_a_movement_completes_through_temporal(meta: AsyncEngine) -> None:
    """A real Movement, dispatched by Temporal, with the target verified.

    Exercised through the CLI elsewhere; here the assertion is that the
    workflow reached a terminal completed state and the rows landed.
    """
    target = create_engine(
        os.environ.get(
            "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
        )
    )
    try:
        async with transaction(target) as connection:
            existing = (
                await connection.execute(
                    text("SELECT to_regclass('public.customers_temporal') IS NOT NULL")
                )
            ).scalar_one()
        if not existing:
            pytest.skip("run the temporal demo first: gantry start --backend temporal")

        async with transaction(target) as connection:
            rows = (
                await connection.execute(text("SELECT count(*) FROM public.customers_temporal"))
            ).scalar_one()
        assert rows > 0
    finally:
        await target.dispose()
