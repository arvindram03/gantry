"""Connecting to Temporal and running a Movement through it."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker

from gantry.core.provenance import DatasetPin
from gantry.lifecycle.plan import PlanVersion
from gantry.registry.base import DatasetRegistry
from gantry.scheduler.temporal.activities import MovementActivities, activity_definitions
from gantry.scheduler.temporal.workflow import (
    MovementWorkflow,
    MovementWorkflowInput,
    MovementWorkflowResult,
)
from gantry.state.checkpoints import CheckpointStore
from gantry.state.plans import PlanStore

TEMPORAL_ADDRESS_ENV = "GANTRY_TEMPORAL_ADDRESS"
TEMPORAL_NAMESPACE_ENV = "GANTRY_TEMPORAL_NAMESPACE"
DEFAULT_ADDRESS = "localhost:17233"
DEFAULT_NAMESPACE = "gantry"
TASK_QUEUE = "gantry-movement"


def temporal_address() -> str:
    return os.environ.get(TEMPORAL_ADDRESS_ENV, DEFAULT_ADDRESS)


def temporal_namespace() -> str:
    return os.environ.get(TEMPORAL_NAMESPACE_ENV, DEFAULT_NAMESPACE)


async def connect(address: str | None = None, namespace: str | None = None) -> Client:
    return await Client.connect(
        address or temporal_address(), namespace=namespace or temporal_namespace()
    )


@asynccontextmanager
async def movement_worker(
    client: Client,
    *,
    plans: PlanStore,
    registry: DatasetRegistry,
    checkpoints: CheckpointStore,
    source_engine: AsyncEngine,
    target_engine: AsyncEngine,
    task_queue: str = TASK_QUEUE,
    max_concurrent_activities: int = 8,
) -> AsyncIterator[Worker]:
    """Run a worker for as long as the block is open.

    Activities are async and mostly waiting on a database, so they run in the
    event loop rather than a thread pool; the work itself happens in Postgres.
    """
    activities = MovementActivities(
        plans=plans,
        registry=registry,
        checkpoints=checkpoints,
        source_engine=source_engine,
        target_engine=target_engine,
    )
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[MovementWorkflow],
        activities=list(activity_definitions(activities)),
        max_concurrent_activities=max_concurrent_activities,
    )
    async with worker:
        yield worker


async def start_movement(
    client: Client,
    plan: PlanVersion,
    *,
    targets: Mapping[str, str],
    max_concurrency: int = 8,
    task_queue: str = TASK_QUEUE,
) -> WorkflowHandle[MovementWorkflow, MovementWorkflowResult]:
    """Start, or reattach to, the workflow for a plan version.

    The workflow id is derived from the operation and plan version, so starting
    an already-running Movement reattaches instead of launching a second one -
    the same idempotence the leased queue got from resubmission being a no-op.
    """
    return await client.start_workflow(
        MovementWorkflow.run,
        MovementWorkflowInput(
            operation=plan.operation,
            plan_version=plan.version,
            targets=dict(targets),
            max_concurrency=max_concurrency,
        ),
        id=workflow_id(plan.operation, plan.version),
        task_queue=task_queue,
    )


def workflow_id(operation: str, plan_version: int) -> str:
    return f"gantry/{operation}/v{plan_version}"


async def signal_movement(client: Client, operation: str, plan_version: int, signal: str) -> None:
    handle = client.get_workflow_handle(workflow_id(operation, plan_version))
    await handle.signal(signal)


async def movement_progress(client: Client, operation: str, plan_version: int) -> dict[str, int]:
    handle = client.get_workflow_handle(workflow_id(operation, plan_version))
    result: dict[str, int] = await handle.query("progress")
    return result


def pins_for(plan: PlanVersion, pins: tuple[DatasetPin, ...]) -> tuple[DatasetPin, ...]:
    """The Dataset versions to store alongside a plan."""
    return pins
