"""Activities: everything that touches the outside world.

Temporal owns dispatch, retries and timeouts here, which is the whole reason
for adopting it. What it does not own is correctness: an activity still commits
before it checkpoints, and still relies on the write being idempotent, because
Temporal guarantees at-least-once execution exactly as the leased queue did.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio import activity
from temporalio.exceptions import ApplicationError

from gantry.core.dataset import DatasetManifest, DatasetRef
from gantry.core.positions import Checkpoint, CheckpointScope, PositionKind, SourcePosition
from gantry.movement.executor import MovementExecutor
from gantry.registry.base import DatasetRegistry
from gantry.scheduler.failures import FailureClass, classify
from gantry.scheduler.temporal.workflow import NodeDescriptor
from gantry.state.checkpoints import CheckpointStore
from gantry.state.plans import PlanStore


class FatalMovementError(ApplicationError):
    """A failure retrying cannot fix."""


class ReplanRequiredMovementError(ApplicationError):
    """The plan describes a world that no longer exists."""


@dataclass
class MovementActivities:
    """Activity implementations, bound to the stores a worker was given."""

    plans: PlanStore
    registry: DatasetRegistry
    checkpoints: CheckpointStore
    source_engine: AsyncEngine
    target_engine: AsyncEngine

    @activity.defn(name="load_plan_nodes")
    async def load_plan_nodes(self, operation: str, plan_version: int) -> list[NodeDescriptor]:
        plan = await self.plans.get(operation, plan_version)
        if plan is None:
            raise ReplanRequiredMovementError(
                f"plan {operation!r} version {plan_version} is not stored",
                type="ReplanRequiredMovementError",
                non_retryable=True,
            )
        return [NodeDescriptor(id=node.id, depends_on=list(node.depends_on)) for node in plan.nodes]

    @activity.defn(name="execute_movement_node")
    async def execute_movement_node(
        self, operation: str, plan_version: int, node_id: str, targets: dict[str, str]
    ) -> int:
        plan = await self.plans.get(operation, plan_version)
        if plan is None:
            raise ReplanRequiredMovementError(
                f"plan {operation!r} version {plan_version} is not stored",
                type="ReplanRequiredMovementError",
                non_retryable=True,
            )
        node = plan.node(node_id)
        if node is None:
            raise ReplanRequiredMovementError(
                f"node {node_id!r} is not in plan version {plan_version}",
                type="ReplanRequiredMovementError",
                non_retryable=True,
            )

        manifests = await self._pinned_manifests(operation, plan_version)
        executor = MovementExecutor(
            source_engine=self.source_engine,
            target_engine=self.target_engine,
            manifests=manifests,
            targets=targets,
        )

        heartbeat = asyncio.create_task(_heartbeat_while_working(node_id))
        try:
            # Commit first. The CommitResult is the evidence that permits the
            # checkpoint below; Temporal's history records that the activity
            # ran, which is a different fact from the data being durable.
            result = await executor.execute(node)
        except BaseException as error:
            raise _as_temporal_error(error) from error
        finally:
            heartbeat.cancel()

        await self.checkpoints.advance(
            operation,
            Checkpoint(
                scope=CheckpointScope.PARTITION,
                scope_id=node_id,
                position=SourcePosition(
                    kind=PositionKind.PARTITION_ID, value=node.scope or node_id
                ),
                committed_at=result.committed_at,
            ),
        )
        return result.rows_changed

    async def _pinned_manifests(
        self, operation: str, plan_version: int
    ) -> dict[str, DatasetManifest]:
        """Resolve the exact Dataset versions the plan was compiled against.

        Reading whatever is latest would let a rediscovery between planning and
        execution silently change what runs.
        """
        pins = await self.plans.pins(operation, plan_version)
        resolved: dict[str, DatasetManifest] = {}
        for pin in pins:
            version = await self.registry.get(DatasetRef(name=pin.name, version=pin.version))
            resolved[version.name] = version.manifest
        return resolved


async def _heartbeat_while_working(node_id: str, interval: float = 20.0) -> None:
    """Tell Temporal the activity is alive while a long COPY runs.

    Without this, a partition that takes longer than the heartbeat timeout is
    indistinguishable from a worker that died holding it.
    """
    while True:
        await asyncio.sleep(interval)
        activity.heartbeat(node_id)


def _as_temporal_error(error: BaseException) -> BaseException:
    """Map a failure onto Temporal's retry semantics.

    The classification is the runtime's, not Temporal's: what the scheduler
    already knew about which failures are worth retrying now decides whether
    Temporal retries them.
    """
    if not isinstance(error, Exception):
        # A kill or a cancellation is not a failure to classify.
        return error

    failure = classify(error)
    if failure is FailureClass.FATAL:
        return FatalMovementError(str(error), type="FatalMovementError", non_retryable=True)
    if failure is FailureClass.NEEDS_REPLAN:
        return ReplanRequiredMovementError(
            str(error), type="ReplanRequiredMovementError", non_retryable=True
        )
    return error


def activity_definitions(
    activities: MovementActivities,
) -> Sequence[Callable[..., Awaitable[object]]]:
    """The bound methods Temporal should register as activities."""
    return (activities.load_plan_nodes, activities.execute_movement_node)
