"""The Movement workflow.

Temporal replays workflow code from its event history to recover, so this
module must be deterministic: no clocks, no randomness, no I/O. Everything that
touches the outside world is an activity, and the results of those activities
are what the history records.

The plan is loaded through an activity rather than passed as input. That keeps
the workflow's starting arguments small and, more importantly, makes the plan a
recorded fact in the history - a replay sees exactly the plan the original run
saw, not whatever the database holds now.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Failures that no amount of retrying will fix. Classified by the activity and
# surfaced here by type name, because Temporal matches on the name.
NON_RETRYABLE = ["FatalMovementError", "ReplanRequiredMovementError"]


@dataclass
class MovementWorkflowInput:
    operation: str
    plan_version: int
    targets: dict[str, str]
    max_concurrency: int = 8


@dataclass
class NodeDescriptor:
    """The part of a plan node the workflow needs to schedule it."""

    id: str
    depends_on: list[str] = field(default_factory=list)


@dataclass
class MovementWorkflowResult:
    completed: list[str]
    failed: list[str]
    rows_changed: int


@workflow.defn(name="GantryMovement")
class MovementWorkflow:
    """Runs a Movement's plan DAG."""

    def __init__(self) -> None:
        self._paused = False
        self._completed: list[str] = []
        self._failed: list[str] = []
        self._rows = 0

    @workflow.signal
    def pause(self) -> None:
        """Stop scheduling new work. In-flight activities are left alone.

        An activity already running keeps running to its checkpoint - the same
        meaning pause has everywhere else in the runtime, because cancelling a
        partition mid-copy would discard work that is about to be durable.
        """
        self._paused = True

    @workflow.signal
    def resume(self) -> None:
        self._paused = False

    @workflow.query
    def progress(self) -> dict[str, int]:
        return {
            "completed": len(self._completed),
            "failed": len(self._failed),
            "rows_changed": self._rows,
            "paused": int(self._paused),
        }

    @workflow.run
    async def run(self, request: MovementWorkflowInput) -> MovementWorkflowResult:
        nodes: list[NodeDescriptor] = await workflow.execute_activity(
            "load_plan_nodes",
            args=[request.operation, request.plan_version],
            # Activities called by name carry no type information, so the
            # payload would arrive as plain dicts without this.
            result_type=list[NodeDescriptor],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

        remaining = {node.id: set(node.depends_on) for node in nodes}
        done: set[str] = set()

        while remaining:
            await workflow.wait_condition(lambda: not self._paused)

            ready = sorted(name for name, deps in remaining.items() if deps <= done)
            if not ready:
                # Nothing can run and nothing is running: the rest of the graph
                # depends on work that failed.
                self._failed.extend(sorted(remaining))
                break

            batch = ready[: request.max_concurrency]
            results = await asyncio.gather(
                *(self._run_node(request, name) for name in batch),
                return_exceptions=True,
            )

            for name, outcome in zip(batch, results, strict=True):
                del remaining[name]
                if isinstance(outcome, BaseException):
                    self._failed.append(name)
                    continue
                done.add(name)
                self._completed.append(name)
                self._rows += int(outcome)

        return MovementWorkflowResult(
            completed=self._completed, failed=self._failed, rows_changed=self._rows
        )

    async def _run_node(self, request: MovementWorkflowInput, node_id: str) -> int:
        rows: int = await workflow.execute_activity(
            "execute_movement_node",
            args=[request.operation, request.plan_version, node_id, request.targets],
            result_type=int,
            # A partition copy is long; the heartbeat is what lets Temporal tell
            # a slow COPY from a dead worker.
            start_to_close_timeout=timedelta(hours=2),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_interval=timedelta(minutes=1),
                maximum_attempts=5,
                non_retryable_error_types=NON_RETRYABLE,
            ),
        )
        return rows
