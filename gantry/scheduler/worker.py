# SPDX-License-Identifier: Apache-2.0
"""The worker loop.

The ordering in `_run_task` is the whole point, and it is enforced by types
rather than by convention:

    lease -> execute -> obtain CommitResult -> advance checkpoint -> complete

A checkpoint cannot be advanced without a `CommitResult`, and only an executor
that actually committed can produce one. Nothing in the runtime can manufacture
progress it did not make.

A crash anywhere before the checkpoint leaves the task leased. The lease
expires, another worker reclaims it, and the effect runs again - which is safe
only because effects are idempotent. Advancing the checkpoint before the commit
would let progress metadata run ahead of durable state, and no amount of
retrying recovers from that.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol

from gantry.core.commit import CommitResult
from gantry.core.positions import Checkpoint, PositionKind, SourcePosition
from gantry.lifecycle.plan import PlanNode, PlanVersion, checkpoint_scope_for
from gantry.scheduler.backend import Task, TaskState, WorkflowBackend
from gantry.scheduler.failures import FailureClass, classify
from gantry.state.checkpoints import CheckpointStore

DEFAULT_LEASE = timedelta(seconds=30)
DEFAULT_MAX_ATTEMPTS = 5


class NodeExecutor(Protocol):
    """Performs one plan node's work and attests that it committed."""

    async def execute(self, node: PlanNode) -> CommitResult: ...


@dataclass
class WorkerReport:
    """What one worker did before stopping."""

    worker: str
    completed: list[str] = field(default_factory=list)
    retried: list[str] = field(default_factory=list)
    quarantined: list[str] = field(default_factory=list)
    fatal: list[str] = field(default_factory=list)
    needs_replan: list[str] = field(default_factory=list)
    rows_written: int = 0
    # Content hashes of the jobs that did the work, in the order first seen.
    jobs: list[str] = field(default_factory=list)
    crashed_on: str | None = None

    @property
    def crashed(self) -> bool:
        return self.crashed_on is not None

    @property
    def stopped_permanently(self) -> bool:
        return bool(self.fatal or self.needs_replan)


class Worker:
    """Leases tasks and runs them under the checkpoint ordering."""

    def __init__(
        self,
        name: str,
        backend: WorkflowBackend,
        checkpoints: CheckpointStore,
        executor: NodeExecutor,
        plan: PlanVersion,
        *,
        clock: Callable[[], datetime],
        lease: timedelta = DEFAULT_LEASE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self._name = name
        self._backend = backend
        self._checkpoints = checkpoints
        self._executor = executor
        self._plan = plan
        self._clock = clock
        self._lease = lease
        self._max_attempts = max_attempts

    async def run(self, *, max_tasks: int | None = None) -> WorkerReport:
        """Drain runnable tasks until none remain, or the process dies.

        A `BaseException` that is not an `Exception` - a kill, a cancellation -
        propagates deliberately. A crashed worker does not get to tidy up after
        itself, and its task stays leased until the lease expires.
        """
        report = WorkerReport(worker=self._name)
        processed = 0

        while max_tasks is None or processed < max_tasks:
            task = await self._backend.lease(self._name, self._lease, now=self._clock())
            if task is None:
                break
            processed += 1

            try:
                result = await self._run_task(task)
            except Exception as error:
                if not await self._handle_failure(task, error, report):
                    break
                continue

            report.completed.append(task.node_id)
            report.rows_written += result.rows_changed
            if result.job is not None and result.job not in report.jobs:
                report.jobs.append(result.job)

        return report

    async def _run_task(self, task: Task) -> CommitResult:
        node = self._plan.node(task.node_id)
        if node is None:
            raise ValueError(f"node {task.node_id!r} is not in plan version {self._plan.version}")

        # 1. Do the work and commit it durably. The CommitResult is the only
        #    evidence that this happened.
        result = await self._executor.execute(node)

        # 2. Only now record progress. A crash between these two lines is the
        #    case the whole design has to survive.
        await self._checkpoints.advance(
            task.operation,
            Checkpoint(
                # Scoped by what the node actually covers. The id stays the
                # node id: two dataset-level nodes over one Dataset would
                # otherwise share a checkpoint, and the later would overwrite
                # the earlier, losing the per-node progress resume depends on.
                # What a reader wants to see is the position's value, which
                # carries the readable scope.
                scope=checkpoint_scope_for(node.kind),
                scope_id=task.node_id,
                position=SourcePosition(
                    kind=PositionKind.PARTITION_ID, value=node.scope or task.node_id
                ),
                committed_at=result.committed_at,
            ),
        )

        # 3. And only then release the task.
        await self._backend.complete(task)
        return result

    async def _handle_failure(self, task: Task, error: Exception, report: WorkerReport) -> bool:
        """Record a failure. Returns False when the worker should stop."""
        failure = classify(error)
        reason = f"{failure.value}: {error}"

        if failure is FailureClass.FATAL:
            await self._backend.release(task, reason, max_attempts=0)
            report.fatal.append(task.node_id)
            return False

        if failure is FailureClass.NEEDS_REPLAN:
            # Retrying reproduces the same error; the plan has to change.
            await self._backend.release(task, reason, max_attempts=0)
            report.needs_replan.append(task.node_id)
            return False

        state = await self._backend.release(task, reason, max_attempts=self._max_attempts)
        if state is TaskState.QUARANTINED:
            report.quarantined.append(task.node_id)
        else:
            report.retried.append(task.node_id)
        return True
