"""The worker loop.

The ordering in `_execute` is the whole point, and it is structural rather than
a convention a caller has to remember:

    read -> process -> write -> commit -> verify durable -> advance checkpoint

A crash anywhere before the checkpoint leaves the task leased. The lease
expires, another worker reclaims it, and the effect runs again - which is safe
only because effects are idempotent. Advancing the checkpoint before the commit
would make progress metadata run ahead of durable state, and no amount of
retrying recovers from that.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from gantry.adapters.fake import SimulatedCrashError
from gantry.core.positions import Checkpoint, CheckpointScope, PositionKind, SourcePosition
from gantry.scheduler.backend import Task, TaskState, WorkflowBackend
from gantry.state.checkpoints import CheckpointStore

DEFAULT_LEASE = timedelta(seconds=30)
DEFAULT_MAX_ATTEMPTS = 5


@dataclass
class WorkerReport:
    """What one worker did before stopping."""

    worker: str
    completed: list[str] = field(default_factory=list)
    retried: list[str] = field(default_factory=list)
    quarantined: list[str] = field(default_factory=list)
    crashed_on: str | None = None

    @property
    def crashed(self) -> bool:
        return self.crashed_on is not None


class Worker:
    """Leases tasks and runs them under the checkpoint ordering."""

    def __init__(
        self,
        name: str,
        backend: WorkflowBackend,
        checkpoints: CheckpointStore,
        execute: Callable[[str, str], str],
        *,
        clock: Callable[[], datetime],
        lease: timedelta = DEFAULT_LEASE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self._name = name
        self._backend = backend
        self._checkpoints = checkpoints
        self._execute = execute
        self._clock = clock
        self._lease = lease
        self._max_attempts = max_attempts

    async def run(self, *, max_tasks: int | None = None) -> WorkerReport:
        """Drain runnable tasks until none remain, or the process dies.

        A `SimulatedCrashError` propagates out deliberately: a crashed worker does
        not get to tidy up after itself, and the task stays leased until its
        lease expires.
        """
        report = WorkerReport(worker=self._name)
        processed = 0

        while max_tasks is None or processed < max_tasks:
            task = await self._backend.lease(self._name, self._lease, now=self._clock())
            if task is None:
                break
            processed += 1

            try:
                await self._run_task(task)
            except SimulatedCrashError as crash:
                report.crashed_on = task.node_id
                raise crash
            except Exception as error:
                state = await self._backend.release(
                    task, str(error), max_attempts=self._max_attempts
                )
                if state is TaskState.QUARANTINED:
                    report.quarantined.append(task.node_id)
                else:
                    report.retried.append(task.node_id)
                continue

            report.completed.append(task.node_id)

        return report

    async def _run_task(self, task: Task) -> None:
        # 1. Perform the effect and commit it durably.
        key = self._execute(task.operation, task.node_id)

        # 2. Only now record progress. A crash between these two lines is the
        #    case the whole design has to survive.
        await self._checkpoints.advance(
            task.operation,
            Checkpoint(
                scope=CheckpointScope.PARTITION,
                scope_id=task.node_id,
                position=SourcePosition(kind=PositionKind.PARTITION_ID, value=key),
                committed_at=self._clock(),
            ),
        )

        # 3. And only then release the task.
        await self._backend.complete(task)
