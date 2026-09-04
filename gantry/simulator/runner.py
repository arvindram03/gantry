"""The execution simulator.

Runs a real plan through the real lifecycle engine and the real leasing rules,
with fake adapters underneath. Nothing here is a mock of the runtime - only the
systems at the edges are fake, so a guarantee proven here is a guarantee about
the code that ships.

Its job is to answer one question before any database is attached: does a
Movement and an Analysis traverse the same engine, and do the guarantees hold
when workers die?
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from gantry.adapters.fake import FakeTarget, FakeWorkload, FaultSpec, SimulatedCrashError
from gantry.core.operation import LifecycleStage, OperationState
from gantry.lifecycle.engine import (
    LifecycleEngine,
    Outcome,
    StageContext,
    StageResult,
)
from gantry.lifecycle.plan import PlanVersion
from gantry.scheduler.backend import TaskState
from gantry.scheduler.memory import InMemoryWorkflowBackend
from gantry.scheduler.worker import Worker
from gantry.state.checkpoints import InMemoryCheckpointStore


@dataclass
class SimulationReport:
    """What one simulated run produced."""

    operation: str
    final_state: OperationState
    completed_nodes: list[str] = field(default_factory=list)
    quarantined_nodes: list[str] = field(default_factory=list)
    crashes: list[str] = field(default_factory=list)
    verification_evidence: str = ""

    @property
    def completed(self) -> bool:
        return self.final_state is OperationState.COMPLETED


class Simulator:
    """Drives a plan to completion, restarting workers as they die."""

    def __init__(
        self,
        plan: PlanVersion,
        *,
        faults: FaultSpec | None = None,
        target: FakeTarget | None = None,
        verification: Callable[[FakeTarget], StageResult] | None = None,
        start: datetime,
        lease: timedelta = timedelta(seconds=30),
    ) -> None:
        self.plan = plan
        self.faults = faults or FaultSpec()
        self.target = target or FakeTarget()
        self._verification = verification
        self._lease = lease
        self._tick = start
        self.backend = InMemoryWorkflowBackend()
        self.checkpoints = InMemoryCheckpointStore()
        self.workload = FakeWorkload(self.target, self.faults)

    def clock(self) -> datetime:
        # Time only moves forward, and never during a task, so lease expiry is
        # driven explicitly by the simulator rather than by wall-clock luck.
        return self._tick

    async def run(self, *, max_restarts: int = 10) -> SimulationReport:
        """Execute the plan, tolerating worker deaths."""
        report = SimulationReport(operation=self.plan.operation, final_state=OperationState.DRAFT)
        await self.backend.submit(self.plan)

        for attempt in range(max_restarts + 1):
            worker = Worker(
                f"worker-{attempt}",
                self.backend,
                self.checkpoints,
                self.workload.run,
                clock=self.clock,
                lease=self._lease,
            )
            try:
                result = await worker.run()
            except SimulatedCrashError as crash:
                report.crashes.append(str(crash))
                # The dead worker's lease has to lapse before anyone else can
                # take the task. Nobody needs to notice the crash itself.
                self._tick += self._lease + timedelta(seconds=1)
                await self.backend.reclaim_expired(now=self._tick)
                continue

            report.completed_nodes.extend(result.completed)
            report.quarantined_nodes.extend(result.quarantined)
            if not result.retried:
                break
            self._tick += timedelta(seconds=1)

        report.final_state = await self._run_lifecycle(report)
        return report

    async def _run_lifecycle(self, report: SimulationReport) -> OperationState:
        """Run the plan's outcome through the lifecycle engine."""
        tasks = await self.backend.tasks(self.plan.operation)
        all_done = all(task.state is TaskState.DONE for task in tasks)

        def execute(context: StageContext) -> StageResult:
            if all_done:
                return StageResult(outcome=Outcome.OK, detail=f"{len(tasks)} nodes complete")
            stuck = sorted(t.node_id for t in tasks if t.state is not TaskState.DONE)
            return StageResult(outcome=Outcome.FAILED, detail=f"incomplete nodes: {stuck}")

        def verify(context: StageContext) -> StageResult:
            if self._verification is None:
                return StageResult(outcome=Outcome.OK)
            result = self._verification(self.target)
            report.verification_evidence = result.detail
            return result

        engine = LifecycleEngine(
            {LifecycleStage.EXECUTE: execute, LifecycleStage.VERIFY: verify},
            clock=self._advance,
        )
        return engine.run(self.plan).final_state

    def _advance(self) -> datetime:
        self._tick += timedelta(seconds=1)
        return self._tick


def duplicate_free(target: FakeTarget) -> StageResult:
    """A verification that rejects any duplicated effect.

    This is the runtime's core claim made checkable: at-least-once delivery is
    only safe if replays leave no second effect behind.
    """
    if target.effect_count == len(set(target.applied)):
        return StageResult(
            outcome=Outcome.OK,
            detail=(
                f"{target.effect_count} effects, "
                f"{target.suppressed_duplicates} duplicates suppressed"
            ),
        )
    return StageResult(  # pragma: no cover - defensive
        outcome=Outcome.VERIFICATION_FAILED, detail="duplicate effects detected"
    )
