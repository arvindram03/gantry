"""Running a Movement end to end.

Holds the sequence the CLI drives: discover, compile, submit, run, and record
what happened. Keeping it here rather than in the CLI means the same sequence
is available to an API or a test without going through argument parsing.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine
from temporalio.client import Client as TemporalClient

from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.core.dataset import DatasetManifest, DatasetVersion
from gantry.core.operation import OperationState, OperationType
from gantry.core.provenance import DatasetPin, Lineage, Provenance
from gantry.core.results import ResultStatus
from gantry.lifecycle.plan import NodeKind, PlanVersion
from gantry.lifecycle.states import ActorKind
from gantry.movement.executor import MovementExecutor
from gantry.movement.model import Movement
from gantry.movement.planner import compile_movement
from gantry.movement.result import MovementResult
from gantry.registry.base import DatasetRegistry
from gantry.results.store import ResultStore
from gantry.scheduler.backend import TaskState, WorkflowBackend
from gantry.scheduler.temporal.runner import connect, movement_worker, start_movement
from gantry.scheduler.worker import Worker
from gantry.state.checkpoints import CheckpointStore
from gantry.state.operations import OperationStore
from gantry.state.plans import PlanStore


@dataclass(frozen=True)
class Progress:
    """A snapshot of where an operation has got to."""

    state: OperationState
    plan_version: int | None
    tasks_total: int
    tasks_done: int
    tasks_pending: int
    tasks_leased: int
    tasks_quarantined: int
    checkpoints: int

    @property
    def percent_complete(self) -> float:
        return 0.0 if not self.tasks_total else 100.0 * self.tasks_done / self.tasks_total


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MovementService:
    """Drives one Movement through the runtime."""

    def __init__(
        self,
        *,
        source_engine: AsyncEngine,
        target_engine: AsyncEngine,
        operations: OperationStore,
        backend_factory: Callable[[str], WorkflowBackend],
        checkpoints: CheckpointStore,
        registry: DatasetRegistry,
        results: ResultStore,
        plans: PlanStore,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._source_engine = source_engine
        self._target_engine = target_engine
        self._operations = operations
        self._backend_factory = backend_factory
        self._checkpoints = checkpoints
        self._registry = registry
        self._results = results
        self._plans = plans
        self._clock: Callable[[], datetime] = clock or _utc_now
        # Populated by plan(); run() needs the Dataset versions the plan was
        # compiled against, because a Result's provenance pins them.
        self._pins: tuple[DatasetPin, ...] = ()
        self._manifests: dict[str, DatasetManifest] = {}

    async def plan(
        self, movement: Movement, *, schemas: Sequence[str] = ("public",)
    ) -> PlanVersion:
        """Discover the source, register its Datasets, and compile the plan."""
        source = PostgresSourceAdapter(self._source_engine)
        manifests: dict[str, DatasetManifest] = {}
        registered: list[DatasetVersion] = []
        for manifest in await source.discover(schemas=schemas):
            complete = await source.profile(manifest)
            version = await self._registry.register(complete)
            manifests[complete.name] = complete
            registered.append(version)

        plan = compile_movement(movement, created_at=self._clock(), manifests=manifests)

        await self._operations.ensure(
            movement.name, OperationType.MOVEMENT, plan_version=plan.version
        )
        record = await self._operations.get(movement.name)
        if record.state is OperationState.DRAFT:
            await self._operations.transition(
                movement.name,
                OperationState.PLANNED,
                actor=ActorKind.OPERATOR,
                reason=f"compiled plan version {plan.version}",
                plan_version=plan.version,
            )

        self._pins = tuple(DatasetPin.from_version(version) for version in registered)
        self._manifests = manifests

        # The plan has to outlive this process: a Temporal activity worker, or
        # any other worker, reconstructs it rather than recompiling.
        await self._plans.put(plan, self._pins)
        await self._backend_factory(movement.name).submit(plan)
        return plan

    async def run(
        self,
        movement: Movement,
        plan: PlanVersion,
        *,
        targets: Mapping[str, str],
        worker_name: str = "worker-0",
    ) -> MovementResult:
        """Execute the plan and record what it produced."""
        started = self._clock()
        backend = self._backend_factory(movement.name)

        record = await self._operations.get(movement.name)
        if record.state in (OperationState.PLANNED, OperationState.PAUSED):
            await self._operations.transition(
                movement.name,
                OperationState.EXECUTING
                if record.state is OperationState.PAUSED
                else OperationState.GENERATED,
                actor=ActorKind.OPERATOR,
                reason="run requested",
            )
        record = await self._operations.get(movement.name)
        for step in (OperationState.VALIDATED, OperationState.EXECUTING):
            if record.state is not step and _precedes(record.state, step):
                await self._operations.transition(
                    movement.name, step, actor=ActorKind.RUNTIME, reason="advancing to execute"
                )
                record = await self._operations.get(movement.name)

        executor = MovementExecutor(
            source_engine=self._source_engine,
            target_engine=self._target_engine,
            manifests=dict(self._manifests),
            targets=dict(targets),
        )
        worker = Worker(
            worker_name,
            backend,
            self._checkpoints,
            executor,
            plan,
            clock=self._clock,
        )
        await backend.reclaim_expired(now=self._clock())
        report = await worker.run()

        progress = await self.progress(movement.name, plan)
        finished = self._clock()

        result = MovementResult(
            # A dot, not a slash: result names are ResourceNames, and a slash is not
            # a legal character in one.
            name=f"{movement.name}.movement",
            status=ResultStatus.OK if progress.tasks_pending == 0 else ResultStatus.FAILED,
            provenance=Provenance(
                generated_at=finished,
                operation=movement.name,
                plan_version=plan.version,
                lineage=Lineage(inputs=self._pins),
                checkpoints=tuple(await self._checkpoints.all(movement.name)),
            ),
            created_at=finished,
            rows_inserted=report.rows_written,
            partitions_total=_partition_count(plan),
            # Completed *partitions*, not completed tasks: a plan also carries
            # discovery and schema nodes, and counting those made a Movement
            # appear to have finished more partitions than it has.
            partitions_complete=await self._completed_partitions(movement.name, plan),
            started_at=started,
            finished_at=finished,
        )
        await self._results.put(result)

        if progress.tasks_pending == 0 and progress.tasks_quarantined == 0:
            current = await self._operations.get(movement.name)
            if current.state is OperationState.EXECUTING:
                await self._operations.transition(
                    movement.name,
                    OperationState.VERIFYING,
                    actor=ActorKind.RUNTIME,
                    reason="every partition complete; verification pending",
                )
        return result

    async def _completed_partitions(self, name: str, plan: PlanVersion) -> int:
        partition_nodes = {
            node.id for node in plan.nodes if node.kind is NodeKind.SNAPSHOT_PARTITION
        }
        tasks = await self._backend_factory(name).tasks(name)
        return sum(
            1 for task in tasks if task.state is TaskState.DONE and task.node_id in partition_nodes
        )

    async def progress(self, name: str, plan: PlanVersion | None = None) -> Progress:
        """Where an operation has got to, independent of who dispatched it.

        Progress is measured in checkpoints against the stored plan, not in
        task rows. Both execution backends write checkpoints; only the leased
        queue writes tasks, so reading tasks would report a Temporal-dispatched
        Movement as unfinished long after it completed. Dispatch state is an
        implementation detail of a backend; the checkpoint is the runtime's own
        record of what is durable.
        """
        record = await self._operations.get(name)
        stored = plan or (
            await self._plans.get(name, record.current_plan_version)
            if record.current_plan_version
            else None
        )
        total = len(stored.nodes) if stored else 0
        done = len(await self._checkpoints.all(name))

        # Task counts are still worth showing when the queue is in use, and are
        # simply zero otherwise.
        tasks = await self._backend_factory(name).tasks(name)
        by_state = dict.fromkeys(TaskState, 0)
        for task in tasks:
            by_state[task.state] += 1

        return Progress(
            state=record.state,
            plan_version=record.current_plan_version,
            tasks_total=total or len(tasks),
            tasks_done=done,
            tasks_pending=max(0, (total or len(tasks)) - done),
            tasks_leased=by_state[TaskState.LEASED],
            tasks_quarantined=by_state[TaskState.QUARANTINED],
            checkpoints=done,
        )

    async def pause(self, name: str, *, reason: str) -> None:
        """Stop handing out work. In-flight tasks run to their checkpoint."""
        await self._operations.transition(
            name, OperationState.PAUSED, actor=ActorKind.OPERATOR, reason=reason
        )

    async def resume(self, name: str, *, reason: str) -> None:
        await self._operations.transition(
            name, OperationState.EXECUTING, actor=ActorKind.OPERATOR, reason=reason
        )

    async def abort(self, name: str, *, reason: str) -> None:
        await self._operations.transition(
            name, OperationState.FAILED, actor=ActorKind.OPERATOR, reason=reason
        )

    async def run_on_temporal(
        self,
        movement: Movement,
        plan: PlanVersion,
        *,
        targets: Mapping[str, str],
        client: TemporalClient | None = None,
        max_concurrency: int = 8,
    ) -> MovementResult:
        """Execute the plan through Temporal instead of the leased queue.

        The runtime's guarantees do not move: the activity still commits before
        it checkpoints, and still depends on the write being idempotent, because
        Temporal delivers at-least-once exactly as the queue did. What moves is
        who owns dispatch, retries and timeouts.
        """
        started = self._clock()
        connection = client or await connect()

        await self._advance_to_executing(movement.name)

        async with movement_worker(
            connection,
            plans=self._plans,
            registry=self._registry,
            checkpoints=self._checkpoints,
            source_engine=self._source_engine,
            target_engine=self._target_engine,
            max_concurrent_activities=max_concurrency,
        ):
            handle = await start_movement(
                connection, plan, targets=targets, max_concurrency=max_concurrency
            )
            outcome = await handle.result()

        finished = self._clock()
        result = MovementResult(
            name=f"{movement.name}.movement",
            status=ResultStatus.OK if not outcome.failed else ResultStatus.FAILED,
            provenance=Provenance(
                generated_at=finished,
                operation=movement.name,
                plan_version=plan.version,
                lineage=Lineage(inputs=self._pins),
                checkpoints=tuple(await self._checkpoints.all(movement.name)),
            ),
            created_at=finished,
            rows_inserted=outcome.rows_changed,
            partitions_total=_partition_count(plan),
            partitions_complete=sum(
                1
                for node in plan.nodes
                if node.kind is NodeKind.SNAPSHOT_PARTITION and node.id in set(outcome.completed)
            ),
            started_at=started,
            finished_at=finished,
        )
        await self._results.put(result)

        if not outcome.failed:
            current = await self._operations.get(movement.name)
            if current.state is OperationState.EXECUTING:
                await self._operations.transition(
                    movement.name,
                    OperationState.VERIFYING,
                    actor=ActorKind.RUNTIME,
                    reason="every partition complete; verification pending",
                )
        return result

    async def _advance_to_executing(self, name: str) -> None:
        """Walk the operation forward to EXECUTING, whatever state it is in."""
        record = await self._operations.get(name)
        if record.state is OperationState.PAUSED:
            await self._operations.transition(
                name, OperationState.EXECUTING, actor=ActorKind.OPERATOR, reason="run requested"
            )
            return
        for step in (
            OperationState.PLANNED,
            OperationState.GENERATED,
            OperationState.VALIDATED,
            OperationState.EXECUTING,
        ):
            record = await self._operations.get(name)
            if _precedes(record.state, step):
                await self._operations.transition(
                    name, step, actor=ActorKind.RUNTIME, reason="advancing to execute"
                )


_ORDER = (
    OperationState.DRAFT,
    OperationState.PLANNED,
    OperationState.GENERATED,
    OperationState.VALIDATED,
    OperationState.EXECUTING,
)


def _precedes(current: OperationState, target: OperationState) -> bool:
    if current not in _ORDER or target not in _ORDER:
        return False
    return _ORDER.index(current) < _ORDER.index(target)


def _partition_count(plan: PlanVersion) -> int:
    return sum(1 for node in plan.nodes if node.kind is NodeKind.SNAPSHOT_PARTITION)
