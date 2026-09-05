# SPDX-License-Identifier: Apache-2.0
"""Driving a Migration.

The whole file is composition. `SNAPSHOTTING` and `CATCHING_UP` hand work to
`MovementService` and read the Operation states back; the workflow state is
*derived* from what the Movements report rather than tracked in parallel. Two
records of the same fact drift, and the one an operator happens to read decides
what they believe.

Nothing here plans, partitions, copies or checkpoints. If it starts to, the
design document's claim that migration is a workflow rather than a primitive
was wrong, and that is worth finding out here rather than arguing about.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from gantry.core.operation import TERMINAL_STATES, OperationState
from gantry.core.positions import PositionKind, SourcePosition
from gantry.lifecycle.migration import MigrationState
from gantry.lifecycle.states import ActorKind
from gantry.migration.cutover import (
    CutoverRecord,
    RollbackRecord,
    RollbackWindow,
    WindowState,
    build_record,
)
from gantry.migration.gates import GateFacts, GateReport, evaluate
from gantry.migration.model import Migration
from gantry.migration.prepare import PrepareReport
from gantry.migration.reconcile import ReconciliationReport
from gantry.state.migrations import MigrationRecord, MigrationStore, MigrationTransition
from gantry.state.operations import OperationStore, UnknownOperationError


class PrepareRefusedError(Exception):
    """The target cannot hold what the source would send it.

    Raised rather than returned because a caller that ignored it would go on
    to move data into a target the runtime has just said will not hold it. The
    report travels with the exception so nothing has to be re-derived.
    """

    def __init__(self, migration: str, report: PrepareReport) -> None:
        super().__init__(f"migration {migration!r} not ready: {report.describe()}")
        self.migration = migration
        self.report = report


class CutoverIncompleteError(Exception):
    """The cutover was approved but could not finish.

    The workflow is left in ROLLING_BACK, not FAILED: traffic may already be
    moving, and the only safe direction from a half-finished cutover is back.
    """

    def __init__(self, migration: str, record: CutoverRecord) -> None:
        failed = [step.describe() for step in record.steps if not step.completed]
        super().__init__(f"cutover of {migration!r} did not complete: {'; '.join(failed)}")
        self.migration = migration
        self.record = record


class WindowOpenError(Exception):
    """Finalize was called while the rollback window was still useful."""

    def __init__(self, migration: str, window: RollbackWindow, now: datetime) -> None:
        state = window.state(now)
        if state is WindowState.DIVERGED:
            detail = "the sides have diverged; decide before closing the window"
        else:
            detail = f"{window.remaining(now)} left before the window closes"
        super().__init__(f"cannot finalize {migration!r}: {detail}")
        self.migration = migration
        self.window = window


class CutoverRefusedError(Exception):
    """A gate said no.

    Carries the report so nothing has to be re-derived, and so the operator
    sees every gate rather than only the first one that refused — fixing them
    one round trip at a time is the experience this avoids.
    """

    def __init__(self, migration: str, report: GateReport) -> None:
        super().__init__(f"cutover refused: {report.describe()}")
        self.migration = migration
        self.report = report


class Reconciler(Protocol):
    """Whatever knows how to reconcile this Migration's Datasets.

    Injected like the runner and the preparer, and for the same reason: the
    workflow knows a Migration can be reconciled and that the answer is a list
    of reports. Which engines, which tables and which key is not its business.
    """

    async def __call__(self, migration: Migration, /) -> list[ReconciliationReport]: ...


class Preparer(Protocol):
    """Whatever knows how to check a target before anything moves.

    Injected for the same reason the runner is: the workflow knows that a
    Migration can be checked and that the answer is a report. It does not know
    about engines, catalogs or connection strings.
    """

    async def __call__(self, migration: Migration, /) -> PrepareReport: ...


class MovementRunner(Protocol):
    """Whatever knows how to run one Movement by name.

    Injected so the workflow composes over an interface rather than over
    `MovementService` itself. The Migration asks; something else knows how.
    """

    async def __call__(self, name: str, /) -> object: ...


@dataclass(frozen=True)
class MovementStatus:
    """What one Movement beneath a Migration reports."""

    name: str
    state: OperationState | None
    plan_version: int | None

    @property
    def started(self) -> bool:
        return self.state is not None

    @property
    def complete(self) -> bool:
        return self.state is OperationState.COMPLETED

    @property
    def failed(self) -> bool:
        return self.state in TERMINAL_STATES and self.state is not OperationState.COMPLETED

    def describe(self) -> str:
        state = "not started" if self.state is None else self.state.value
        version = "" if self.plan_version is None else f"  plan v{self.plan_version}"
        return f"{self.name}  {state}{version}"


@dataclass(frozen=True)
class MigrationStatus:
    """The workflow, and the Operations underneath it."""

    record: MigrationRecord
    movements: tuple[MovementStatus, ...]
    # Present only on the call that ran it. Reconciliation costs real queries
    # against both databases, so `status` does not re-run it to answer a
    # question about workflow state.
    reconciliation: tuple[ReconciliationReport, ...] = ()

    @property
    def state(self) -> MigrationState:
        return self.record.state

    @property
    def all_movements_complete(self) -> bool:
        return bool(self.movements) and all(m.complete for m in self.movements)

    @property
    def any_movement_failed(self) -> bool:
        return any(m.failed for m in self.movements)

    def describe(self) -> str:
        done = sum(1 for m in self.movements if m.complete)
        return f"{self.record.name}  {self.state.value}  movements {done}/{len(self.movements)}"


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MigrationService:
    """Runs a Migration by driving the Movements it names."""

    def __init__(
        self,
        *,
        migrations: MigrationStore,
        operations: OperationStore,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._migrations = migrations
        self._operations = operations
        self._clock: Callable[[], datetime] = clock or _utc_now

    async def plan(self, migration: Migration) -> MigrationRecord:
        """Register the Migration and walk it to `PLANNED`.

        Discovery and planning of the *data* happen inside each Movement's own
        `plan`. What happens here is only the workflow admitting that those
        Movements are the ones it will drive.
        """
        record = await self._migrations.ensure(migration)
        if record.state is not MigrationState.DRAFT:
            return record

        await self._migrations.transition(
            migration.name,
            MigrationState.DISCOVERING,
            actor=ActorKind.RUNTIME,
            reason="resolving the movements this migration composes",
        )
        await self._migrations.transition(
            migration.name,
            MigrationState.PLANNED,
            actor=ActorKind.RUNTIME,
            reason=f"composed from {len(migration.movements)} movement(s)",
        )
        return await self._migrations.get(migration.name)

    async def run(
        self,
        migration: Migration,
        *,
        runner: MovementRunner,
        preparer: Preparer | None = None,
        reconciler: Reconciler | None = None,
    ) -> MigrationStatus:
        """Drive the Movements, and derive the workflow state from what they did.

        The runner is injected rather than constructed here, and that is the
        whole composition argument in one parameter: this method knows that a
        Movement can be asked to run and that it reports an Operation state.
        It does not know about engines, partitions, checkpoints or specs, and
        it must not learn.
        """
        record = await self._migrations.ensure(migration)
        if record.state is MigrationState.DRAFT:
            record = await self.plan(migration)

        if record.state is MigrationState.PLANNED:
            await self._migrations.transition(
                migration.name,
                MigrationState.PREPARING,
                actor=ActorKind.RUNTIME,
                reason="checking targets before moving anything",
            )

            # The point of Prepare: refuse here, where a mismatch costs a
            # minute, rather than at cutover, where it costs the snapshot and
            # the catch-up too. The workflow goes back to PLANNED rather than
            # FAILED — an incompatible target is repairable input, not a dead
            # end, exactly as a failed Analysis validation returns to DRAFT.
            if preparer is not None:
                report = await preparer(migration)
                if not report.ready:
                    await self._migrations.transition(
                        migration.name,
                        MigrationState.PLANNED,
                        actor=ActorKind.RUNTIME,
                        reason=f"prepare refused: {report.describe()}",
                        evidence={"failures": [f.describe() for f in report.failures]},
                    )
                    raise PrepareRefusedError(migration.name, report)

            await self._migrations.transition(
                migration.name,
                MigrationState.SNAPSHOTTING,
                actor=ActorKind.RUNTIME,
                reason=f"running {len(migration.runnable)} movement(s)",
            )

        for name in migration.runnable:
            try:
                await runner(name)
            except Exception as error:
                # Recorded, then re-raised. Swallowing it would leave the
                # workflow claiming a phase it never finished.
                await self.fail(migration.name, reason=f"movement {name!r} failed: {error}")
                raise

        status = await self.status(migration.name)
        if status.any_movement_failed:
            await self.fail(migration.name, reason="a movement did not complete")
            return await self.status(migration.name)

        # Catch-up is a phase even when there is nothing to catch up: a
        # snapshot-only Movement passes through it in zero time, and skipping
        # the state would make the trail of a snapshot migration and a
        # streaming one different shapes for no reason.
        await self._advance(
            migration.name,
            MigrationState.CATCHING_UP,
            reason="applying changes made during the snapshot",
        )
        await self._advance(
            migration.name,
            MigrationState.VERIFYING,
            reason="reconciling source against target",
        )

        if reconciler is not None:
            reports = await reconciler(migration)
            disagreed = [report for report in reports if not report.agreed]
            evidence = {"datasets": [report.describe() for report in reports]}

            if disagreed:
                # Back to CATCHING_UP, not FAILED. Under live writes a
                # disagreement is more often a stream that has not finished
                # applying than data that is wrong, and the runtime cannot
                # tell those apart from one reading. Retrying is the cheap
                # answer; failing the migration is not reversible.
                await self._migrations.transition(
                    migration.name,
                    MigrationState.CATCHING_UP,
                    actor=ActorKind.RUNTIME,
                    reason=(
                        f"reconciliation found {len(disagreed)} dataset(s) disagreeing; "
                        f"applying more changes before verifying again"
                    ),
                    evidence=evidence,
                )
            else:
                await self._migrations.transition(
                    migration.name,
                    MigrationState.READY_FOR_CUTOVER,
                    actor=ActorKind.RUNTIME,
                    reason=f"reconciliation agreed on {len(reports)} dataset(s)",
                    evidence=evidence,
                )
            return await self.status(migration.name, reconciliation=tuple(reports))

        return await self.status(migration.name)

    async def status(
        self, name: str, *, reconciliation: tuple[ReconciliationReport, ...] = ()
    ) -> MigrationStatus:
        """The workflow state, and every Movement beneath it.

        Movement state is read from the Operations rather than mirrored into
        the Migration. A Movement that finished while nobody was watching is
        reflected here the moment anyone asks.
        """
        record = await self._migrations.get(name)
        statuses = []
        for movement in record.movements:
            try:
                operation = await self._operations.get(movement)
            except UnknownOperationError:
                statuses.append(MovementStatus(name=movement, state=None, plan_version=None))
                continue
            statuses.append(
                MovementStatus(
                    name=movement,
                    state=operation.state,
                    plan_version=operation.current_plan_version,
                )
            )
        return MigrationStatus(
            record=record, movements=tuple(statuses), reconciliation=reconciliation
        )

    async def _advance(
        self,
        name: str,
        to_state: MigrationState,
        *,
        reason: str,
        actor: ActorKind = ActorKind.RUNTIME,
        evidence: Mapping[str, object] | None = None,
    ) -> None:
        """Move to a state, or stay put if already there.

        The retry loop is why this exists. Reconciliation disagreeing sends the
        workflow back to CATCHING_UP, and running again has to walk
        CATCHING_UP -> VERIFYING from where it already is. A blind transition
        raises on the self-edge, which made the one path the design most
        expects to be taken the one that failed.
        """
        current = await self._migrations.get(name)
        if current.state is to_state:
            return
        await self._migrations.transition(
            name, to_state, actor=actor, reason=reason, evidence=evidence
        )

    async def gates(self, migration: Migration, *, facts: GateFacts | None = None) -> GateReport:
        """Evaluate the declared gates against what was measured.

        Read-only. Asking "why can't I cut over" must never move the workflow,
        because an operator will ask it repeatedly while fixing whatever is
        wrong.
        """
        return evaluate(migration.name, migration.cutover, facts or GateFacts())

    async def cutover(
        self,
        migration: Migration,
        *,
        approved_by: str,
        reason: str,
        facts: GateFacts | None = None,
    ) -> GateReport:
        """Move traffic, if every gate agrees.

        The gates are evaluated **here**, immediately before the transition,
        rather than trusted from an earlier `gates` call. A report from ten
        minutes ago is a claim about ten minutes ago, and the stream has been
        moving since.
        """
        measured = facts or GateFacts()
        report = evaluate(
            migration.name,
            migration.cutover,
            GateFacts(
                partitions_total=measured.partitions_total,
                partitions_verified=measured.partitions_verified,
                streaming=measured.streaming,
                cdc_lag=measured.cdc_lag,
                verification=measured.verification,
                target_healthy=measured.target_healthy,
                prepare=measured.prepare,
                reconciliation=measured.reconciliation,
                approved_by=approved_by,
            ),
        )

        if not report.passed:
            # Recorded on the way out. A refused cutover is a decision, and the
            # trail should show that someone tried and what stopped them.
            await self._migrations.transition(
                migration.name,
                MigrationState.CATCHING_UP,
                actor=ActorKind.RUNTIME,
                reason=f"cutover refused: {report.describe()}",
                evidence=report.as_evidence(),
            )
            raise CutoverRefusedError(migration.name, report)

        await self._migrations.transition(
            migration.name,
            MigrationState.CUTTING_OVER,
            actor=ActorKind.OPERATOR,
            actor_id=approved_by,
            reason=reason,
            evidence=report.as_evidence(),
        )
        return report

    async def complete_cutover(
        self,
        migration: Migration,
        *,
        approved_by: str,
        reason: str,
        gates: GateReport,
        lag: timedelta | None = None,
        reconciliation: Sequence[ReconciliationReport] = (),
        position: SourcePosition | None = None,
    ) -> CutoverRecord:
        """Finish a cutover already in `CUTTING_OVER`, opening the window.

        The steps run *after* the transition rather than before it, and that
        ordering is the honest one: the moment an operator approves, traffic is
        being moved by whatever moves it. If draining then fails, the workflow
        must be able to roll back — which it can only do from `CUTTING_OVER`.
        Doing the work first and transitioning after would leave a half-cut-over
        migration in a state with no way out.
        """
        record = build_record(
            migration.name,
            approved_by=approved_by,
            reason=reason,
            gates=gates,
            lag=lag,
            lag_threshold=migration.cutover.max_cdc_lag,
            reconciliation=reconciliation,
            position=position,
        )

        if not record.completed:
            failed = [step for step in record.steps if not step.completed]
            await self._migrations.transition(
                migration.name,
                MigrationState.ROLLING_BACK,
                actor=ActorKind.OPERATOR,
                actor_id=approved_by,
                reason=f"cutover could not complete: {failed[0].detail}",
                evidence=record.as_evidence(),
            )
            raise CutoverIncompleteError(migration.name, record)

        await self._migrations.transition(
            migration.name,
            MigrationState.ROLLBACK_WINDOW,
            actor=ActorKind.RUNTIME,
            reason=(
                f"cut over at {record.position.value if record.position else 'unknown'}; "
                f"source authoritative for {migration.rollback.window}"
            ),
            evidence=record.as_evidence(),
        )
        return record

    async def window(
        self,
        migration: Migration,
        *,
        reconciliation: Sequence[ReconciliationReport] = (),
        now: datetime | None = None,
    ) -> RollbackWindow:
        """Where the rollback window stands. Reports; never acts.

        Divergence here may mean the migration was wrong, or it may mean the
        application is writing to the target correctly and the source is stale
        by design. Nothing in the runtime can tell those apart, so rolling back
        stays a decision someone makes.
        """
        record = await self._migrations.get(migration.name)
        opened = await self._opened_at(migration.name) or record.updated_at
        return RollbackWindow(
            migration=migration.name,
            opened_at=opened,
            duration=migration.rollback.window,
            source_authoritative=migration.rollback.source_remains_authoritative,
            reconciliation=tuple(reconciliation),
        )

    async def roll_back(
        self,
        migration: Migration,
        *,
        decided_by: str,
        reason: str,
        position: SourcePosition | None = None,
    ) -> RollbackRecord:
        """Return authority to the source, on someone's say-so."""
        # Read the cutover position from the trail when the caller does not
        # supply one. Rolling back means treating the source as authoritative
        # *from a known point*, and the cutover already recorded which — asking
        # the operator to repeat it would invite them to get it wrong.
        record = RollbackRecord(
            migration=migration.name,
            decided_by=decided_by,
            reason=reason,
            at=self._clock(),
            cutover_position=position or await self._cutover_position(migration.name),
        )
        await self._migrations.transition(
            migration.name,
            MigrationState.ROLLING_BACK,
            actor=ActorKind.OPERATOR,
            actor_id=decided_by,
            reason=reason,
            evidence=record.as_evidence(),
        )
        await self._migrations.transition(
            migration.name,
            MigrationState.ROLLED_BACK,
            actor=ActorKind.RUNTIME,
            reason="authority returned to the source",
            evidence=record.as_evidence(),
        )
        return record

    async def finalize(
        self,
        migration: Migration,
        *,
        reconciliation: Sequence[ReconciliationReport] = (),
        now: datetime | None = None,
    ) -> MigrationStatus:
        """Close the window and mark the source decommissionable."""
        window = await self.window(migration, reconciliation=reconciliation, now=now)
        moment = now or self._clock()
        state = window.state(moment)

        if state is not WindowState.ELAPSED:
            raise WindowOpenError(migration.name, window, moment)

        await self._migrations.transition(
            migration.name,
            MigrationState.COMPLETED,
            actor=ActorKind.RUNTIME,
            reason="rollback window closed; source may be decommissioned",
            evidence={"window_closed_at": window.closes_at.isoformat()},
        )
        return await self.status(migration.name)

    async def _cutover_position(self, name: str) -> SourcePosition | None:
        """The position the cutover recorded, recovered from its evidence."""
        for entry in reversed(await self._migrations.history(name)):
            if entry.to_state is not MigrationState.ROLLBACK_WINDOW or not entry.evidence:
                continue
            value = entry.evidence.get("position")
            kind = entry.evidence.get("position_kind")
            if value and kind:
                return SourcePosition(kind=PositionKind(str(kind)), value=str(value))
        return None

    async def _opened_at(self, name: str) -> datetime | None:
        """When the rollback window opened, from the trail rather than a field.

        The transition into ROLLBACK_WINDOW already records the instant. Storing
        it a second time on the migration row would give two answers to one
        question, and eventually they would disagree.
        """
        for entry in reversed(await self._migrations.history(name)):
            if entry.to_state is MigrationState.ROLLBACK_WINDOW:
                return entry.occurred_at
        return None

    async def pause(self, name: str, *, reason: str, actor_id: str | None = None) -> None:
        await self._migrations.transition(
            name,
            MigrationState.PAUSED,
            actor=ActorKind.OPERATOR,
            reason=reason,
            actor_id=actor_id,
        )

    async def fail(self, name: str, *, reason: str) -> None:
        await self._migrations.transition(
            name, MigrationState.FAILED, actor=ActorKind.RUNTIME, reason=reason
        )

    async def history(self, name: str) -> Sequence[MigrationTransition]:
        """Every transition, with who caused it. Append-only."""
        return await self._migrations.history(name)
