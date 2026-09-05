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

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from gantry.core.operation import TERMINAL_STATES, OperationState
from gantry.lifecycle.migration import MigrationState
from gantry.lifecycle.states import ActorKind
from gantry.migration.model import Migration
from gantry.migration.prepare import PrepareReport
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
        await self._migrations.transition(
            migration.name,
            MigrationState.CATCHING_UP,
            actor=ActorKind.RUNTIME,
            reason="applying changes made during the snapshot",
        )
        await self._migrations.transition(
            migration.name,
            MigrationState.VERIFYING,
            actor=ActorKind.RUNTIME,
            reason="reconciling source against target",
        )
        return await self.status(migration.name)

    async def status(self, name: str) -> MigrationStatus:
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
        return MigrationStatus(record=record, movements=tuple(statuses))

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
