# SPDX-License-Identifier: Apache-2.0
"""Durable state for the Migration workflow.

The same shape as `OperationStore`, deliberately: optimistic concurrency on a
row version, validation before the write so an illegal transition never
reaches the audit trail, and an append-only history. A cutover is the last
place to discover that two actors both advanced the workflow.

What this store does **not** hold is execution state. Partition progress,
checkpoints and plan versions belong to the Operations beneath the workflow
and stay there. If this file grows a checkpoint table, the composition claim
has failed.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Row, and_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.lifecycle.migration import MigrationState, check_transition, requires_operator
from gantry.lifecycle.states import ActorKind
from gantry.migration.model import Migration
from gantry.state.database import transaction
from gantry.state.tables import migration_transitions, migrations


class UnknownMigrationError(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(f"no migration named {name!r}")
        self.name = name


class ConcurrentMigrationUpdateError(Exception):
    """Two actors advanced one Migration at once."""

    def __init__(self, name: str) -> None:
        super().__init__(f"migration {name!r} changed underneath this update; re-read it and retry")
        self.name = name


class ApprovalRequiredError(Exception):
    """An agent proposed a transition only an operator may make.

    Agents propose; they do not execute (RFC 0 §9.3). Cutting over moves
    production traffic and rolling back moves it again — the refusal is in the
    transition function rather than in a policy that could be relaxed, for the
    same reason the access ladder is a function call rather than a prompt.
    """

    def __init__(self, name: str, requested: MigrationState, actor: ActorKind) -> None:
        super().__init__(
            f"migration {name!r}: {requested.value} is an operator decision; "
            f"a {actor.value} may propose it but not perform it"
        )
        self.name = name
        self.requested = requested
        self.actor = actor


class UnattributedDecisionError(Exception):
    """An operator transition arrived without an identity.

    A cutover approved by "operator" and nobody in particular is not an
    approval, it is a checkbox.
    """

    def __init__(self, name: str, requested: MigrationState) -> None:
        super().__init__(
            f"migration {name!r}: {requested.value} needs an actor_id naming who decided"
        )
        self.name = name
        self.requested = requested


@dataclass(frozen=True)
class MigrationRecord:
    name: str
    state: MigrationState
    movements: tuple[str, ...]
    row_version: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class MigrationTransition:
    migration: str
    from_state: MigrationState
    to_state: MigrationState
    actor: ActorKind
    actor_id: str | None
    reason: str
    evidence: dict[str, object] | None
    occurred_at: datetime

    def describe(self) -> str:
        who = f"{self.actor.value}" + (f" ({self.actor_id})" if self.actor_id else "")
        return (
            f"{self.occurred_at.isoformat(timespec='seconds')}  "
            f"{self.from_state.value} -> {self.to_state.value}  {who}  {self.reason}"
        )


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MigrationStore:
    """Reads and advances Migration workflow state."""

    def __init__(self, engine: AsyncEngine, clock: Callable[[], datetime] | None = None) -> None:
        self._engine = engine
        self._clock: Callable[[], datetime] = clock or _utc_now

    async def ensure(self, migration: Migration) -> MigrationRecord:
        """Create the Migration if it is new, leaving an existing one alone.

        Re-submitting a spec must not reset a workflow that is already partway
        through a cutover.
        """
        now = self._clock()
        async with transaction(self._engine) as connection:
            await connection.execute(
                insert(migrations)
                .values(
                    name=migration.name,
                    state=MigrationState.DRAFT.value,
                    movements=list(migration.movements),
                    spec=migration.model_dump(mode="json"),
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_nothing(index_elements=["name"])
            )
        return await self.get(migration.name)

    async def get(self, name: str) -> MigrationRecord:
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(select(migrations).where(migrations.c.name == name))
            ).one_or_none()
        if row is None:
            raise UnknownMigrationError(name)
        return _to_record(row)

    async def spec(self, name: str) -> Migration:
        """The Migration as submitted, rebuilt from the stored document."""
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(select(migrations).where(migrations.c.name == name))
            ).one_or_none()
        if row is None:
            raise UnknownMigrationError(name)
        return Migration.model_validate(row._mapping["spec"])

    async def list(self) -> Sequence[MigrationRecord]:
        async with transaction(self._engine) as connection:
            rows = (await connection.execute(select(migrations).order_by(migrations.c.name))).all()
        return tuple(_to_record(row) for row in rows)

    async def transition(
        self,
        name: str,
        to_state: MigrationState,
        *,
        actor: ActorKind,
        reason: str,
        actor_id: str | None = None,
        evidence: dict[str, object] | None = None,
    ) -> MigrationTransition:
        """Advance the workflow, recording who moved it and why."""
        current = await self.get(name)
        check_transition(name, current.state, to_state)

        if requires_operator(to_state):
            if actor is not ActorKind.OPERATOR:
                raise ApprovalRequiredError(name, to_state, actor)
            if not (actor_id or "").strip():
                raise UnattributedDecisionError(name, to_state)

        occurred_at = self._clock()
        async with transaction(self._engine) as connection:
            result = await connection.execute(
                update(migrations)
                .where(
                    and_(
                        migrations.c.name == name,
                        migrations.c.row_version == current.row_version,
                    )
                )
                .values(
                    state=to_state.value,
                    updated_at=occurred_at,
                    row_version=current.row_version + 1,
                )
            )
            if result.rowcount == 0:
                raise ConcurrentMigrationUpdateError(name)

            await connection.execute(
                migration_transitions.insert().values(
                    migration=name,
                    from_state=current.state.value,
                    to_state=to_state.value,
                    actor=actor.value,
                    actor_id=actor_id,
                    reason=reason,
                    evidence=evidence,
                    occurred_at=occurred_at,
                )
            )

        return MigrationTransition(
            migration=name,
            from_state=current.state,
            to_state=to_state,
            actor=actor,
            actor_id=actor_id,
            reason=reason,
            evidence=evidence,
            occurred_at=occurred_at,
        )

    async def history(self, name: str) -> Sequence[MigrationTransition]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(migration_transitions)
                    .where(migration_transitions.c.migration == name)
                    .order_by(migration_transitions.c.occurred_at, migration_transitions.c.id)
                )
            ).all()
        return tuple(
            MigrationTransition(
                migration=row._mapping["migration"],
                from_state=MigrationState(row._mapping["from_state"]),
                to_state=MigrationState(row._mapping["to_state"]),
                actor=ActorKind(row._mapping["actor"]),
                actor_id=row._mapping["actor_id"],
                reason=row._mapping["reason"],
                evidence=row._mapping["evidence"],
                occurred_at=row._mapping["occurred_at"],
            )
            for row in rows
        )


def _to_record(row: Row[tuple[object, ...]]) -> MigrationRecord:
    mapping = row._mapping
    return MigrationRecord(
        name=mapping["name"],
        state=MigrationState(mapping["state"]),
        movements=tuple(mapping["movements"] or ()),
        row_version=mapping["row_version"],
        created_at=mapping["created_at"],
        updated_at=mapping["updated_at"],
    )
