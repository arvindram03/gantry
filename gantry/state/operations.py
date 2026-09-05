# SPDX-License-Identifier: Apache-2.0
"""Operation state and its audit trail.

Every state change goes through `transition`, which validates the move against
the state machine, records who caused it and why, and updates the operation row
under optimistic concurrency. There is no other path: an operation whose state
changed without an audit row would be a state nobody can account for.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Row, and_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.operation import OperationState, OperationType
from gantry.lifecycle.states import ActorKind, StateTransition, transition
from gantry.state.database import transaction
from gantry.state.tables import operations, state_transitions


class ConcurrentUpdateError(Exception):
    """Raised when another worker changed the operation first."""

    def __init__(self, name: str) -> None:
        super().__init__(f"operation {name!r} was modified concurrently; re-read and retry")


class UnknownOperationError(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(f"operation {name!r} does not exist")


@dataclass(frozen=True)
class OperationRecord:
    name: str
    operation_type: OperationType
    state: OperationState
    current_plan_version: int | None
    row_version: int
    created_at: datetime
    updated_at: datetime


def _utc_now() -> datetime:
    return datetime.now(UTC)


class OperationStore:
    """Reads and advances operation state."""

    def __init__(self, engine: AsyncEngine, clock: Callable[[], datetime] | None = None) -> None:
        self._engine = engine
        self._clock: Callable[[], datetime] = clock or _utc_now

    async def ensure(
        self, name: str, operation_type: OperationType, *, plan_version: int | None = None
    ) -> OperationRecord:
        """Create the operation if it is new, leaving an existing one alone.

        Starting an operation again must not reset its state; a restart is not
        a new migration.
        """
        now = self._clock()
        async with transaction(self._engine) as connection:
            await connection.execute(
                insert(operations)
                .values(
                    name=name,
                    operation_type=operation_type.value,
                    state=OperationState.DRAFT.value,
                    current_plan_version=plan_version,
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_nothing(index_elements=["name"])
            )
        return await self.get(name)

    async def get(self, name: str) -> OperationRecord:
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(select(operations).where(operations.c.name == name))
            ).one_or_none()
        if row is None:
            raise UnknownOperationError(name)
        return _to_record(row)

    async def list(self) -> Sequence[OperationRecord]:
        async with transaction(self._engine) as connection:
            rows = (await connection.execute(select(operations).order_by(operations.c.name))).all()
        return tuple(_to_record(row) for row in rows)

    async def transition(
        self,
        name: str,
        to_state: OperationState,
        *,
        actor: ActorKind,
        reason: str,
        plan_version: int | None = None,
    ) -> StateTransition:
        """Move an operation to a new state, recording why.

        Validation happens before the write, so an illegal transition never
        reaches the database and never appears in the audit trail.
        """
        current = await self.get(name)
        record = transition(
            name,
            current.state,
            to_state,
            actor=actor,
            reason=reason,
            occurred_at=self._clock(),
            plan_version=plan_version or current.current_plan_version,
        )

        async with transaction(self._engine) as connection:
            result = await connection.execute(
                update(operations)
                .where(
                    and_(
                        operations.c.name == name,
                        operations.c.row_version == current.row_version,
                    )
                )
                .values(
                    state=to_state.value,
                    updated_at=record.occurred_at,
                    row_version=current.row_version + 1,
                    **({"current_plan_version": plan_version} if plan_version is not None else {}),
                )
            )
            if result.rowcount == 0:
                raise ConcurrentUpdateError(name)

            await connection.execute(
                state_transitions.insert().values(
                    operation=name,
                    from_state=record.from_state.value,
                    to_state=record.to_state.value,
                    actor=record.actor.value,
                    reason=record.reason,
                    plan_version=record.plan_version,
                    occurred_at=record.occurred_at,
                )
            )
        return record

    async def history(self, name: str) -> Sequence[StateTransition]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(state_transitions)
                    .where(state_transitions.c.operation == name)
                    .order_by(state_transitions.c.occurred_at, state_transitions.c.id)
                )
            ).all()
        return tuple(
            StateTransition(
                operation=row._mapping["operation"],
                from_state=OperationState(row._mapping["from_state"]),
                to_state=OperationState(row._mapping["to_state"]),
                actor=ActorKind(row._mapping["actor"]),
                reason=row._mapping["reason"],
                plan_version=row._mapping["plan_version"],
                occurred_at=row._mapping["occurred_at"],
            )
            for row in rows
        )


def _to_record(row: Row[tuple[object, ...]]) -> OperationRecord:
    mapping = row._mapping
    return OperationRecord(
        name=mapping["name"],
        operation_type=OperationType(mapping["operation_type"]),
        state=OperationState(mapping["state"]),
        current_plan_version=mapping["current_plan_version"],
        row_version=mapping["row_version"],
        created_at=mapping["created_at"],
        updated_at=mapping["updated_at"],
    )
