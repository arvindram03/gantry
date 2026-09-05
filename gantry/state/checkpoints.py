"""Checkpoint storage.

Design document section 8.2: a checkpoint must only advance after the side
effect it attests to is durably committed. The store cannot enforce that on its
own - the worker's ordering does - but it keeps `committed_at` as the time the
effect was confirmed durable rather than the time this row was written, so an
inverted ordering is visible after the fact.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from sqlalchemy import Row, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.positions import Checkpoint, CheckpointScope, PositionKind, SourcePosition
from gantry.state.database import transaction
from gantry.state.tables import checkpoints


class CheckpointStore(Protocol):
    async def advance(self, operation: str, checkpoint: Checkpoint) -> None:
        """Record progress. Replaces any earlier checkpoint for the same scope."""
        ...

    async def get(
        self, operation: str, scope: CheckpointScope, scope_id: str
    ) -> Checkpoint | None: ...

    async def all(self, operation: str) -> Sequence[Checkpoint]: ...


class InMemoryCheckpointStore:
    """Checkpoints held in process memory."""

    def __init__(self) -> None:
        self._checkpoints: dict[tuple[str, str, str], Checkpoint] = {}

    async def advance(self, operation: str, checkpoint: Checkpoint) -> None:
        self._checkpoints[(operation, checkpoint.scope.value, checkpoint.scope_id)] = checkpoint

    async def get(self, operation: str, scope: CheckpointScope, scope_id: str) -> Checkpoint | None:
        return self._checkpoints.get((operation, scope.value, scope_id))

    async def all(self, operation: str) -> Sequence[Checkpoint]:
        return tuple(
            checkpoint
            for (op, _, _), checkpoint in sorted(self._checkpoints.items())
            if op == operation
        )


class PostgresCheckpointStore:
    """Checkpoints in the metadata store.

    Writes are upserts keyed by scope, so replaying a node overwrites its
    checkpoint rather than accumulating history. Progress is a current
    position, not a log; the log of what happened is the audit trail.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def advance(self, operation: str, checkpoint: Checkpoint) -> None:
        statement = (
            insert(checkpoints)
            .values(
                operation=operation,
                scope=checkpoint.scope.value,
                scope_id=checkpoint.scope_id,
                position_kind=checkpoint.position.kind.value,
                position_value=checkpoint.position.value,
                committed_at=checkpoint.committed_at,
            )
            .on_conflict_do_update(
                index_elements=["operation", "scope", "scope_id"],
                set_={
                    "position_kind": checkpoint.position.kind.value,
                    "position_value": checkpoint.position.value,
                    "committed_at": checkpoint.committed_at,
                },
            )
        )
        async with transaction(self._engine) as connection:
            await connection.execute(statement)

    async def get(self, operation: str, scope: CheckpointScope, scope_id: str) -> Checkpoint | None:
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(
                    select(checkpoints).where(
                        checkpoints.c.operation == operation,
                        checkpoints.c.scope == scope.value,
                        checkpoints.c.scope_id == scope_id,
                    )
                )
            ).one_or_none()
        return None if row is None else _to_checkpoint(row)

    async def all(self, operation: str) -> Sequence[Checkpoint]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(checkpoints)
                    .where(checkpoints.c.operation == operation)
                    .order_by(checkpoints.c.scope, checkpoints.c.scope_id)
                )
            ).all()
        return tuple(_to_checkpoint(row) for row in rows)


def _to_checkpoint(row: Row[tuple[object, ...]]) -> Checkpoint:
    mapping = row._mapping
    return Checkpoint(
        scope=CheckpointScope(mapping["scope"]),
        scope_id=mapping["scope_id"],
        position=SourcePosition(
            kind=PositionKind(mapping["position_kind"]), value=mapping["position_value"]
        ),
        committed_at=mapping["committed_at"],
    )
