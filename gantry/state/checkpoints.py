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

from gantry.core.positions import Checkpoint, CheckpointScope


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
