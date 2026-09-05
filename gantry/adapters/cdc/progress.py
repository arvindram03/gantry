"""Recording how far a change stream has been applied.

Both positions live in Gantry's checkpoint store, for different jobs:

- the **stream position** says where to resume reading
- the **source LSN** says how old the data is, which is what stale-write
  rejection compares against

Keeping them together, in the same store as every other checkpoint, is what
lets progress advance or not advance as one fact. Committing a Kafka consumer
offset instead would put resumption in a different durability domain from the
target write, and reintroduce the two-phase commit the design avoids.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from gantry.core.changes import StreamPosition
from gantry.core.positions import Checkpoint, CheckpointScope, PositionKind, SourcePosition
from gantry.state.checkpoints import CheckpointStore

# Scope ids are prefixed so a stream checkpoint cannot collide with a partition
# checkpoint that happens to share a name.
_STREAM_PREFIX = "stream:"
_APPLIED_PREFIX = "applied:"


class CDCProgress:
    """Reads and advances a change stream's recorded position."""

    def __init__(self, checkpoints: CheckpointStore, operation: str) -> None:
        self._checkpoints = checkpoints
        self._operation = operation

    async def record(
        self, position: StreamPosition, *, source_lsn: int, committed_at: datetime
    ) -> None:
        """Record progress, after the change it describes is durably applied."""
        await self._checkpoints.advance(
            self._operation,
            Checkpoint(
                scope=CheckpointScope.STREAM,
                scope_id=f"{_STREAM_PREFIX}{position.topic}:{position.partition}",
                position=SourcePosition(kind=PositionKind.OFFSET, value=str(position.offset)),
                committed_at=committed_at,
            ),
        )
        await self._checkpoints.advance(
            self._operation,
            Checkpoint(
                scope=CheckpointScope.STREAM,
                scope_id=f"{_APPLIED_PREFIX}{position.topic}",
                position=SourcePosition(kind=PositionKind.LSN, value=str(source_lsn)),
                committed_at=committed_at,
            ),
        )

    async def resume_position(self, topic: str, partition: int = 0) -> StreamPosition | None:
        """Where to start reading, or None if this stream has never been applied."""
        checkpoint = await self._checkpoints.get(
            self._operation,
            CheckpointScope.STREAM,
            f"{_STREAM_PREFIX}{topic}:{partition}",
        )
        if checkpoint is None:
            return None
        return StreamPosition(
            topic=topic, partition=partition, offset=int(checkpoint.position.value)
        )

    async def applied_lsn(self, topic: str) -> int | None:
        """The newest source LSN applied from this topic."""
        checkpoint = await self._checkpoints.get(
            self._operation, CheckpointScope.STREAM, f"{_APPLIED_PREFIX}{topic}"
        )
        return None if checkpoint is None else int(checkpoint.position.value)

    async def stream_checkpoints(self) -> Sequence[Checkpoint]:
        return tuple(
            checkpoint
            for checkpoint in await self._checkpoints.all(self._operation)
            if checkpoint.scope is CheckpointScope.STREAM
        )
