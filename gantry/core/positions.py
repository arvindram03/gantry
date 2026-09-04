"""Source positions and checkpoints.

Spec section 8.2: a checkpoint is durable evidence of committed progress, and
must only advance after the side effect it attests to is durably committed.
This module carries the types; the worker loop enforces the ordering.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator


class PositionKind(StrEnum):
    """How a source expresses progress.

    Values are compared only within a kind - an LSN and a Kafka offset are not
    mutually orderable, and the runtime must never assume otherwise.
    """

    LSN = "lsn"
    OFFSET = "offset"
    TIMESTAMP = "timestamp"
    KEY_HIGH_WATER = "key_high_water"
    PARTITION_ID = "partition_id"
    OPAQUE = "opaque"


class CheckpointScope(StrEnum):
    """What a checkpoint covers. Spec section 8.5 requires replay from each of these."""

    OPERATION = "operation"
    DATASET = "dataset"
    PARTITION = "partition"
    STREAM = "stream"


class SourcePosition(BaseModel):
    """An adapter-supplied position, kept opaque on purpose.

    The runtime stores and compares positions but does not interpret them;
    interpretation belongs to the adapter that produced them.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: PositionKind
    value: str

    def comparable_with(self, other: SourcePosition) -> bool:
        return self.kind is other.kind


class Checkpoint(BaseModel):
    """Durable evidence that work up to `position` was committed.

    `committed_at` records when the underlying side effect was confirmed
    durable, not when this record was constructed.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    scope: CheckpointScope
    scope_id: str
    position: SourcePosition
    committed_at: datetime

    @model_validator(mode="after")
    def _require_tz(self) -> Checkpoint:
        if self.committed_at.tzinfo is None:
            raise ValueError("committed_at must be timezone-aware")
        return self
