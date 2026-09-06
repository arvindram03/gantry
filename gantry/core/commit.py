# SPDX-License-Identifier: Apache-2.0
"""Evidence that a write was durably committed.

Design document section 8.2: a checkpoint may only advance after the side
effect it attests to is durably committed. `CommitResult` is that attestation.
The worker takes one before advancing a checkpoint, so the ordering is enforced
by what the code needs rather than by remembering to do it in the right order.

The row breakdown exists for a specific reason: a replay of an identical batch
must report every row unchanged. That is how idempotency is observed rather
than assumed.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CommitResult(BaseModel):
    """The outcome of one durable write."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rows_inserted: int = Field(default=0, ge=0)
    rows_updated: int = Field(default=0, ge=0)
    # Rows that were already present and identical. A replay produces these and
    # nothing else.
    rows_unchanged: int = Field(default=0, ge=0)
    # Rows the target refused because they were older than what it holds.
    # Always zero until stale-write rejection arrives with CDC.
    rows_rejected_stale: int = Field(default=0, ge=0)
    # The content hash of the job that did this work, when a job did it.
    # Opaque here on purpose: the runtime records which job ran without
    # learning what kind it was, which is what keeps the worker from knowing
    # anything about executors.
    job: str | None = None
    committed_at: datetime

    @model_validator(mode="after")
    def _require_tz(self) -> CommitResult:
        if self.committed_at.tzinfo is None:
            raise ValueError("committed_at must be timezone-aware")
        return self

    @property
    def rows_changed(self) -> int:
        return self.rows_inserted + self.rows_updated

    @property
    def rows_seen(self) -> int:
        return self.rows_changed + self.rows_unchanged + self.rows_rejected_stale

    @property
    def is_noop(self) -> bool:
        """True when the write changed nothing, as a replay must not."""
        return self.rows_changed == 0
