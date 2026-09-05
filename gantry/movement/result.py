"""The Result a Movement produces.

The first concrete instance of the Result abstraction, and it inherits the
provenance contract rather than inventing one: what a Movement moved is
traceable to the exact Dataset versions it read and the checkpoints it reached.
An Analysis Result will carry findings instead of row counts, and the same
provenance underneath.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import ConfigDict, Field, model_validator

from gantry.core.results import Result, ResultKind


class MovementResult(Result):
    """What a Movement moved, and what it is now known to be."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ResultKind = ResultKind.MOVEMENT

    rows_inserted: int = Field(default=0, ge=0)
    rows_updated: int = Field(default=0, ge=0)
    rows_unchanged: int = Field(default=0, ge=0)

    partitions_total: int = Field(default=0, ge=0)
    partitions_complete: int = Field(default=0, ge=0)
    partitions_verified: int = Field(default=0, ge=0)

    started_at: datetime
    finished_at: datetime

    @model_validator(mode="after")
    def _check_times(self) -> MovementResult:
        for label, value in (("started_at", self.started_at), ("finished_at", self.finished_at)):
            if value.tzinfo is None:
                raise ValueError(f"{label} must be timezone-aware")
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must not precede started_at")
        if self.partitions_complete > self.partitions_total:
            raise ValueError("more partitions complete than exist")
        return self

    @property
    def duration(self) -> timedelta:
        return self.finished_at - self.started_at

    @property
    def rows_moved(self) -> int:
        return self.rows_inserted + self.rows_updated

    @property
    def rows_per_second(self) -> float | None:
        seconds = self.duration.total_seconds()
        return None if seconds <= 0 else self.rows_moved / seconds

    @property
    def is_complete(self) -> bool:
        """Whether every planned partition finished.

        Deliberately separate from `is_trustworthy`: finishing every partition
        is not the same as having verified them, and conflating the two is how
        a migration gets declared successful because all the jobs ran.
        """
        return self.partitions_total > 0 and self.partitions_complete == self.partitions_total
