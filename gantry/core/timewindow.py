# SPDX-License-Identifier: Apache-2.0
"""Half-open time windows.

Analysis specs declare a window; verification and provenance record the window
actually read. Both use this type so a Result's window means one thing.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, model_validator


class TimeWindow(BaseModel):
    """A half-open interval `[start, end)`.

    Timezone-aware endpoints are required: a naive datetime in a provenance
    record is unresolvable after the fact.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    start: datetime
    end: datetime

    @model_validator(mode="after")
    def _check_bounds(self) -> TimeWindow:
        for label, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None:
                raise ValueError(f"{label} must be timezone-aware")
        if self.end <= self.start:
            raise ValueError(f"end ({self.end}) must be after start ({self.start})")
        return self

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def contains(self, moment: datetime) -> bool:
        """Half-open: the start instant is inside the window, the end instant is not."""
        if moment.tzinfo is None:
            raise ValueError("moment must be timezone-aware")
        return self.start <= moment < self.end
