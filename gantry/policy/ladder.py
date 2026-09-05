"""The progressive access ladder.

The RFC's ordering, as a type:

    describe -> profile -> aggregate/query -> partition -> sample -> exact records

Each rung is more expensive and more revealing than the one above it, and the
ordering is the point: an agent starts at the top and earns its way down, so
the default posture is metadata rather than rows.

The rungs are an `IntEnum` because the ordering is load-bearing - a policy
grants access *down to* a rung, and comparing rungs is how that is expressed.
"""

from __future__ import annotations

from enum import IntEnum


class AccessRung(IntEnum):
    """One step on the ladder, ordered from least to most revealing."""

    # What the Dataset is: schema, keys, physical reference.
    DESCRIBE = 1
    # What is in it, in aggregate: row counts, null rates, distinct counts.
    PROFILE = 2
    # A grouped aggregate over it.
    QUERY = 3
    # How it is split, including the key values at partition boundaries.
    PARTITION = 4
    # A bounded number of raw rows.
    SAMPLE = 5
    # Raw rows, addressed by key, unbounded.
    RECORDS = 6

    @property
    def returns_values(self) -> bool:
        """Whether this rung can put field values in front of the caller.

        `DESCRIBE` returns names and types. Everything below it can carry data
        — a profile's most-common values as readily as a sample's rows — which
        is why redaction applies from `PROFILE` down.
        """
        return self >= AccessRung.PROFILE

    @property
    def returns_rows(self) -> bool:
        """Whether this rung returns individual records rather than summaries."""
        return self >= AccessRung.SAMPLE

    def describe(self) -> str:
        return self.name.lower()
