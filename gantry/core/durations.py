# SPDX-License-Identifier: Apache-2.0
"""Duration parsing for spec fields written for humans.

Specs express time as `2s`, `30m`, `24h`. Everything downstream works in
`timedelta`, so a lag threshold and a rollback window are the same kind of
value regardless of how they were written.
"""

from __future__ import annotations

import re
from datetime import timedelta

# Longest-first so `ms` is not read as `m` followed by junk.
_UNIT_SECONDS: dict[str, float] = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}

_DURATION_RE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ms|[smhdw])\s*$")


class DurationError(ValueError):
    """Raised when a duration string cannot be interpreted."""


def parse_duration(value: str | timedelta) -> timedelta:
    """Parse `2s`, `30m`, `24h`, `7d` into a `timedelta`.

    A bare number is rejected: an unqualified `30` in a spec is ambiguous, and
    guessing a unit is worse than asking.
    """
    if isinstance(value, timedelta):
        return value

    match = _DURATION_RE.match(value)
    if match is None:
        raise DurationError(
            f"cannot parse duration: {value!r} (expected a number and one of "
            f"{', '.join(sorted(_UNIT_SECONDS))}, e.g. '2s' or '24h')"
        )
    seconds = float(match.group("value")) * _UNIT_SECONDS[match.group("unit")]
    return timedelta(seconds=seconds)


def format_duration(value: timedelta) -> str:
    """Render a duration using the largest unit that divides it exactly."""
    total = value.total_seconds()
    if total < 0:
        raise DurationError(f"duration cannot be negative: {value}")
    for unit in ("w", "d", "h", "m", "s"):
        scale = _UNIT_SECONDS[unit]
        if total >= scale and total % scale == 0:
            return f"{int(total / scale)}{unit}"
    return f"{int(total * 1000)}ms"
