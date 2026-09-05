# SPDX-License-Identifier: Apache-2.0
"""Byte-size parsing for spec fields written for humans.

Dataset manifests carry sizes like `14.2TB`. Specs stay readable; everything
downstream works in integer bytes.
"""

from __future__ import annotations

import re

# Decimal units for SI suffixes, binary units for IEC suffixes. Conflating the
# two is a common source of quietly wrong capacity estimates.
_UNITS: dict[str, int] = {
    "B": 1,
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
    "TB": 10**12,
    "PB": 10**15,
    "KIB": 2**10,
    "MIB": 2**20,
    "GIB": 2**30,
    "TIB": 2**40,
    "PIB": 2**50,
}

_SIZE_RE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[A-Za-z]*)\s*$")


class ByteSizeError(ValueError):
    """Raised when a size string cannot be interpreted."""


def parse_byte_size(value: str | int) -> int:
    """Parse `14.2TB`, `512 MiB` or a bare integer into a count of bytes."""
    if isinstance(value, int):
        if value < 0:
            raise ByteSizeError(f"byte size cannot be negative: {value}")
        return value

    match = _SIZE_RE.match(value)
    if match is None:
        raise ByteSizeError(f"cannot parse byte size: {value!r}")

    unit = (match.group("unit") or "B").upper()
    if unit not in _UNITS:
        raise ByteSizeError(f"unknown size unit {unit!r} in {value!r} (known: {sorted(_UNITS)})")

    return int(float(match.group("value")) * _UNITS[unit])


def format_byte_size(value: int) -> str:
    """Render a byte count using the largest SI unit that keeps it readable."""
    if value < 0:
        raise ByteSizeError(f"byte size cannot be negative: {value}")
    for unit in ("PB", "TB", "GB", "MB", "KB"):
        scale = _UNITS[unit]
        if value >= scale:
            return f"{value / scale:.4g}{unit}"
    return f"{value}B"
