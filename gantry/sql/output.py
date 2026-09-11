# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class InlineRows:
    """Rows returned with the result, already bounded by the policy.

    `truncated` says the engine had more rows than `max_rows` allowed, so a
    caller can tell a complete small answer from a clipped large one.
    Construction rejects rows whose width does not match `columns`.
    """

    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    truncated: bool = False

    def __post_init__(self) -> None:
        width = len(self.columns)
        if any(len(row) != width for row in self.rows):
            raise ValueError("inline row width must match the column count")
