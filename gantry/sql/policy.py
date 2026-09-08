# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SQLPolicy:
    read_only: bool = True
    allowed_schemas: Collection[str] = frozenset()
    allowed_tables: Collection[str] = frozenset()
    denied_tables: Collection[str] = frozenset()
    max_rows: int = 1_000
    timeout_seconds: float = 30
    max_bytes_scanned: int | None = None
    max_cost_usd: float | None = None
    allow_multiple_statements: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.read_only, bool):
            raise TypeError("read_only must be a boolean")
        if not isinstance(self.allow_multiple_statements, bool):
            raise TypeError("allow_multiple_statements must be a boolean")
        for field_name in ("allowed_schemas", "allowed_tables", "denied_tables"):
            values = getattr(self, field_name)
            if isinstance(values, str):
                raise TypeError(f"{field_name} must be a collection of names, not a string")
            if any(not isinstance(value, str) for value in values):
                raise TypeError(f"{field_name} must contain only strings")
            if any(not value.strip() for value in values):
                raise ValueError(f"{field_name} must not contain empty names")
            object.__setattr__(self, field_name, frozenset(value.lower() for value in values))
        if isinstance(self.max_rows, bool) or not isinstance(self.max_rows, int):
            raise TypeError("max rows must be an integer")
        if self.max_rows <= 0:
            raise ValueError("max rows must be positive")
        if isinstance(self.timeout_seconds, bool) or not isinstance(
            self.timeout_seconds, (int, float)
        ):
            raise TypeError("timeout must be numeric")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        if self.max_bytes_scanned is not None and (
            isinstance(self.max_bytes_scanned, bool) or not isinstance(self.max_bytes_scanned, int)
        ):
            raise TypeError("max bytes scanned must be an integer")
        if self.max_bytes_scanned is not None and self.max_bytes_scanned < 0:
            raise ValueError("max bytes scanned must not be negative")
        if self.max_cost_usd is not None and (
            isinstance(self.max_cost_usd, bool) or not isinstance(self.max_cost_usd, (int, float))
        ):
            raise TypeError("max cost must be numeric")
        if self.max_cost_usd is not None and self.max_cost_usd < 0:
            raise ValueError("max cost must not be negative")
