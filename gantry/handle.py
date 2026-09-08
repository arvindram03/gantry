# SPDX-License-Identifier: Apache-2.0
"""Durable execution identity."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class ExecutionHandle:
    gantry_id: str
    engine: str
    target: str
    native_id: str
    submitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        values = {
            "gantry_id": self.gantry_id,
            "engine": self.engine,
            "target": self.target,
            "native_id": self.native_id,
        }
        for field_name, value in values.items():
            if not value.strip():
                raise ValueError(f"execution handle {field_name} must not be empty")
        if self.submitted_at.tzinfo is None:
            raise ValueError("execution handle submitted_at must be timezone-aware")
