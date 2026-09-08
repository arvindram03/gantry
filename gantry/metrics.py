# SPDX-License-Identifier: Apache-2.0
"""Portable, optional execution metrics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ExecutionMetrics:
    rows_read: int | None = None
    rows_written: int | None = None
    bytes_read: int | None = None
    bytes_written: int | None = None
    runtime_seconds: float | None = None
    estimated_cost_usd: float | None = None
    worker_count: int | None = None
    native: Mapping[str, object] = field(default_factory=dict)
