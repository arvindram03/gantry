# SPDX-License-Identifier: Apache-2.0
"""Flink-specific streaming metrics."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from gantry.metrics import ExecutionMetrics


@dataclass(frozen=True, slots=True)
class FlinkMetrics:
    records_in: int | None = None
    records_out: int | None = None
    runtime_seconds: float | None = None
    restart_count: int | None = None
    watermark_lag_seconds: float | None = None
    native: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def from_execution_metrics(cls, metrics: ExecutionMetrics) -> FlinkMetrics:
        restarts = metrics.native.get("restart_count")
        lag = metrics.native.get("watermark_lag_seconds")
        return cls(
            records_in=metrics.rows_read,
            records_out=metrics.rows_written,
            runtime_seconds=metrics.runtime_seconds,
            restart_count=restarts if isinstance(restarts, int) else None,
            watermark_lag_seconds=(
                float(lag) if isinstance(lag, (int, float)) and not isinstance(lag, bool) else None
            ),
            native=metrics.native,
        )

    def to_execution_metrics(self) -> ExecutionMetrics:
        return ExecutionMetrics(
            rows_read=self.records_in,
            rows_written=self.records_out,
            runtime_seconds=self.runtime_seconds,
            native={
                **self.native,
                "records_in": self.records_in,
                "records_out": self.records_out,
                "restart_count": self.restart_count,
                "watermark_lag_seconds": self.watermark_lag_seconds,
            },
        )
