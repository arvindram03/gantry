# SPDX-License-Identifier: Apache-2.0
"""Health checks for long-running Flink SQL jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from gantry.execution import Execution, ExecutionState
from gantry.flink.metrics import FlinkMetrics
from gantry.verifier import CheckResult, VerificationResult


@runtime_checkable
class FlinkHealthCheck(Protocol):
    def check(self, execution: Execution, metrics: FlinkMetrics) -> CheckResult: ...


@dataclass(frozen=True, slots=True)
class JobSucceeded:
    def check(self, execution: Execution, metrics: FlinkMetrics) -> CheckResult:
        del metrics
        ok = execution.state is ExecutionState.SUCCEEDED
        return CheckResult(
            "job_succeeded",
            ok,
            ExecutionState.SUCCEEDED.value,
            execution.state.value,
            None if ok else "Flink batch job did not succeed",
        )


@dataclass(frozen=True, slots=True)
class JobRunning:
    def check(self, execution: Execution, metrics: FlinkMetrics) -> CheckResult:
        del metrics
        ok = execution.state is ExecutionState.RUNNING
        return CheckResult(
            "running",
            ok,
            ExecutionState.RUNNING.value,
            execution.state.value,
            None if ok else "Flink job is not running",
        )


@dataclass(frozen=True, slots=True)
class MaxRestartCount:
    maximum: int

    def __post_init__(self) -> None:
        if isinstance(self.maximum, bool) or not isinstance(self.maximum, int):
            raise TypeError("maximum restarts must be an integer")
        if self.maximum < 0:
            raise ValueError("maximum restarts must not be negative")

    def check(self, execution: Execution, metrics: FlinkMetrics) -> CheckResult:
        del execution
        actual = metrics.restart_count
        ok = actual is not None and actual <= self.maximum
        return CheckResult(
            "restart_count",
            ok,
            f"<= {self.maximum}",
            actual,
            None if ok else "restart count is unavailable or exceeds the limit",
        )


@dataclass(frozen=True, slots=True)
class MaxWatermarkLag:
    seconds: float | str

    def __post_init__(self) -> None:
        seconds = _duration_seconds(self.seconds)
        if seconds < 0:
            raise ValueError("maximum watermark lag must not be negative")
        object.__setattr__(self, "seconds", seconds)

    def check(self, execution: Execution, metrics: FlinkMetrics) -> CheckResult:
        del execution
        actual = metrics.watermark_lag_seconds
        maximum = float(self.seconds)
        ok = actual is not None and actual <= maximum
        return CheckResult(
            "watermark_lag",
            ok,
            f"<= {maximum}s",
            actual,
            None if ok else "watermark lag is unavailable or exceeds the limit",
        )


@dataclass(frozen=True, slots=True)
class MinOutputRate:
    records_per_second: float

    def __post_init__(self) -> None:
        if isinstance(self.records_per_second, bool) or not isinstance(
            self.records_per_second, (int, float)
        ):
            raise TypeError("minimum output rate must be numeric")
        if self.records_per_second < 0:
            raise ValueError("minimum output rate must not be negative")

    def check(self, execution: Execution, metrics: FlinkMetrics) -> CheckResult:
        del execution
        value = metrics.native.get("output_rate")
        actual = (
            float(value)
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else None
        )
        ok = actual is not None and actual >= self.records_per_second
        return CheckResult(
            "min_output_rate",
            ok,
            f">= {self.records_per_second} records/s",
            actual,
            None if ok else "output rate is unavailable or below the minimum",
        )


def verify_health(
    execution: Execution,
    metrics: FlinkMetrics,
    checks: tuple[FlinkHealthCheck, ...],
) -> VerificationResult:
    results = tuple(check.check(execution, metrics) for check in checks)
    return VerificationResult(ok=all(result.ok for result in results), checks=results)


def _duration_seconds(value: float | str) -> float:
    if isinstance(value, bool):
        raise TypeError("duration must be numeric or use an s/m/h suffix")
    if isinstance(value, (int, float)):
        return float(value)
    normalized = value.strip().lower()
    multiplier = 1.0
    if normalized.endswith("ms"):
        multiplier = 0.001
        normalized = normalized[:-2]
    elif normalized.endswith("s"):
        normalized = normalized[:-1]
    elif normalized.endswith("m"):
        multiplier = 60.0
        normalized = normalized[:-1]
    elif normalized.endswith("h"):
        multiplier = 3600.0
        normalized = normalized[:-1]
    try:
        return float(normalized) * multiplier
    except ValueError as error:
        raise ValueError("duration must be numeric or use an ms/s/m/h suffix") from error
