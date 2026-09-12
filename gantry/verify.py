# SPDX-License-Identifier: Apache-2.0
"""Trusted verification checks for SQL materializations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

from gantry.sql.schema import Table
from gantry.verifier import CheckResult

if TYPE_CHECKING:
    from gantry.flink.verification import (
        JobRunning,
        JobSucceeded,
        MaxRestartCount,
        MaxWatermarkLag,
    )


@runtime_checkable
class MaterializationCheck(Protocol):
    """A trusted check evaluated against destination metadata."""

    def evaluate(self, table: Table | None) -> CheckResult: ...


@dataclass(frozen=True, slots=True)
class DestinationExists:
    requires_row_count: ClassVar[bool] = False

    def evaluate(self, table: Table | None) -> CheckResult:
        exists = table is not None
        return CheckResult(
            name="destination_exists",
            ok=exists,
            expected=True,
            actual=exists,
            message=None if exists else "materialized destination does not exist",
        )


@dataclass(frozen=True, slots=True)
class OutputExists:
    requires_row_count: ClassVar[bool] = False

    def evaluate(self, table: Table | None) -> CheckResult:
        exists = table is not None
        return CheckResult(
            name="output_exists",
            ok=exists,
            expected=True,
            actual=exists,
            message=None if exists else "Flink output does not exist",
        )


@dataclass(frozen=True, slots=True)
class RowCount:
    requires_row_count: ClassVar[bool] = True

    minimum: int | None = None
    maximum: int | None = None

    def __post_init__(self) -> None:
        for name, value in (("minimum", self.minimum), ("maximum", self.maximum)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                raise TypeError(f"row count {name} must be an integer")
            if value is not None and value < 0:
                raise ValueError(f"row count {name} must not be negative")
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("row count minimum must not exceed maximum")

    def evaluate(self, table: Table | None) -> CheckResult:
        actual_value = None if table is None else table.metadata.get("rows")
        if not isinstance(actual_value, int) or isinstance(actual_value, bool):
            return CheckResult(
                "row_count",
                False,
                {"min": self.minimum, "max": self.maximum},
                actual_value,
                "destination row count is unavailable",
                supported=False,
            )
        actual = actual_value
        ok = True
        if self.minimum is not None:
            ok = actual >= self.minimum
        if ok and self.maximum is not None:
            ok = actual <= self.maximum
        expected = {"min": self.minimum, "max": self.maximum}
        if not ok:
            message = f"destination row count {actual} is outside the accepted range"
        else:
            message = None
        return CheckResult("row_count", ok, expected, actual, message)


@dataclass(frozen=True, slots=True)
class RequiredColumns:
    requires_row_count: ClassVar[bool] = False

    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("required columns must not be empty")
        if any(not isinstance(column, str) for column in self.columns):
            raise TypeError("required columns must contain only strings")
        if any(not column.strip() for column in self.columns):
            raise ValueError("required columns must not contain empty names")
        object.__setattr__(self, "columns", tuple(column.lower() for column in self.columns))

    def evaluate(self, table: Table | None) -> CheckResult:
        actual = () if table is None else tuple(column.name for column in table.columns)
        present = {column.lower() for column in actual}
        missing = tuple(column for column in self.columns if column not in present)
        return CheckResult(
            name="required_columns",
            ok=not missing,
            expected=self.columns,
            actual=actual,
            message=None if not missing else f"required columns are missing: {', '.join(missing)}",
        )


def destination_exists() -> DestinationExists:
    return DestinationExists()


def output_exists() -> OutputExists:
    return OutputExists()


def row_count(*, min: int | None = None, max: int | None = None) -> RowCount:  # noqa: A002
    return RowCount(minimum=min, maximum=max)


def null_rate(*, column: str, max: float) -> NullRate:  # noqa: A002
    """Reject a destination where a column is emptier than it should be.

    The failure this catches is a query that runs, produces the right number of
    rows, and joins wrongly — so the column everyone downstream keys on is null
    in most of them. Row count says the table is fine. This does not.
    """
    return NullRate(column=column, maximum=max)


@dataclass(frozen=True, slots=True)
class NullRate:
    """The fraction of rows where one column is null, bounded above.

    Measured at the destination by the provider, not reported by the statement
    that wrote it. A provider that cannot measure it makes the check
    unsupported rather than passing it, because an unmeasured bound would be
    indistinguishable from a satisfied one.
    """

    requires_row_count: ClassVar[bool] = False

    column: str
    maximum: float

    def __post_init__(self) -> None:
        if not self.column.strip():
            raise ValueError("null rate column must not be empty")
        if isinstance(self.maximum, bool) or not isinstance(self.maximum, (int, float)):
            raise TypeError("null rate maximum must be numeric")
        if not 0 <= self.maximum <= 1:
            raise ValueError("null rate maximum must be a fraction between 0 and 1")

    @property
    def null_rate_columns(self) -> tuple[str, ...]:
        return (self.column,)

    def evaluate(self, table: Table | None) -> CheckResult:
        expected = {"column": self.column, "max": self.maximum}
        if table is None:
            return CheckResult(
                "null_rate",
                False,
                expected,
                None,
                "destination does not exist",
            )
        rates = table.metadata.get("null_rates")
        observed = rates.get(self.column) if isinstance(rates, Mapping) else None
        if not isinstance(observed, (int, float)) or isinstance(observed, bool):
            return CheckResult(
                "null_rate",
                False,
                expected,
                None,
                f"null rate for {self.column} is unavailable from this provider",
                supported=False,
            )
        ok = observed <= self.maximum
        return CheckResult(
            "null_rate",
            ok,
            expected,
            {"value": observed},
            None if ok else f"{self.column} is null in {observed:.1%} of rows",
        )


def required_columns(columns: Sequence[str]) -> RequiredColumns:
    return RequiredColumns(tuple(columns))


def job_succeeded() -> JobSucceeded:
    from gantry.flink.verification import JobSucceeded

    return JobSucceeded()


def running() -> JobRunning:
    from gantry.flink.verification import JobRunning

    return JobRunning()


def restart_count(*, max: int) -> MaxRestartCount:  # noqa: A002
    from gantry.flink.verification import MaxRestartCount

    return MaxRestartCount(max)


def watermark_lag(*, max_seconds: float | str) -> MaxWatermarkLag:
    from gantry.flink.verification import MaxWatermarkLag

    return MaxWatermarkLag(max_seconds)


__all__ = [
    "DestinationExists",
    "MaterializationCheck",
    "NullRate",
    "OutputExists",
    "RequiredColumns",
    "RowCount",
    "destination_exists",
    "job_succeeded",
    "null_rate",
    "output_exists",
    "required_columns",
    "restart_count",
    "row_count",
    "running",
    "watermark_lag",
]
