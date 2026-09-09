# SPDX-License-Identifier: Apache-2.0
"""Trusted verification checks for SQL materializations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar, Protocol, runtime_checkable

from gantry.sql.schema import Table
from gantry.verifier import CheckResult


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


def row_count(*, min: int | None = None, max: int | None = None) -> RowCount:  # noqa: A002
    return RowCount(minimum=min, maximum=max)


def required_columns(columns: Sequence[str]) -> RequiredColumns:
    return RequiredColumns(tuple(columns))


__all__ = [
    "DestinationExists",
    "MaterializationCheck",
    "RequiredColumns",
    "RowCount",
    "destination_exists",
    "required_columns",
    "row_count",
]
