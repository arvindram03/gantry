# SPDX-License-Identifier: Apache-2.0
"""Declarative verification checks and safe agent-input parsing."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

from gantry.sql.schema import Table
from gantry.verifier import CheckResult, CheckSource

if TYPE_CHECKING:
    from gantry.flink.verification import (
        JobRunning,
        JobSucceeded,
        MaxRestartCount,
        MaxWatermarkLag,
    )


@runtime_checkable
class MaterializationCheck(Protocol):
    """A trusted check evaluated against a table's metadata.

    The same checks serve a materialization and a query. A materialization is
    checked against the destination it created; a query is checked against the
    shape of the rows it returned, described as a table so one check can do
    both. Two of them do need a destination and say so with
    `requires_destination`, which makes them unsupported on a query rather than
    quietly true.
    """

    def evaluate(self, table: Table | None) -> CheckResult: ...


class VerificationInputError(ValueError):
    """An agent supplied a check outside the declarative verification DSL."""


class VerificationUnsupported(VerificationInputError):  # noqa: N818
    """A valid check is not supported by this governed operation."""


class VerificationConflict(VerificationInputError):  # noqa: N818
    """The trusted and agent commitments cannot both be satisfied."""


@dataclass(frozen=True, slots=True)
class DestinationExists:
    requires_row_count: ClassVar[bool] = False
    requires_destination: ClassVar[bool] = True

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
    requires_destination: ClassVar[bool] = True

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
                "row count is unavailable",
                supported=False,
            )
        actual = actual_value
        ok = True
        if self.minimum is not None:
            ok = actual >= self.minimum
        if ok and self.maximum is not None:
            ok = actual <= self.maximum
        expected = {"min": self.minimum, "max": self.maximum}
        message = None if ok else f"row count {actual} is outside the accepted range"
        return CheckResult("row_count", ok, expected, actual, message)


@dataclass(frozen=True, slots=True)
class DocumentCount(RowCount):
    """MongoDB spelling of a result-size assertion."""

    requires_document_count: ClassVar[bool] = True

    def evaluate(self, table: object | None) -> CheckResult:
        metadata = getattr(table, "metadata", None)
        actual = metadata.get("document_count") if isinstance(metadata, Mapping) else None
        if not isinstance(actual, int) or isinstance(actual, bool):
            return CheckResult(
                "document_count",
                False,
                {"min": self.minimum, "max": self.maximum},
                actual,
                "document count is unavailable",
                supported=False,
            )
        ok = (self.minimum is None or actual >= self.minimum) and (
            self.maximum is None or actual <= self.maximum
        )
        return CheckResult(
            "document_count",
            ok,
            {"min": self.minimum, "max": self.maximum},
            actual,
            None if ok else f"document count {actual} is outside the accepted range",
        )


@dataclass(frozen=True, slots=True)
class NotEmpty:
    requires_row_count: ClassVar[bool] = True
    allows_truncated: ClassVar[bool] = True

    requires_document_count: ClassVar[bool] = True

    def evaluate(self, table: object | None) -> CheckResult:
        metadata = getattr(table, "metadata", None)
        actual = None
        if isinstance(metadata, Mapping):
            actual = metadata.get("rows", metadata.get("document_count"))
        if not isinstance(actual, int) or isinstance(actual, bool):
            return CheckResult(
                "not_empty", False, {"min": 1}, actual, "row count is unavailable", supported=False
            )
        return CheckResult(
            "not_empty",
            actual > 0,
            {"min": 1},
            actual,
            None if actual > 0 else "result is empty",
        )


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
            expected={"columns": list(self.columns)},
            actual={"columns": list(actual)},
            message=None if not missing else f"required columns are missing: {', '.join(missing)}",
        )


@dataclass(frozen=True, slots=True)
class RequiredFields(RequiredColumns):
    """MongoDB spelling of a required-shape assertion."""

    def evaluate(self, table: object | None) -> CheckResult:
        actual = () if table is None else tuple(getattr(table, "fields", ()))
        present = {field.lower() for field in actual}
        missing = tuple(field for field in self.columns if field not in present)
        return CheckResult(
            name="required_fields",
            ok=not missing,
            expected={"fields": list(self.columns)},
            actual={"fields": list(actual)},
            message=None if not missing else f"required fields are missing: {', '.join(missing)}",
        )


def destination_exists() -> DestinationExists:
    return DestinationExists()


def output_exists() -> OutputExists:
    return OutputExists()


def row_count(*, min: int | None = None, max: int | None = None) -> RowCount:  # noqa: A002
    return RowCount(minimum=min, maximum=max)


def document_count(
    *,
    min: int | None = None,  # noqa: A002
    max: int | None = None,  # noqa: A002
) -> DocumentCount:
    return DocumentCount(minimum=min, maximum=max)


def not_empty() -> NotEmpty:
    return NotEmpty()


def null_rate(column: str, *, max: float) -> NullRate:  # noqa: A002
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
                "there is nothing to measure",
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


def required_fields(fields: Sequence[str]) -> RequiredFields:
    return RequiredFields(tuple(fields))


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


_CONSTRUCTORS: dict[str, Callable[[Mapping[object, object]], object]] = {
    "not_empty": lambda item: not_empty(),
    "row_count": lambda item: row_count(
        min=_optional_int(item, "min"), max=_optional_int(item, "max")
    ),
    "document_count": lambda item: document_count(
        min=_optional_int(item, "min"), max=_optional_int(item, "max")
    ),
    "required_columns": lambda item: required_columns(_string_list(item, "columns")),
    "required_fields": lambda item: required_fields(_string_list(item, "fields")),
    "null_rate": lambda item: null_rate(column=_string(item, "column"), max=_number(item, "max")),
    "output_exists": lambda item: output_exists(),
    "destination_exists": lambda item: destination_exists(),
    "running": lambda item: running(),
    "restart_count": lambda item: restart_count(max=_integer(item, "max")),
    "watermark_lag": lambda item: watermark_lag(max_seconds=_duration(item, "max_seconds")),
}

_ALLOWED_KEYS = {
    "not_empty": {"type"},
    "row_count": {"type", "min", "max"},
    "document_count": {"type", "min", "max"},
    "required_columns": {"type", "columns"},
    "required_fields": {"type", "fields"},
    "null_rate": {"type", "column", "max"},
    "output_exists": {"type"},
    "destination_exists": {"type"},
    "running": {"type"},
    "restart_count": {"type", "max"},
    "watermark_lag": {"type", "max_seconds"},
}


def parse_agent_checks(
    values: object,
    *,
    capabilities: Collection[str],
) -> tuple[object, ...]:
    """Parse untrusted JSON into allowlisted check objects, failing closed."""
    if values is None:
        return ()
    if not isinstance(values, Sequence) or isinstance(values, str | bytes):
        raise VerificationInputError("verify must be an array of declarative checks")
    checks: list[object] = []
    for index, value in enumerate(values):
        if not isinstance(value, Mapping):
            raise VerificationInputError(f"verify[{index}] must be an object")
        check_type = value.get("type")
        if not isinstance(check_type, str) or check_type not in _CONSTRUCTORS:
            raise VerificationInputError(f"verify[{index}] has an unknown check type")
        if check_type not in capabilities:
            raise VerificationUnsupported(
                f"verification check {check_type!r} is unsupported by this operation"
            )
        unexpected = set(value) - _ALLOWED_KEYS[check_type]
        if unexpected:
            names = ", ".join(sorted(str(key) for key in unexpected))
            raise VerificationInputError(f"verify[{index}] has unexpected fields: {names}")
        try:
            checks.append(_CONSTRUCTORS[check_type](value))
        except (TypeError, ValueError) as error:
            raise VerificationInputError(f"invalid {check_type} check: {error}") from error
    return tuple(checks)


def validate_agent_checks(
    values: Sequence[object], *, capabilities: Collection[str]
) -> tuple[object, ...]:
    """Validate direct-call check objects without allowing callbacks."""
    supported = set(capabilities)
    checks = tuple(values)
    for check in checks:
        name = check_type(check)
        if name is None:
            raise VerificationInputError(
                "agent verify must contain gantry.verify declarative checks"
            )
        if name not in supported:
            raise VerificationUnsupported(
                f"verification check {name!r} is unsupported by this operation"
            )
    return checks


def check_type(check: object) -> str | None:
    from gantry.flink.verification import JobRunning, MaxRestartCount, MaxWatermarkLag
    from gantry.nosql.verify import (
        DestinationExists as NoSQLDestinationExists,
    )
    from gantry.nosql.verify import (
        DocumentCount as NoSQLDocumentCount,
    )
    from gantry.nosql.verify import (
        RequiredFields as NoSQLRequiredFields,
    )

    types = {
        NotEmpty: "not_empty",
        RowCount: "row_count",
        DocumentCount: "document_count",
        RequiredColumns: "required_columns",
        RequiredFields: "required_fields",
        NullRate: "null_rate",
        OutputExists: "output_exists",
        DestinationExists: "destination_exists",
        JobRunning: "running",
        MaxRestartCount: "restart_count",
        MaxWatermarkLag: "watermark_lag",
        NoSQLDestinationExists: "destination_exists",
        NoSQLDocumentCount: "document_count",
        NoSQLRequiredFields: "required_fields",
    }
    # Subclasses (DocumentCount/RequiredFields) must win over their bases.
    return next((name for cls, name in reversed(tuple(types.items())) if type(check) is cls), None)


def check_config(check: object) -> dict[str, object]:
    """Return the bounded public commitment, never provenance or executable state."""
    kind = check_type(check)
    if kind is None:
        return {"type": type(check).__name__}
    payload: dict[str, object] = {"type": kind}
    if kind in {"row_count", "document_count"}:
        payload.update(
            {
                "min": getattr(check, "minimum", None),
                "max": getattr(check, "maximum", None),
            }
        )
    elif kind in {"required_columns", "required_fields"}:
        values = getattr(check, "columns", getattr(check, "fields", ()))
        payload["fields" if kind == "required_fields" else "columns"] = list(values)
    elif isinstance(check, NullRate):
        payload.update({"column": check.column, "max": check.maximum})
    elif kind == "restart_count":
        payload["max"] = getattr(check, "maximum")  # noqa: B009
    elif kind == "watermark_lag":
        payload["max_seconds"] = getattr(check, "seconds")  # noqa: B009
    return {key: value for key, value in payload.items() if value is not None}


def verification_schema(capabilities: Collection[str]) -> dict[str, object]:
    """JSON Schema for only the checks supported by one operation."""
    variants = [_check_schema(name) for name in sorted(capabilities)]
    if not variants:
        return {"type": "array", "maxItems": 0}
    return {"type": "array", "items": {"oneOf": variants}}


def detect_conflicts(trusted: Sequence[object], agent: Sequence[object]) -> None:
    """Reject statically impossible count commitments before execution."""
    trusted_bounds = _count_bounds(trusted)
    agent_bounds = _count_bounds(agent)
    for family in ("rows", "documents"):
        trusted_min, trusted_max = trusted_bounds[family]
        agent_min, agent_max = agent_bounds[family]
        minimum = max(value for value in (trusted_min, agent_min, 0) if value is not None)
        maxima = [value for value in (trusted_max, agent_max) if value is not None]
        maximum = min(maxima) if maxima else None
        if maximum is not None and minimum > maximum:
            raise VerificationConflict(
                f"verification conflict: {family} minimum {minimum} exceeds maximum {maximum}"
            )


def sourced_result(
    result: CheckResult,
    source: CheckSource,
    *,
    observation_source: str,
) -> CheckResult:
    metadata = {**result.metadata, "observation_source": observation_source}
    refs = result.evidence_refs or (f"observation:{result.name}",)
    return replace(result, source=source, metadata=metadata, evidence_refs=refs)


def _count_bounds(checks: Sequence[object]) -> dict[str, tuple[int | None, int | None]]:
    result: dict[str, tuple[int | None, int | None]] = {
        "rows": (None, None),
        "documents": (None, None),
    }
    for check in checks:
        family: str | None = None
        minimum: int | None = None
        maximum: int | None = None
        if isinstance(check, NotEmpty):
            for not_empty_family in ("rows", "documents"):
                old_min, old_max = result[not_empty_family]
                result[not_empty_family] = (
                    max(value for value in (old_min, 1) if value is not None),
                    old_max,
                )
            continue
        elif check_type(check) == "document_count":
            family = "documents"
            minimum = getattr(check, "minimum", None)
            maximum = getattr(check, "maximum", None)
        elif type(check) is RowCount:
            family, minimum, maximum = "rows", check.minimum, check.maximum
        if family is None:
            continue
        old_min, old_max = result[family]
        result[family] = (
            max(value for value in (old_min, minimum) if value is not None)
            if old_min is not None or minimum is not None
            else None,
            min(value for value in (old_max, maximum) if value is not None)
            if old_max is not None or maximum is not None
            else None,
        )
    return result


def _string(item: Mapping[object, object], key: str) -> str:
    value = item.get(key)
    if not isinstance(value, str):
        raise TypeError(f"{key} must be a string")
    return value


def _optional_int(item: Mapping[object, object], key: str) -> int | None:
    value = item.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{key} must be an integer")
    return value


def _integer(item: Mapping[object, object], key: str) -> int:
    value = _optional_int(item, key)
    if value is None:
        raise TypeError(f"{key} is required")
    return value


def _number(item: Mapping[object, object], key: str) -> float:
    value = item.get(key)
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise TypeError(f"{key} must be numeric")
    return float(value)


def _duration(item: Mapping[object, object], key: str) -> float | str:
    value = item.get(key)
    if not isinstance(value, int | float | str) or isinstance(value, bool):
        raise TypeError(f"{key} must be numeric or a duration string")
    return float(value) if isinstance(value, int | float) else value


def _string_list(item: Mapping[object, object], key: str) -> tuple[str, ...]:
    value = item.get(key)
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise TypeError(f"{key} must be an array of strings")
    if any(not isinstance(entry, str) for entry in value):
        raise TypeError(f"{key} must contain only strings")
    return tuple(value)


def _check_schema(name: str) -> dict[str, object]:
    properties: dict[str, object] = {"type": {"const": name}}
    required = ["type"]
    if name in {"row_count", "document_count"}:
        properties.update(
            {
                "min": {"type": "integer", "minimum": 0},
                "max": {"type": "integer", "minimum": 0},
            }
        )
    elif name in {"required_columns", "required_fields"}:
        key = "columns" if name == "required_columns" else "fields"
        properties[key] = {"type": "array", "items": {"type": "string"}, "minItems": 1}
        required.append(key)
    elif name == "null_rate":
        properties.update(
            {
                "column": {"type": "string", "minLength": 1},
                "max": {"type": "number", "minimum": 0, "maximum": 1},
            }
        )
        required.extend(["column", "max"])
    elif name == "restart_count":
        properties["max"] = {"type": "integer", "minimum": 0}
        required.append("max")
    elif name == "watermark_lag":
        properties["max_seconds"] = {
            "anyOf": [{"type": "number", "minimum": 0}, {"type": "string"}]
        }
        required.append("max_seconds")
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


__all__ = [
    "DestinationExists",
    "DocumentCount",
    "MaterializationCheck",
    "NotEmpty",
    "NullRate",
    "OutputExists",
    "RequiredColumns",
    "RequiredFields",
    "RowCount",
    "VerificationConflict",
    "VerificationInputError",
    "VerificationUnsupported",
    "check_config",
    "check_type",
    "destination_exists",
    "detect_conflicts",
    "document_count",
    "job_succeeded",
    "not_empty",
    "null_rate",
    "output_exists",
    "parse_agent_checks",
    "required_columns",
    "required_fields",
    "restart_count",
    "row_count",
    "running",
    "sourced_result",
    "validate_agent_checks",
    "verification_schema",
    "watermark_lag",
]
