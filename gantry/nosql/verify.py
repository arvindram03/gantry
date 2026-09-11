# SPDX-License-Identifier: Apache-2.0
"""Trusted verification checks for MongoDB materializations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import ClassVar, Protocol, runtime_checkable

from gantry.verifier import CheckResult


@dataclass(frozen=True, slots=True)
class CollectionSnapshot:
    name: str
    metadata: Mapping[str, object] = field(default_factory=dict)
    fields: tuple[str, ...] = ()


@runtime_checkable
class DocumentCheck(Protocol):
    """A trusted check evaluated against destination metadata."""

    def evaluate(self, collection: CollectionSnapshot | None) -> CheckResult: ...


@dataclass(frozen=True, slots=True)
class DestinationExists:
    requires_document_count: ClassVar[bool] = False

    def evaluate(self, collection: CollectionSnapshot | None) -> CheckResult:
        exists = collection is not None
        return CheckResult(
            name="destination_exists",
            ok=exists,
            expected=True,
            actual=exists,
            message=None if exists else "materialized destination does not exist",
        )


@dataclass(frozen=True, slots=True)
class DocumentCount:
    requires_document_count: ClassVar[bool] = True
    minimum: int | None = None
    maximum: int | None = None

    def __post_init__(self) -> None:
        for name, value in (("minimum", self.minimum), ("maximum", self.maximum)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                raise TypeError(f"document count {name} must be an integer")
            if value is not None and value < 0:
                raise ValueError(f"document count {name} must not be negative")
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("document count minimum must not exceed maximum")

    def evaluate(self, collection: CollectionSnapshot | None) -> CheckResult:
        actual_value = None if collection is None else collection.metadata.get("document_count")
        if not isinstance(actual_value, int) or isinstance(actual_value, bool):
            return CheckResult(
                "document_count",
                False,
                {"min": self.minimum, "max": self.maximum},
                actual_value,
                "destination document count is unavailable",
            )
        actual = actual_value
        ok = True
        if self.minimum is not None:
            ok = actual >= self.minimum
        if ok and self.maximum is not None:
            ok = actual <= self.maximum
        expected = {"min": self.minimum, "max": self.maximum}
        message = (
            None if ok else f"destination document count {actual} is outside the accepted range"
        )
        return CheckResult("document_count", ok, expected, actual, message)


@dataclass(frozen=True, slots=True)
class RequiredFields:
    """Field presence within a bounded document sample.

    Fields absent from every sampled document read as missing even if present
    in unsampled documents — a documented limitation, not a bug.
    """

    requires_document_count: ClassVar[bool] = False
    fields: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.fields:
            raise ValueError("required fields must not be empty")
        if any(not isinstance(f, str) for f in self.fields):
            raise TypeError("required fields must contain only strings")
        if any(not f.strip() for f in self.fields):
            raise ValueError("required fields must not contain empty names")
        object.__setattr__(self, "fields", tuple(f.lower() for f in self.fields))

    def evaluate(self, collection: CollectionSnapshot | None) -> CheckResult:
        actual = () if collection is None else collection.fields
        present = {f.lower() for f in actual}
        missing = tuple(f for f in self.fields if f not in present)
        return CheckResult(
            name="required_fields",
            ok=not missing,
            expected=self.fields,
            actual=actual,
            message=None if not missing else f"required fields are missing: {', '.join(missing)}",
        )


def destination_exists() -> DestinationExists:
    return DestinationExists()


def document_count(*, min: int | None = None, max: int | None = None) -> DocumentCount:  # noqa: A002
    return DocumentCount(minimum=min, maximum=max)


def required_fields(fields: Sequence[str]) -> RequiredFields:
    return RequiredFields(tuple(fields))


__all__ = [
    "CollectionSnapshot",
    "DestinationExists",
    "DocumentCheck",
    "DocumentCount",
    "RequiredFields",
    "destination_exists",
    "document_count",
    "required_fields",
]
