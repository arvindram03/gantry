"""Spec errors carrying YAML field paths.

A spec error is read by a human editing YAML, or by an agent repairing a
generated spec. Both need the path to the offending field, not a stack trace.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError


class SpecError(Exception):
    """Base class for every spec-layer failure."""


class UnsupportedKindError(SpecError):
    def __init__(self, kind: str, supported: Sequence[str]) -> None:
        super().__init__(f"unsupported kind {kind!r} (supported: {', '.join(sorted(supported))})")
        self.kind = kind


class UnsupportedApiVersionError(SpecError):
    def __init__(self, api_version: str, supported: Sequence[str]) -> None:
        super().__init__(
            f"unsupported apiVersion {api_version!r} (supported: {', '.join(sorted(supported))})"
        )
        self.api_version = api_version


class SpecValidationError(SpecError):
    """A spec that parsed as YAML but failed validation."""

    def __init__(self, source: Path | str, errors: Sequence[str]) -> None:
        joined = "\n".join(f"  {line}" for line in errors)
        super().__init__(f"{source}: invalid spec\n{joined}")
        self.source = str(source)
        self.errors = list(errors)

    @classmethod
    def from_pydantic(cls, source: Path | str, error: ValidationError) -> SpecValidationError:
        lines: list[str] = []
        for item in error.errors():
            path = ".".join(str(part) for part in item["loc"]) or "<root>"
            lines.append(f"{path}: {item['msg']}")
        return cls(source, lines)
