# SPDX-License-Identifier: Apache-2.0
"""Load and validate spec documents."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from pydantic import BaseModel, ValidationError

from gantry.spec.analysis import AnalysisSpec
from gantry.spec.dataset import DatasetSpec
from gantry.spec.errors import (
    SpecError,
    SpecValidationError,
    UnsupportedKindError,
)
from gantry.spec.migration import MigrationSpec
from gantry.spec.movement import MovementSpec
from gantry.spec.yaml_compat import safe_load

AnySpec = DatasetSpec | MovementSpec | AnalysisSpec | MigrationSpec

_SPEC_TYPES: dict[str, type[AnySpec]] = {
    "Dataset": DatasetSpec,
    "Movement": MovementSpec,
    "Analysis": AnalysisSpec,
    "Migration": MigrationSpec,
}

SUPPORTED_KINDS: tuple[str, ...] = tuple(_SPEC_TYPES)


def load_spec(source: Path | str, text: str | None = None) -> AnySpec:
    """Parse any spec document, dispatching on `kind`.

    `text` lets callers validate in-memory content while still reporting a
    meaningful source in errors.
    """
    raw = _read_document(source, text)
    kind = str(raw.get("kind"))
    spec_type = _SPEC_TYPES.get(kind)
    if spec_type is None:
        raise UnsupportedKindError(kind, SUPPORTED_KINDS)
    return _validate(source, raw, spec_type)


def load_dataset_spec(source: Path | str, text: str | None = None) -> DatasetSpec:
    """Parse a `kind: Dataset` document, rejecting any other kind."""
    return _load_as(source, text, "Dataset", DatasetSpec)


def load_movement_spec(source: Path | str, text: str | None = None) -> MovementSpec:
    """Parse a `kind: Movement` document, rejecting any other kind."""
    return _load_as(source, text, "Movement", MovementSpec)


def load_analysis_spec(source: Path | str, text: str | None = None) -> AnalysisSpec:
    """Parse a `kind: Analysis` document, rejecting any other kind."""
    return _load_as(source, text, "Analysis", AnalysisSpec)


def load_migration_spec(source: Path | str, text: str | None = None) -> MigrationSpec:
    """Parse a `kind: Migration` document, rejecting any other kind."""
    return _load_as(source, text, "Migration", MigrationSpec)


def _load_as[SpecT: BaseModel](
    source: Path | str,
    text: str | None,
    kind: str,
    spec_type: type[SpecT],
) -> SpecT:
    """Load a document of one expected kind.

    The declared kind is checked before validation: a Movement handed to the
    Dataset loader should report "expected Dataset", not a page of Movement
    field errors.
    """
    raw = _read_document(source, text)
    declared = str(raw.get("kind"))
    if declared != kind:
        raise UnsupportedKindError(declared, (kind,))
    return _validate(source, raw, spec_type)


def _validate[SpecT: BaseModel](
    source: Path | str, raw: dict[str, object], spec_type: type[SpecT]
) -> SpecT:
    try:
        return spec_type.model_validate(raw)
    except ValidationError as exc:
        raise SpecValidationError.from_pydantic(source, exc) from exc


def _read_document(source: Path | str, text: str | None) -> dict[str, object]:
    if text is None:
        path = Path(source)
        if not path.is_file():
            raise SpecError(f"{path}: no such file")
        text = path.read_text(encoding="utf-8")

    try:
        loaded: object = safe_load(text)
    except yaml.YAMLError as exc:
        raise SpecError(f"{source}: not valid YAML: {exc}") from exc

    if loaded is None:
        raise SpecError(f"{source}: document is empty")
    if not isinstance(loaded, dict):
        raise SpecError(f"{source}: document must be a mapping, got {type(loaded).__name__}")
    return {str(key): value for key, value in loaded.items()}


def spec_json_schema(kind: str) -> str:
    """Emit the JSON Schema for one spec kind.

    Generated from the models so a published schema cannot drift from what the
    runtime actually accepts. Never hand-edited.
    """
    spec_type = _SPEC_TYPES.get(kind)
    if spec_type is None:
        raise UnsupportedKindError(kind, SUPPORTED_KINDS)
    schema: dict[str, object] = spec_type.model_json_schema(by_alias=True)
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = f"Gantry {kind}"
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def dataset_json_schema() -> str:
    """Emit the Dataset JSON Schema."""
    return spec_json_schema("Dataset")
