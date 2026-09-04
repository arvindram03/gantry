"""Load and validate spec documents."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from pydantic import ValidationError

from gantry.spec.dataset import DatasetSpec
from gantry.spec.errors import (
    SpecError,
    SpecValidationError,
    UnsupportedKindError,
)

# Movement and Analysis join this map on Day 3.
SUPPORTED_KINDS: tuple[str, ...] = ("Dataset",)


def load_dataset_spec(source: Path | str, text: str | None = None) -> DatasetSpec:
    """Parse a `kind: Dataset` document.

    `text` lets callers validate in-memory content while still reporting a
    meaningful source in errors.
    """
    raw = _read_document(source, text)
    kind = raw.get("kind")
    if kind != "Dataset":
        raise UnsupportedKindError(str(kind), SUPPORTED_KINDS)
    try:
        return DatasetSpec.model_validate(raw)
    except ValidationError as exc:
        raise SpecValidationError.from_pydantic(source, exc) from exc


def _read_document(source: Path | str, text: str | None) -> dict[str, object]:
    if text is None:
        path = Path(source)
        if not path.is_file():
            raise SpecError(f"{path}: no such file")
        text = path.read_text(encoding="utf-8")

    try:
        loaded: object = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise SpecError(f"{source}: not valid YAML: {exc}") from exc

    if loaded is None:
        raise SpecError(f"{source}: document is empty")
    if not isinstance(loaded, dict):
        raise SpecError(f"{source}: document must be a mapping, got {type(loaded).__name__}")
    return {str(key): value for key, value in loaded.items()}


def dataset_json_schema() -> str:
    """Emit the Dataset JSON Schema.

    Generated from the models so the published schema cannot drift from what
    the runtime actually accepts. Never hand-edited.
    """
    schema: dict[str, object] = DatasetSpec.model_json_schema(by_alias=True)
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = "Gantry Dataset"
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"
