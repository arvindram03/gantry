"""Spec parsing and validation.

Owns the YAML surface: document shape, aliases, deprecated spellings and error
reporting. Converts to core domain types and holds no runtime state.
"""

from gantry.spec.apiversion import (
    CANONICAL_API_VERSION,
    SUPPORTED_API_VERSIONS,
    is_deprecated_api_version,
    normalize_api_version,
)
from gantry.spec.dataset import DatasetSpec
from gantry.spec.errors import (
    SpecError,
    SpecValidationError,
    UnsupportedApiVersionError,
    UnsupportedKindError,
)
from gantry.spec.loader import SUPPORTED_KINDS, dataset_json_schema, load_dataset_spec

__all__ = [
    "CANONICAL_API_VERSION",
    "SUPPORTED_API_VERSIONS",
    "SUPPORTED_KINDS",
    "DatasetSpec",
    "SpecError",
    "SpecValidationError",
    "UnsupportedApiVersionError",
    "UnsupportedKindError",
    "dataset_json_schema",
    "is_deprecated_api_version",
    "load_dataset_spec",
    "normalize_api_version",
]
