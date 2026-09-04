"""Spec parsing and validation.

Owns the YAML surface: document shape, aliases, deprecated spellings and error
reporting. Converts to core domain types and holds no runtime state.

Movement and Analysis are siblings over a shared `OperationSpec` base, and both
carry the same verification vocabulary.
"""

from gantry.spec.analysis import AnalysisSpec, ExecutionEngine
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
from gantry.spec.loader import (
    SUPPORTED_KINDS,
    AnySpec,
    dataset_json_schema,
    load_analysis_spec,
    load_dataset_spec,
    load_movement_spec,
    load_spec,
    spec_json_schema,
)
from gantry.spec.movement import MovementSpec, OrderingScope, StrategyMode
from gantry.spec.operation import OperationKind, OperationSpec
from gantry.spec.verification import CheckName, VerificationRequirement

__all__ = [
    "CANONICAL_API_VERSION",
    "SUPPORTED_API_VERSIONS",
    "SUPPORTED_KINDS",
    "AnalysisSpec",
    "AnySpec",
    "CheckName",
    "DatasetSpec",
    "ExecutionEngine",
    "MovementSpec",
    "OperationKind",
    "OperationSpec",
    "OrderingScope",
    "SpecError",
    "SpecValidationError",
    "StrategyMode",
    "UnsupportedApiVersionError",
    "UnsupportedKindError",
    "VerificationRequirement",
    "dataset_json_schema",
    "is_deprecated_api_version",
    "load_analysis_spec",
    "load_dataset_spec",
    "load_movement_spec",
    "load_spec",
    "normalize_api_version",
    "spec_json_schema",
]
