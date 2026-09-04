"""Verification requirements.

Domain vocabulary shared by Movement and Analysis. Movement checks compare a
source against a target; Analysis checks bound a computation's output. They
share one model and one evaluator, with different verifiers behind them.

The YAML spellings that map onto these live in the spec layer - this module has
no opinion about how a requirement was written down.
"""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from gantry.core.durations import DurationError, parse_duration
from gantry.core.names import FieldName


class CheckName(StrEnum):
    """Every check the runtime knows how to evaluate.

    Movement checks compare a source against a target. Analysis checks bound a
    computation's output. They share a lifecycle stage, not an implementation.
    """

    # Movement
    ROW_COUNT = "row_count"
    CHUNK_CHECKSUM = "chunk_checksum"
    PRIMARY_KEY_UNIQUE = "primary_key_unique"
    FOREIGN_KEY_INTEGRITY = "foreign_key_integrity"
    # Analysis
    ROW_EXPANSION = "row_expansion"
    JOIN_COVERAGE = "join_coverage"
    TEMPORAL_ALIGNMENT = "temporal_alignment"
    # Either
    NULL_RATE = "null_rate"


class _Requirement(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)


class RowCountCheck(_Requirement):
    """Source and target row counts must agree."""

    check: Literal[CheckName.ROW_COUNT] = CheckName.ROW_COUNT


class ChunkChecksumCheck(_Requirement):
    """Key-ranged checksums must agree, computed in the engine."""

    check: Literal[CheckName.CHUNK_CHECKSUM] = CheckName.CHUNK_CHECKSUM


class PrimaryKeyUniqueCheck(_Requirement):
    check: Literal[CheckName.PRIMARY_KEY_UNIQUE] = CheckName.PRIMARY_KEY_UNIQUE


class ForeignKeyIntegrityCheck(_Requirement):
    check: Literal[CheckName.FOREIGN_KEY_INTEGRITY] = CheckName.FOREIGN_KEY_INTEGRITY


class RowExpansionCheck(_Requirement):
    """Bounds how far a join may multiply rows.

    The clearest expression of the guarantee boundary: an engine can report
    success on a join that expands 84M rows to 1.7B, and this check is what
    rejects it.
    """

    check: Literal[CheckName.ROW_EXPANSION] = CheckName.ROW_EXPANSION
    max: float = Field(gt=0)


class JoinCoverageCheck(_Requirement):
    """Minimum fraction of left rows that must find a match."""

    check: Literal[CheckName.JOIN_COVERAGE] = CheckName.JOIN_COVERAGE
    min: float = Field(ge=0, le=1)


class NullRateCheck(_Requirement):
    """Maximum fraction of nulls permitted in a field."""

    check: Literal[CheckName.NULL_RATE] = CheckName.NULL_RATE
    field: FieldName
    max: float = Field(ge=0, le=1)


class TemporalAlignmentCheck(_Requirement):
    """Maximum time skew tolerated between joined signals."""

    check: Literal[CheckName.TEMPORAL_ALIGNMENT] = CheckName.TEMPORAL_ALIGNMENT
    max_difference: timedelta = Field(alias="maxDifference")

    @field_validator("max_difference", mode="before")
    @classmethod
    def _parse(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except DurationError as exc:
                raise ValueError(str(exc)) from exc
        return value


VerificationRequirement = Annotated[
    RowCountCheck
    | ChunkChecksumCheck
    | PrimaryKeyUniqueCheck
    | ForeignKeyIntegrityCheck
    | RowExpansionCheck
    | JoinCoverageCheck
    | NullRateCheck
    | TemporalAlignmentCheck,
    Field(discriminator="check"),
]

PARAMETERLESS_CHECKS: frozenset[str] = frozenset(
    {
        CheckName.ROW_COUNT,
        CheckName.CHUNK_CHECKSUM,
        CheckName.PRIMARY_KEY_UNIQUE,
        CheckName.FOREIGN_KEY_INTEGRITY,
    }
)
