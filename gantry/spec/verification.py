"""Verification requirements, shared by Movement and Analysis.

The design documents write these two different ways. A Movement lists bare
check names:

    verification:
      required: [row_count, chunk_checksum]

An Analysis lists parameterised checks:

    verify:
      - rowExpansion: {max: 1.1}
      - joinCoverage: {min: 0.95}

Both are the same thing: a list of requirements the runtime must satisfy before
a Result is trustworthy. Both spellings parse into one model, so there is one
schema and one evaluator with different verifiers behind it - rather than a
Movement verification path and a separate Analysis one.
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


# camelCase spellings from the Analysis examples map onto canonical names.
CHECK_ALIASES: dict[str, str] = {
    "rowCount": CheckName.ROW_COUNT,
    "chunkChecksum": CheckName.CHUNK_CHECKSUM,
    "primaryKeyUnique": CheckName.PRIMARY_KEY_UNIQUE,
    "foreignKeyIntegrity": CheckName.FOREIGN_KEY_INTEGRITY,
    "rowExpansion": CheckName.ROW_EXPANSION,
    "joinCoverage": CheckName.JOIN_COVERAGE,
    "temporalAlignment": CheckName.TEMPORAL_ALIGNMENT,
    "nullRate": CheckName.NULL_RATE,
}


def normalize_check_name(name: str) -> str:
    return CHECK_ALIASES.get(name, name)


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

_PARAMETERLESS: frozenset[str] = frozenset(
    {
        CheckName.ROW_COUNT,
        CheckName.CHUNK_CHECKSUM,
        CheckName.PRIMARY_KEY_UNIQUE,
        CheckName.FOREIGN_KEY_INTEGRITY,
    }
)


def normalize_requirement(entry: object) -> object:
    """Normalise one requirement entry into a taggable mapping.

    Accepts a bare name (`row_count`) or a single-key mapping carrying
    parameters (`{rowExpansion: {max: 1.1}}`), and leaves an already-tagged
    mapping alone so parsed specs round-trip.
    """
    if isinstance(entry, str):
        return {"check": normalize_check_name(entry)}

    if not isinstance(entry, dict):
        raise ValueError(
            f"verification entry must be a check name or a single-key mapping, "
            f"got {type(entry).__name__}"
        )

    if "check" in entry:
        tagged: dict[str, object] = {str(k): v for k, v in entry.items()}
        tagged["check"] = normalize_check_name(str(tagged["check"]))
        return tagged

    if len(entry) != 1:
        raise ValueError(
            f"verification entry must have exactly one check name, got {sorted(entry)}"
        )

    raw_name, params = next(iter(entry.items()))
    name = normalize_check_name(str(raw_name))
    if params is None:
        return {"check": name}
    if not isinstance(params, dict):
        raise ValueError(f"parameters for {name!r} must be a mapping, got {type(params).__name__}")
    if name in _PARAMETERLESS and params:
        raise ValueError(f"check {name!r} takes no parameters, got {sorted(params)}")
    return {"check": name, **{str(k): v for k, v in params.items()}}


def normalize_requirements(value: object) -> object:
    """Normalise a whole `verify` / `verification.required` list."""
    if not isinstance(value, list | tuple):
        return value
    return [normalize_requirement(entry) for entry in value]
