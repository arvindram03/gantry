"""Provenance: why we believe a Result.

RFC Provenance: every finding must trace to the Analysis version, the
computation, intermediate artifacts, Dataset manifest versions, source
partitions and time ranges, and the Movement checkpoint state where relevant.

These types exist from the start rather than being bolted on later - a
provenance chain assembled after the fact has holes in it.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.dataset import DatasetVersion
from gantry.core.names import ContentHash, ResourceName
from gantry.core.positions import Checkpoint
from gantry.core.timewindow import TimeWindow


class ArtifactKind(StrEnum):
    """Generated, retained work products (RFC: Generate)."""

    SQL = "sql"
    PLAN = "plan"
    JOB_SPEC = "job_spec"
    CONNECTOR_CONFIG = "connector_config"
    INTERMEDIATE_DATASET = "intermediate_dataset"


class ArtifactRef(BaseModel):
    """A versioned generated artifact, referenced by content hash.

    Generated code is provenance, not a transient string: an artifact that
    cannot be reproduced cannot support a claim.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ArtifactKind
    reference: str
    content_hash: ContentHash | None = None


class DatasetPin(BaseModel):
    """An exact Dataset version that was read.

    Always pinned - `DatasetRef` may float to latest, a pin may not.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    version: int = Field(ge=1)
    content_hash: ContentHash

    @classmethod
    def from_version(cls, version: DatasetVersion) -> DatasetPin:
        return cls(
            name=version.name,
            version=version.version,
            content_hash=version.content_hash,
        )

    def __str__(self) -> str:
        return f"{self.name}@{self.version}"


class Lineage(BaseModel):
    """Which Datasets went in, and which came out."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    inputs: tuple[DatasetPin, ...] = ()
    output: DatasetPin | None = None


class Provenance(BaseModel):
    """The evidence trail attached to every Result."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    generated_at: datetime
    operation: ResourceName | None = None
    plan_version: int | None = Field(default=None, ge=1)
    lineage: Lineage = Lineage()
    artifacts: tuple[ArtifactRef, ...] = ()
    checkpoints: tuple[Checkpoint, ...] = ()
    window: TimeWindow | None = None

    @model_validator(mode="after")
    def _require_tz(self) -> Provenance:
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must be timezone-aware")
        return self
