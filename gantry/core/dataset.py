"""The Dataset resource.

Spec (Core Resource Model, rev 1): Dataset is the first of four core resources
and is addressable in its own right, not a field nested inside another spec.
Both Movement and Analysis take Datasets as inputs.

Manifests are immutable and content-addressed. Registering changed content
creates a new version; a Result's provenance pins the version it read.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ContentHash, FieldName, ResourceName
from gantry.core.schema import DatasetSchema


class AgentAccessPolicy(StrEnum):
    """How much of a Dataset an agent may reach.

    Default is `aggregate_or_masked`, matching the RFC's agent access defaults
    (rows deny, aggregates allow, metadata allow). Enforcement lands with the
    progressive access ladder; the declaration lives here.
    """

    DENY = "deny"
    AGGREGATE_OR_MASKED = "aggregate_or_masked"
    ALLOW = "allow"


class AccessPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    agent_policy: AgentAccessPolicy = AgentAccessPolicy.AGGREGATE_OR_MASKED


class PhysicalRef(BaseModel):
    """Where the data actually lives.

    `reference` is adapter-specific (a qualified table, a topic, an object
    prefix) and is not parsed by the runtime.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    adapter: str
    reference: str
    estimated_rows: int | None = Field(default=None, ge=0)
    estimated_bytes: int | None = Field(default=None, ge=0)


class DatasetStatistics(BaseModel):
    """Profile output. Populated by discovery and profiling, empty until then.

    Everything here is estimated from sampled statistics rather than a full
    scan. A profile that has to read a 100M-row table to describe it is not a
    profile, it is a migration.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    change_rate_per_second: float | None = Field(default=None, ge=0)
    row_count: int | None = Field(default=None, ge=0)
    profiled_at: datetime | None = None

    # Partition planning needs the key range and how many distinct values sit
    # in it: equal key spans are not equal row counts when a key is skewed.
    key_min: str | None = None
    key_max: str | None = None
    distinct_keys: int | None = Field(default=None, ge=0)
    null_rates: dict[FieldName, float] = {}
    # Equi-depth boundaries per column, each interval holding roughly the same
    # number of rows. This is what makes partitioning skew-aware without
    # scanning: equal key spans are not equal row counts, and the database
    # already keeps a sample that knows the difference. Kept per column because
    # a dataset may be partitioned by its key or by a time field, and those are
    # rarely the same column.
    histograms: dict[FieldName, tuple[str, ...]] = {}
    # True when the planner's own statistics were missing or stale, so callers
    # can tell "no skew" from "no information".
    stale_statistics: bool = False


class DatasetManifest(BaseModel):
    """Immutable description of a Dataset.

    Deliberately carries no registration metadata: the content hash must depend
    on the description alone, so re-registering an unchanged manifest is a no-op
    rather than a new version.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    physical: PhysicalRef
    dataset_schema: DatasetSchema = DatasetSchema()
    statistics: DatasetStatistics = DatasetStatistics()
    access: AccessPolicy = AccessPolicy()
    sensitive_fields: tuple[FieldName, ...] = ()

    @model_validator(mode="after")
    def _check_sensitive_fields(self) -> DatasetManifest:
        if len(set(self.sensitive_fields)) != len(self.sensitive_fields):
            raise ValueError(f"duplicate sensitive fields: {self.sensitive_fields}")
        if not self.dataset_schema.fields:
            return self
        known = {f.name for f in self.dataset_schema.fields}
        unknown = [f for f in self.sensitive_fields if f not in known]
        if unknown:
            raise ValueError(f"sensitive fields not present in schema: {unknown}")
        return self

    def canonical_json(self) -> str:
        """Stable serialisation used for content addressing.

        Sorted keys and no insignificant whitespace, so the hash depends on
        content rather than on field ordering or formatting.

        Defaults are excluded so the payload carries only what was actually
        declared. That makes additive schema changes hash-stable: adding an
        optional field to this model would otherwise change every registered
        manifest's hash and re-version every Dataset in the registry.

        `statistics.profiled_at` is excluded for a different reason: it records
        when we looked, not what we saw. Hashing it would mint a new version on
        every profiling run of an unchanged table.
        """
        payload: dict[str, object] = self.model_dump(
            mode="json",
            exclude_defaults=True,
            exclude={"statistics": {"profiled_at"}},
        )
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @property
    def content_hash(self) -> ContentHash:
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"


class DatasetVersion(BaseModel):
    """A registered manifest at a point in time."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    version: int = Field(ge=1)
    manifest: DatasetManifest
    content_hash: ContentHash
    registered_at: datetime

    @model_validator(mode="after")
    def _check_integrity(self) -> DatasetVersion:
        if self.manifest.name != self.name:
            raise ValueError(f"manifest name {self.manifest.name!r} != version name {self.name!r}")
        if self.manifest.content_hash != self.content_hash:
            raise ValueError("content_hash does not match manifest content")
        if self.registered_at.tzinfo is None:
            raise ValueError("registered_at must be timezone-aware")
        return self

    def as_ref(self) -> DatasetRef:
        return DatasetRef(name=self.name, version=self.version)


class DatasetRef(BaseModel):
    """A reference to a Dataset, optionally pinned to a version.

    `version=None` resolves to the latest registered version. Specs may float;
    provenance never does.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    version: int | None = Field(default=None, ge=1)

    @property
    def is_pinned(self) -> bool:
        return self.version is not None

    def __str__(self) -> str:
        return self.name if self.version is None else f"{self.name}@{self.version}"
