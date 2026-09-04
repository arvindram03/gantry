"""The `kind: Dataset` spec.

Mirrors the manifest format in the design document, then converts to the core
`DatasetManifest`. The spec layer owns YAML shape and back-compatibility; the
core layer owns the domain model. Keeping them separate means spec spellings
can change without disturbing everything downstream.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from gantry.core.dataset import (
    AccessPolicy,
    AgentAccessPolicy,
    DatasetManifest,
    DatasetStatistics,
    PhysicalRef,
)
from gantry.core.names import FieldName, ResourceName
from gantry.core.schema import DatasetSchema, FieldSchema
from gantry.core.sizes import ByteSizeError, parse_byte_size
from gantry.spec.apiversion import CANONICAL_API_VERSION, normalize_api_version

KIND = "Dataset"


class MetadataBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName


class PhysicalBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    adapter: str
    reference: str
    estimated_rows: int | None = Field(default=None, alias="estimatedRows", ge=0)
    # Written for humans as `14.2TB`; stored as bytes.
    estimated_bytes: str | int | None = Field(default=None, alias="estimatedBytes")

    # A field validator, not a model validator: the error path must name
    # `physical.estimatedBytes` rather than just `physical`.
    @field_validator("estimated_bytes")
    @classmethod
    def _check_size(cls, value: str | int | None) -> str | int | None:
        if value is not None:
            try:
                parse_byte_size(value)
            except ByteSizeError as exc:
                raise ValueError(str(exc)) from exc
        return value

    def bytes_value(self) -> int | None:
        return None if self.estimated_bytes is None else parse_byte_size(self.estimated_bytes)


class FieldBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: FieldName
    type: str
    nullable: bool = True


class SchemaBlock(BaseModel):
    """The manifest's `schema:` block.

    The design document names the time field twice - `schema.timestamp` and
    `semantics.timeField`. `semantics.timeField` is canonical; `schema.timestamp`
    is accepted as an alias, and a conflict between the two is an error rather
    than a silent precedence rule.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    timestamp: FieldName | None = None
    keys: tuple[FieldName, ...] = ()
    fields: tuple[FieldBlock, ...] = ()


class SemanticsBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    time_field: FieldName | None = Field(default=None, alias="timeField")


class StatisticsBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    change_rate_per_second: float | None = Field(default=None, alias="changeRatePerSecond", ge=0)
    row_count: int | None = Field(default=None, alias="rowCount", ge=0)


class AccessBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    agent_policy: AgentAccessPolicy = Field(
        default=AgentAccessPolicy.AGGREGATE_OR_MASKED, alias="agentPolicy"
    )


class DatasetSpec(BaseModel):
    """A parsed `kind: Dataset` document."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    api_version: str = Field(alias="apiVersion")
    kind: Literal["Dataset"]
    metadata: MetadataBlock
    physical: PhysicalBlock
    dataset_schema: SchemaBlock = Field(default=SchemaBlock(), alias="schema")
    statistics: StatisticsBlock = StatisticsBlock()
    semantics: SemanticsBlock = SemanticsBlock()
    access: AccessBlock = AccessBlock()
    sensitive_fields: tuple[FieldName, ...] = Field(default=(), alias="sensitiveFields")

    @model_validator(mode="after")
    def _check_document(self) -> DatasetSpec:
        if normalize_api_version(self.api_version) != CANONICAL_API_VERSION:
            raise ValueError(
                f"apiVersion must be {CANONICAL_API_VERSION!r}, got {self.api_version!r}"
            )

        declared = {
            source: value
            for source, value in (
                ("schema.timestamp", self.dataset_schema.timestamp),
                ("semantics.timeField", self.semantics.time_field),
            )
            if value is not None
        }
        if len(set(declared.values())) > 1:
            pairs = ", ".join(f"{k}={v!r}" for k, v in sorted(declared.items()))
            raise ValueError(f"conflicting time field declarations: {pairs}")
        return self

    @property
    def time_field(self) -> FieldName | None:
        """Resolved time field, from either accepted spelling."""
        return self.semantics.time_field or self.dataset_schema.timestamp

    def to_manifest(self) -> DatasetManifest:
        """Convert to the immutable core manifest."""
        return DatasetManifest(
            name=self.metadata.name,
            physical=PhysicalRef(
                adapter=self.physical.adapter,
                reference=self.physical.reference,
                estimated_rows=self.physical.estimated_rows,
                estimated_bytes=self.physical.bytes_value(),
            ),
            dataset_schema=DatasetSchema(
                keys=self.dataset_schema.keys,
                time_field=self.time_field,
                fields=tuple(
                    FieldSchema(name=f.name, type=f.type, nullable=f.nullable)
                    for f in self.dataset_schema.fields
                ),
            ),
            statistics=DatasetStatistics(
                change_rate_per_second=self.statistics.change_rate_per_second,
                row_count=self.statistics.row_count,
            ),
            access=AccessPolicy(agent_policy=self.access.agent_policy),
            sensitive_fields=self.sensitive_fields,
        )
