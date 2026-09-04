"""The Movement domain model.

This is what the runtime plans and executes. It is deliberately not the spec
model: the spec layer owns YAML shape, aliases and deprecated spellings, and
converts into this. Changing spec format then costs one conversion function
rather than a change that reaches the planner, scheduler and state store.

The two genuinely differ. A spec may carry `cutover:` and `rollback:` for
compatibility; a Movement has no such fields, because a Movement does not imply
a cutover. Those belong to the Migration workflow.
"""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ContentHash, FieldName, ResourceName
from gantry.core.verification import VerificationRequirement


class MovementMode(StrEnum):
    SNAPSHOT = "snapshot"
    STREAM = "stream"
    SNAPSHOT_THEN_STREAM = "snapshot_then_stream"


class OrderingScope(StrEnum):
    GLOBAL = "global"
    PARTITION = "partition"
    KEY = "key"
    TRANSACTION = "transaction"
    NONE = "none"


class PartitionStrategy(StrEnum):
    RANGE = "range"
    TIME_RANGE = "time_range"
    HASH = "hash"


class WriteMode(StrEnum):
    UPSERT = "upsert"
    INSERT = "insert"
    MERGE = "merge"


class Endpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    adapter: str
    connection_ref: str


class Ordering(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    scope: OrderingScope
    version_field: FieldName | None = None


class Partitioning(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    strategy: PartitionStrategy
    column: FieldName
    rows_per_partition: int | None = None
    interval_seconds: int | None = None
    buckets: int | None = None


class MovementDataset(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    source: str
    target: str
    depends_on: tuple[ResourceName, ...] = ()
    key_columns: tuple[FieldName, ...] = Field(min_length=1)
    ordering: Ordering
    partitioning: Partitioning | None = None
    write_mode: WriteMode
    verification: tuple[VerificationRequirement, ...] = ()


class CdcConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    adapter: str
    checkpoint_type: str | None = None


class RuntimeLimits(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    max_concurrency: int | None = None
    max_retries: int | None = None
    source_rows_per_second: int | None = None
    target_rows_per_second: int | None = None
    target_cpu_max_percent: float | None = None
    source_cpu_max_percent: float | None = None


class Movement(BaseModel):
    """A reliable transfer or synchronisation, as the runtime sees it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    source: Endpoint
    destination: Endpoint
    mode: MovementMode
    datasets: tuple[MovementDataset, ...] = Field(min_length=1)
    cdc: CdcConfig | None = None
    limits: RuntimeLimits = RuntimeLimits()

    @model_validator(mode="after")
    def _check_datasets(self) -> Movement:
        names = [dataset.name for dataset in self.datasets]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate dataset names: {sorted(names)}")
        return self

    def dataset(self, name: str) -> MovementDataset | None:
        return next((d for d in self.datasets if d.name == name), None)

    def guarantee_fingerprint(self) -> ContentHash:
        """Hash of the guarantees that may not change without a replan.

        Design document section 10: source and target identity, ordering
        guarantee, migration key and verification requirements are immutable
        within a plan version. Tuning concurrency is a runtime adjustment;
        changing an ordering scope is a different migration wearing the same
        name.
        """
        payload = {
            "source": self.source.model_dump(mode="json"),
            "destination": self.destination.model_dump(mode="json"),
            "mode": self.mode.value,
            "datasets": [
                {
                    "name": dataset.name,
                    "source": dataset.source,
                    "target": dataset.target,
                    "key_columns": list(dataset.key_columns),
                    "ordering": dataset.ordering.model_dump(mode="json"),
                    "write_mode": dataset.write_mode.value,
                    "verification": [
                        check.model_dump(mode="json") for check in dataset.verification
                    ],
                }
                for dataset in self.datasets
            ],
        }
        rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return f"sha256:{hashlib.sha256(rendered.encode('utf-8')).hexdigest()}"
