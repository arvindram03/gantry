"""The `kind: Movement` spec.

A Movement makes or keeps a Dataset reliably available. It does not imply a
cutover: per the design document, a Movement may run once, periodically or
continuously, and cutover belongs to the Migration workflow built on top.
"""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from gantry.core.durations import DurationError, parse_duration
from gantry.core.names import FieldName, ResourceName
from gantry.core.verification import VerificationRequirement
from gantry.movement.model import CdcConfig as DomainCdc
from gantry.movement.model import Endpoint as DomainEndpoint
from gantry.movement.model import Movement as DomainMovement
from gantry.movement.model import MovementDataset as DomainDataset
from gantry.movement.model import MovementMode as DomainMovementMode
from gantry.movement.model import Ordering as DomainOrdering
from gantry.movement.model import OrderingScope as DomainOrderingScope
from gantry.movement.model import Partitioning as DomainPartitioning
from gantry.movement.model import PartitionStrategy as DomainPartitionStrategy
from gantry.movement.model import RuntimeLimits as DomainLimits
from gantry.movement.model import WriteMode as DomainWriteMode
from gantry.spec.operation import LimitsBlock, OperationSpec
from gantry.spec.verification import normalize_requirements

KIND = "Movement"


class StrategyMode(StrEnum):
    SNAPSHOT = "snapshot"
    STREAM = "stream"
    SNAPSHOT_THEN_STREAM = "snapshot_then_stream"


class OrderingScope(StrEnum):
    """Ordering boundaries from the design document's section 8.3.

    Ordering is scoped explicitly so a migration does not accidentally pay for
    global ordering it never needed.
    """

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


class EndpointBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    adapter: str
    # A reference to a secret, never a secret: design document section 16.
    connection_ref: str = Field(alias="connectionRef")


class StrategyBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: StrategyMode


class KeyBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    columns: tuple[FieldName, ...] = Field(min_length=1)


class OrderingBlock(BaseModel):
    """Ordering scope and the field used to reject stale writes.

    Scope defaults to `key`, and the resolved value is written into the plan -
    an implicit ordering guarantee is not a guarantee.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    scope: OrderingScope = OrderingScope.KEY
    version_field: FieldName | None = Field(default=None, alias="versionField")

    @model_validator(mode="after")
    def _check_stale_write_protection(self) -> OrderingBlock:
        if self.scope in (OrderingScope.KEY, OrderingScope.GLOBAL) and self.version_field is None:
            raise ValueError(
                f"ordering.scope {self.scope.value!r} requires ordering.versionField "
                f"so the target can reject stale writes"
            )
        return self


class PartitioningBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    strategy: PartitionStrategy
    column: FieldName
    rows_per_partition: int | None = Field(default=None, alias="rowsPerPartition", ge=1)
    interval: timedelta | None = None
    buckets: int | None = Field(default=None, ge=1)

    @field_validator("interval", mode="before")
    @classmethod
    def _parse_interval(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except DurationError as exc:
                raise ValueError(str(exc)) from exc
        return value

    @model_validator(mode="after")
    def _check_strategy_parameters(self) -> PartitioningBlock:
        required: dict[PartitionStrategy, tuple[str, object]] = {
            PartitionStrategy.RANGE: ("rowsPerPartition", self.rows_per_partition),
            PartitionStrategy.TIME_RANGE: ("interval", self.interval),
            PartitionStrategy.HASH: ("buckets", self.buckets),
        }
        field_name, value = required[self.strategy]
        if value is None:
            raise ValueError(f"partitioning.strategy {self.strategy.value!r} requires {field_name}")
        return self


class WriteBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: WriteMode = WriteMode.UPSERT


class DatasetVerificationBlock(BaseModel):
    """Per-dataset verification, in the Movement spelling."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    required: tuple[VerificationRequirement, ...] = ()

    @field_validator("required", mode="before")
    @classmethod
    def _normalize(cls, value: object) -> object:
        return normalize_requirements(value)


class MovementDatasetBlock(BaseModel):
    """One dataset moved by this Movement.

    `source` and `target` are physical references. Registering these as Dataset
    resources happens at plan time, so a Movement spec stays self-contained.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    name: ResourceName
    source: str
    target: str
    depends_on: tuple[ResourceName, ...] = Field(default=(), alias="dependsOn")
    key: KeyBlock
    ordering: OrderingBlock = OrderingBlock(scope=OrderingScope.NONE)
    partitioning: PartitioningBlock | None = None
    write: WriteBlock = WriteBlock()
    verification: DatasetVerificationBlock = DatasetVerificationBlock()

    @model_validator(mode="after")
    def _check_partition_column(self) -> MovementDatasetBlock:
        if self.name in self.depends_on:
            raise ValueError(f"dataset {self.name!r} cannot depend on itself")
        return self


class CheckpointBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    type: str


class CdcBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    adapter: str
    checkpoint: CheckpointBlock | None = None


class RateLimitsBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    source_rows_per_second: int | None = Field(default=None, alias="sourceRowsPerSecond", ge=1)
    target_rows_per_second: int | None = Field(default=None, alias="targetRowsPerSecond", ge=1)


class RuntimePoliciesBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    target_cpu_max_percent: float | None = Field(
        default=None, alias="targetCpuMaxPercent", gt=0, le=100
    )
    source_cpu_max_percent: float | None = Field(
        default=None, alias="sourceCpuMaxPercent", gt=0, le=100
    )


class RuntimeBlock(LimitsBlock):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    rate_limits: RateLimitsBlock = Field(default=RateLimitsBlock(), alias="rateLimits")
    policies: RuntimePoliciesBlock = RuntimePoliciesBlock()


class CutoverGatesBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    all_partitions_verified: bool | None = Field(default=None, alias="allPartitionsVerified")
    max_cdc_lag: timedelta | None = Field(default=None, alias="maxCdcLag")
    critical_verification_failures: int | None = Field(
        default=None, alias="criticalVerificationFailures", ge=0
    )
    require_approval: bool | None = Field(default=None, alias="requireApproval")

    @field_validator("max_cdc_lag", mode="before")
    @classmethod
    def _parse_lag(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except DurationError as exc:
                raise ValueError(str(exc)) from exc
        return value


class CutoverBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    gates: CutoverGatesBlock = CutoverGatesBlock()


class RollbackBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    window: timedelta | None = None
    source_remains_authoritative: bool | None = Field(
        default=None, alias="sourceRemainsAuthoritative"
    )

    @field_validator("window", mode="before")
    @classmethod
    def _parse_window(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except DurationError as exc:
                raise ValueError(str(exc)) from exc
        return value


class MovementSpec(OperationSpec):
    """A parsed `kind: Movement` document."""

    kind: Literal["Movement"]
    source: EndpointBlock
    destination: EndpointBlock
    strategy: StrategyBlock
    datasets: tuple[MovementDatasetBlock, ...] = Field(min_length=1)
    cdc: CdcBlock | None = None
    runtime: RuntimeBlock = RuntimeBlock()
    # Cutover and rollback belong to the Migration workflow, not to Movement.
    # The design document's Movement example carries them, so they parse here
    # and are reported as deprecated placement rather than silently accepted.
    cutover: CutoverBlock | None = None
    rollback: RollbackBlock | None = None

    @model_validator(mode="after")
    def _check_datasets(self) -> MovementSpec:
        names = [dataset.name for dataset in self.datasets]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate dataset names: {sorted(names)}")

        known = set(names)
        for dataset in self.datasets:
            unknown = [dep for dep in dataset.depends_on if dep not in known]
            if unknown:
                raise ValueError(f"dataset {dataset.name!r} depends on unknown datasets: {unknown}")

        self._check_acyclic(names)

        if self.strategy.mode is not StrategyMode.SNAPSHOT and self.cdc is None:
            raise ValueError(f"strategy.mode {self.strategy.mode.value!r} requires a cdc block")
        return self

    def _check_acyclic(self, names: list[str]) -> None:
        """Reject dependency cycles here rather than at plan time.

        A cycle is a spec error, and reporting it against the spec is far more
        useful than reporting it against a generated DAG.
        """
        edges = {dataset.name: set(dataset.depends_on) for dataset in self.datasets}
        resolved: set[str] = set()
        while True:
            ready = {name for name, deps in edges.items() if deps <= resolved}
            if ready == resolved:
                break
            resolved = ready
        unresolved = sorted(set(names) - resolved)
        if unresolved:
            raise ValueError(f"dependency cycle among datasets: {unresolved}")

    @property
    def has_migration_blocks(self) -> bool:
        """Whether the spec carries Migration-workflow blocks on a Movement."""
        return self.cutover is not None or self.rollback is not None

    def to_movement(self) -> DomainMovement:
        """Convert to the domain model the runtime plans against.

        Cutover and rollback are deliberately dropped: they are Migration
        workflow concerns, and a Movement does not imply a cutover. This is the
        seam that keeps spec format changeable - everything downstream sees the
        domain model, not these field names.
        """
        return DomainMovement(
            name=self.metadata.name,
            source=DomainEndpoint(
                adapter=self.source.adapter, connection_ref=self.source.connection_ref
            ),
            destination=DomainEndpoint(
                adapter=self.destination.adapter,
                connection_ref=self.destination.connection_ref,
            ),
            mode=DomainMovementMode(self.strategy.mode.value),
            datasets=tuple(
                DomainDataset(
                    name=dataset.name,
                    source=dataset.source,
                    target=dataset.target,
                    depends_on=dataset.depends_on,
                    key_columns=dataset.key.columns,
                    ordering=DomainOrdering(
                        scope=DomainOrderingScope(dataset.ordering.scope.value),
                        version_field=dataset.ordering.version_field,
                    ),
                    partitioning=(
                        None
                        if dataset.partitioning is None
                        else DomainPartitioning(
                            strategy=DomainPartitionStrategy(dataset.partitioning.strategy.value),
                            column=dataset.partitioning.column,
                            rows_per_partition=dataset.partitioning.rows_per_partition,
                            interval_seconds=(
                                None
                                if dataset.partitioning.interval is None
                                else int(dataset.partitioning.interval.total_seconds())
                            ),
                            buckets=dataset.partitioning.buckets,
                        )
                    ),
                    write_mode=DomainWriteMode(dataset.write.mode.value),
                    verification=dataset.verification.required,
                )
                for dataset in self.datasets
            ),
            cdc=(
                None
                if self.cdc is None
                else DomainCdc(
                    adapter=self.cdc.adapter,
                    checkpoint_type=(
                        None if self.cdc.checkpoint is None else self.cdc.checkpoint.type
                    ),
                )
            ),
            limits=DomainLimits(
                max_concurrency=self.runtime.max_concurrency,
                max_retries=self.runtime.max_retries,
                source_rows_per_second=self.runtime.rate_limits.source_rows_per_second,
                target_rows_per_second=self.runtime.rate_limits.target_rows_per_second,
                target_cpu_max_percent=self.runtime.policies.target_cpu_max_percent,
                source_cpu_max_percent=self.runtime.policies.source_cpu_max_percent,
            ),
        )
