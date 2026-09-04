"""The `kind: Analysis` spec.

An Analysis computes over one or more Datasets and produces a Result. It is a
sibling of Movement, not a layer above it: both are Operations, and both run
the same lifecycle under the same guarantee boundary.

Gantry compiles an Analysis onto an engine that already exists. It does not
become a query language, and this spec is deliberately narrow: normalise, join,
window, aggregate. Anything beyond that is a new spec version, not an escape
hatch.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from gantry.analysis.model import Analysis as DomainAnalysis
from gantry.analysis.model import AnalysisLimits as DomainLimits
from gantry.analysis.model import ExecutionEngine as DomainEngine
from gantry.analysis.model import FieldNormalization as DomainNormalization
from gantry.analysis.model import Join as DomainJoin
from gantry.analysis.model import TemporalJoin as DomainTemporalJoin
from gantry.analysis.model import TemporalJoinStrategy as DomainTemporalStrategy
from gantry.core.durations import DurationError, parse_duration
from gantry.core.names import FieldName, ResourceName
from gantry.core.sizes import parse_byte_size
from gantry.core.timewindow import TimeWindow
from gantry.spec.operation import LimitsBlock, OperationSpec

KIND = "Analysis"


class AnalysisMode(StrEnum):
    BATCH = "batch"
    CONTINUOUS = "continuous"


class ExecutionEngine(StrEnum):
    """Engines an Analysis may compile onto.

    `AUTO` resolves from the physical adapters of the input Datasets. v1 ships
    Postgres and DuckDB; the rest are named so specs written against them fail
    with "not supported in v1" rather than "unknown value".
    """

    AUTO = "auto"
    POSTGRES = "postgres"
    DUCKDB = "duckdb"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    SPARK = "spark"
    FLINK = "flink"


V1_ENGINES: frozenset[str] = frozenset(
    {ExecutionEngine.AUTO, ExecutionEngine.POSTGRES, ExecutionEngine.DUCKDB}
)


class TemporalJoinStrategy(StrEnum):
    NEAREST_PRECEDING = "nearest_preceding"
    NEAREST_FOLLOWING = "nearest_following"
    NEAREST = "nearest"


class ObjectiveBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    explain: str


class InputBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset: ResourceName


class WindowBlock(BaseModel):
    """An absolute window, or a sliding one for continuous mode."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    start: datetime | None = None
    end: datetime | None = None
    type: Literal["sliding"] | None = None
    duration: timedelta | None = None

    @field_validator("duration", mode="before")
    @classmethod
    def _parse_duration(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except DurationError as exc:
                raise ValueError(str(exc)) from exc
        return value

    @model_validator(mode="after")
    def _check_window(self) -> WindowBlock:
        absolute = self.start is not None or self.end is not None
        sliding = self.type is not None or self.duration is not None
        if absolute and sliding:
            raise ValueError("window must be absolute (start/end) or sliding, not both")
        if not absolute and not sliding:
            raise ValueError("window requires either start and end, or type and duration")

        if absolute:
            if self.start is None or self.end is None:
                raise ValueError("an absolute window requires both start and end")
            for label, value in (("start", self.start), ("end", self.end)):
                if value.tzinfo is None:
                    raise ValueError(f"window.{label} must be timezone-aware")
            if self.end <= self.start:
                raise ValueError("window.end must be after window.start")
        elif self.type is None or self.duration is None:
            raise ValueError("a sliding window requires both type and duration")
        return self


class NormalizeFieldBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    aliases: tuple[FieldName, ...] = Field(min_length=1)


class NormalizeBlock(BaseModel):
    """Canonical field names and the source spellings that map onto them."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    fields: dict[FieldName, NormalizeFieldBlock] = {}


class TemporalJoinBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    strategy: TemporalJoinStrategy
    max_distance: timedelta | None = Field(default=None, alias="maxDistance")

    @field_validator("max_distance", mode="before")
    @classmethod
    def _parse_distance(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except DurationError as exc:
                raise ValueError(str(exc)) from exc
        return value


class JoinBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    left: ResourceName
    right: ResourceName
    on: tuple[FieldName, ...] = Field(min_length=1)
    temporal: TemporalJoinBlock | None = None

    @model_validator(mode="after")
    def _check_sides(self) -> JoinBlock:
        if self.left == self.right:
            raise ValueError(f"join left and right must differ, both are {self.left!r}")
        return self


class ExecutionBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    engine: ExecutionEngine = ExecutionEngine.AUTO
    max_bytes_scanned: str | None = Field(default=None, alias="maxBytesScanned")
    timeout: timedelta | None = None

    @field_validator("timeout", mode="before")
    @classmethod
    def _parse_timeout(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except DurationError as exc:
                raise ValueError(str(exc)) from exc
        return value

    @model_validator(mode="after")
    def _check_engine_supported(self) -> ExecutionBlock:
        if self.engine not in V1_ENGINES:
            raise ValueError(
                f"execution.engine {self.engine.value!r} is not supported in v1 "
                f"(supported: {', '.join(sorted(V1_ENGINES))})"
            )
        return self


class OutputBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: str = "AnalysisResult"


class AnalysisSpec(OperationSpec):
    """A parsed `kind: Analysis` document."""

    kind: Literal["Analysis"]
    objective: ObjectiveBlock | None = None
    inputs: tuple[InputBlock, ...] = Field(min_length=1)
    mode: AnalysisMode = AnalysisMode.BATCH
    window: WindowBlock | None = None
    normalize: NormalizeBlock = NormalizeBlock()
    joins: tuple[JoinBlock, ...] = ()
    signals: tuple[str, ...] = ()
    execution: ExecutionBlock = ExecutionBlock()
    output: OutputBlock = OutputBlock()
    limits: LimitsBlock = LimitsBlock()

    @model_validator(mode="after")
    def _check_document(self) -> AnalysisSpec:
        if self.mode is AnalysisMode.CONTINUOUS:
            raise ValueError(
                "mode 'continuous' is not supported in v1; continuous Analysis is planned for v2"
            )

        names = [item.dataset for item in self.inputs]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate inputs: {sorted(names)}")

        known = set(names)
        for join in self.joins:
            unknown = [side for side in (join.left, join.right) if side not in known]
            if unknown:
                raise ValueError(f"join references datasets that are not inputs: {sorted(unknown)}")
        return self

    @property
    def input_names(self) -> tuple[str, ...]:
        return tuple(item.dataset for item in self.inputs)

    def to_analysis(self) -> DomainAnalysis:
        """Convert to the domain model the compiler and planner work against.

        The same seam as `MovementSpec.to_movement`: spec field names stop here.
        """
        window: TimeWindow | None = None
        if self.window is not None and self.window.start is not None:
            # Sliding windows belong to continuous mode, which v1 rejects.
            window = TimeWindow(start=self.window.start, end=_require_end(self.window))

        return DomainAnalysis(
            name=self.metadata.name,
            objective=None if self.objective is None else self.objective.explain,
            inputs=self.input_names,
            window=window,
            normalize=tuple(
                DomainNormalization(canonical=canonical, aliases=block.aliases)
                for canonical, block in sorted(self.normalize.fields.items())
            ),
            joins=tuple(
                DomainJoin(
                    left=join.left,
                    right=join.right,
                    on=join.on,
                    temporal=(
                        None
                        if join.temporal is None
                        else DomainTemporalJoin(
                            strategy=DomainTemporalStrategy(join.temporal.strategy.value),
                            max_distance_seconds=(
                                None
                                if join.temporal.max_distance is None
                                else int(join.temporal.max_distance.total_seconds())
                            ),
                        )
                    ),
                )
                for join in self.joins
            ),
            signals=self.signals,
            engine=DomainEngine(self.execution.engine.value),
            verification=self.verify,
            limits=DomainLimits(
                max_concurrency=self.limits.max_concurrency,
                max_retries=self.limits.max_retries,
                max_bytes_scanned=(
                    None
                    if self.execution.max_bytes_scanned is None
                    else parse_byte_size(self.execution.max_bytes_scanned)
                ),
                timeout_seconds=(
                    None
                    if self.execution.timeout is None
                    else int(self.execution.timeout.total_seconds())
                ),
            ),
        )


def _require_end(window: WindowBlock) -> datetime:
    if window.end is None:  # pragma: no cover - validation guarantees this
        raise ValueError("an absolute window requires both start and end")
    return window.end
