# SPDX-License-Identifier: Apache-2.0
"""Partition planning.

Partitions are derived from a pinned Dataset manifest, never from a live query.
That is what makes bounds deterministic: replanning an operation against the
same manifest version produces byte-identical partitions even if the table has
since grown by a hundred million rows. A partition whose bounds move under a
replay is a partition whose checkpoint means nothing.

Bounds come from the planner's equi-depth histogram, where each interval holds
roughly the same number of rows. Splitting a key range into equal spans is the
obvious approach and the wrong one: equal key spans are equal row counts only
when the key is uniform, and a skewed key turns one of those spans into the
whole migration.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from gantry.core.dataset import DatasetManifest
from gantry.core.names import FieldName, ResourceName


class PartitionMethod(StrEnum):
    """How a partition set's bounds were derived.

    Recorded on every partition so an operator can tell a well-informed plan
    from a fallback, rather than having to infer it from the shape.
    """

    HISTOGRAM = "histogram"
    # No histogram: a uniform split of the observed key range. Correct, but
    # balanced only if the key happens to be uniform.
    UNIFORM = "uniform"
    # Fixed calendar intervals, not rebalanced.
    TIME_INTERVAL = "time_interval"
    # Nothing to go on at all, or a key that cannot be split.
    SINGLE = "single"


class Partition(BaseModel):
    """One independently executable, independently checkpointed unit of work.

    Bounds are half-open `[lo, hi)`. The first partition has no lower bound and
    the last no upper bound, so rows outside the key range observed at planning
    time are still copied rather than silently skipped.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset: ResourceName
    index: int = Field(ge=0)
    column: FieldName
    lo: str | None = None
    hi: str | None = None
    estimated_rows: int | None = Field(default=None, ge=0)
    method: PartitionMethod

    @property
    def id(self) -> str:
        return f"{self.dataset}/{self.index:05d}"

    def describe_bounds(self) -> str:
        lo = "-inf" if self.lo is None else self.lo
        hi = "+inf" if self.hi is None else self.hi
        return f"[{lo}, {hi})"


class PartitionPlan(BaseModel):
    """The partitions for one dataset, plus how they were arrived at."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset: ResourceName
    column: FieldName
    partitions: tuple[Partition, ...] = Field(min_length=1)
    method: PartitionMethod
    requested: int
    # True when fewer partitions were produced than asked for because the
    # histogram had no more boundaries to offer. Producing the requested count
    # would mean inventing cut points the sample does not support.
    limited_by_statistics: bool = False

    @property
    def count(self) -> int:
        return len(self.partitions)


def plan_time_partitions(
    manifest: DatasetManifest,
    *,
    column: FieldName,
    interval: timedelta,
) -> PartitionPlan:
    """Divide a dataset into fixed time intervals.

    Unlike range partitioning, this deliberately does not rebalance. Asking for
    daily partitions means asking for boundaries that line up with days - so a
    partition can be recomputed, compared or replayed against a calendar - and
    silently resizing them to equalise row counts would take that away. Uneven
    traffic therefore produces uneven partitions, which is the intended answer.
    """
    if interval <= timedelta(0):
        raise ValueError(f"interval must be positive, got {interval}")

    bounds = manifest.statistics.histograms.get(column, ())
    span = _time_span(bounds)
    if span is None:
        return _single_partition_plan(
            manifest, column, requested=1, rows=_row_count(manifest), limited=True
        )

    start, end = span
    cuts: list[str] = []
    edge = _floor_to_interval(start, interval)
    while edge <= end:
        edge += interval
        if edge <= end:
            cuts.append(edge.isoformat())

    return PartitionPlan(
        dataset=manifest.name,
        column=column,
        partitions=_partitions_from_cuts(
            manifest.name, column, cuts, _row_count(manifest), PartitionMethod.TIME_INTERVAL
        ),
        method=PartitionMethod.TIME_INTERVAL,
        requested=len(cuts) + 1,
    )


def plan_partitions(
    manifest: DatasetManifest,
    *,
    column: FieldName | None = None,
    rows_per_partition: int | None = None,
    target_partitions: int | None = None,
) -> PartitionPlan:
    """Divide a dataset into balanced, deterministic partitions.

    Exactly one of `rows_per_partition` or `target_partitions` sizes the plan.
    """
    if (rows_per_partition is None) == (target_partitions is None):
        raise ValueError("pass exactly one of rows_per_partition or target_partitions")

    key = column or _single_key(manifest)
    statistics = manifest.statistics
    rows = statistics.row_count or manifest.physical.estimated_rows or 0

    requested = (
        target_partitions
        if target_partitions is not None
        else max(1, math.ceil(rows / max(1, rows_per_partition or 1)))
    )

    bounds = statistics.histograms.get(key, ())
    if requested <= 1 or len(bounds) < 3:
        return _fallback_plan(manifest, key, requested, rows)

    cuts = _select_cuts(bounds, requested)
    partitions = _partitions_from_cuts(manifest.name, key, cuts, rows, PartitionMethod.HISTOGRAM)
    return PartitionPlan(
        dataset=manifest.name,
        column=key,
        partitions=partitions,
        method=PartitionMethod.HISTOGRAM,
        requested=requested,
        limited_by_statistics=len(partitions) < requested,
    )


def _single_key(manifest: DatasetManifest) -> FieldName:
    keys = manifest.dataset_schema.keys
    if len(keys) != 1:
        raise ValueError(
            f"dataset {manifest.name!r} has {len(keys)} key columns; "
            f"range partitioning needs exactly one, or an explicit column"
        )
    return keys[0]


def _select_cuts(bounds: tuple[str, ...], requested: int) -> list[str]:
    """Choose interior cut points from the histogram.

    With B equi-depth buckets and N partitions, each partition should span B/N
    buckets, so cut k belongs at bucket boundary round(k * B / N). Spacing the
    cuts across the interior boundaries instead leaves the first and last
    partitions covering a different number of buckets than the rest - which is
    how a nominally balanced plan ends up six times heavier at one end.

    Asking for more partitions than the histogram has boundaries yields fewer,
    larger partitions rather than fabricated precision.
    """
    buckets = len(bounds) - 1
    wanted = requested - 1
    if wanted >= buckets:
        return list(bounds[1:-1])

    step = buckets / requested
    last = len(bounds) - 2
    cuts = [bounds[min(last, max(1, round(k * step)))] for k in range(1, requested)]
    return list(dict.fromkeys(cuts))


def _partitions_from_cuts(
    dataset: str, column: str, cuts: list[str], rows: int, method: PartitionMethod
) -> tuple[Partition, ...]:
    count = len(cuts) + 1
    per_partition = rows // count if count else rows
    edges: list[str | None] = [None, *cuts, None]
    return tuple(
        Partition(
            dataset=dataset,
            index=index,
            column=column,
            lo=edges[index],
            hi=edges[index + 1],
            estimated_rows=per_partition,
            method=method,
        )
        for index in range(count)
    )


def _fallback_plan(
    manifest: DatasetManifest, key: FieldName, requested: int, rows: int
) -> PartitionPlan:
    """Plan without a usable histogram.

    A numeric key range can still be split evenly. Anything else becomes a
    single partition: one large unit of work is honest, whereas guessing at
    boundaries produces a plan that looks balanced and is not.
    """
    statistics = manifest.statistics
    numeric_range = _numeric_range(statistics.key_min, statistics.key_max)

    if requested <= 1 or numeric_range is None:
        return PartitionPlan(
            dataset=manifest.name,
            column=key,
            partitions=(
                Partition(
                    dataset=manifest.name,
                    index=0,
                    column=key,
                    estimated_rows=rows or None,
                    method=PartitionMethod.SINGLE,
                ),
            ),
            method=PartitionMethod.SINGLE,
            requested=requested,
            limited_by_statistics=requested > 1,
        )

    low, high = numeric_range
    width = (high - low) / requested
    cuts = [str(int(low + width * i)) for i in range(1, requested)]
    return PartitionPlan(
        dataset=manifest.name,
        column=key,
        partitions=_partitions_from_cuts(
            manifest.name, key, list(dict.fromkeys(cuts)), rows, PartitionMethod.UNIFORM
        ),
        method=PartitionMethod.UNIFORM,
        requested=requested,
        limited_by_statistics=False,
    )


def _row_count(manifest: DatasetManifest) -> int:
    return manifest.statistics.row_count or manifest.physical.estimated_rows or 0


def _single_partition_plan(
    manifest: DatasetManifest, column: FieldName, *, requested: int, rows: int, limited: bool
) -> PartitionPlan:
    return PartitionPlan(
        dataset=manifest.name,
        column=column,
        partitions=(
            Partition(
                dataset=manifest.name,
                index=0,
                column=column,
                estimated_rows=rows or None,
                method=PartitionMethod.SINGLE,
            ),
        ),
        method=PartitionMethod.SINGLE,
        requested=requested,
        limited_by_statistics=limited,
    )


def _time_span(bounds: tuple[str, ...]) -> tuple[datetime, datetime] | None:
    """The observed time range, taken from the histogram's outer boundaries."""
    if len(bounds) < 2:
        return None
    try:
        start = datetime.fromisoformat(bounds[0])
        end = datetime.fromisoformat(bounds[-1])
    except ValueError:
        return None
    return (start, end) if end > start else None


def _floor_to_interval(moment: datetime, interval: timedelta) -> datetime:
    """Align a starting point to the interval grid.

    Without this, boundaries are anchored to whenever the first row happened to
    land, and "daily partitions" would not line up with days.
    """
    if interval < timedelta(days=1):
        return moment.replace(microsecond=0)
    return moment.replace(hour=0, minute=0, second=0, microsecond=0)


def _numeric_range(low: str | None, high: str | None) -> tuple[Decimal, Decimal] | None:
    if low is None or high is None:
        return None
    try:
        parsed = (Decimal(low), Decimal(high))
    except InvalidOperation:
        return None
    return parsed if parsed[1] > parsed[0] else None
