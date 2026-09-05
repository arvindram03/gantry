"""Partition planning.

Partitions are derived from a pinned manifest, so these tests need no database.
That is the point: bounds are a pure function of a Dataset version, which is
what makes a replay resume against the partitions its checkpoints refer to.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from itertools import pairwise

import pytest
from gantry.core.dataset import DatasetManifest, DatasetStatistics, PhysicalRef
from gantry.core.schema import DatasetSchema, FieldSchema
from gantry.movement.partitioning import (
    PartitionMethod,
    plan_partitions,
    plan_time_partitions,
)


def manifest(
    *,
    rows: int = 1_000_000,
    histograms: Mapping[str, tuple[str, ...]] | None = None,
    key_min: str | None = "1",
    key_max: str | None = "1000000",
    keys: tuple[str, ...] = ("order_id",),
) -> DatasetManifest:
    return DatasetManifest(
        name="public.orders",
        physical=PhysicalRef(adapter="postgres", reference="public.orders", estimated_rows=rows),
        dataset_schema=DatasetSchema(
            keys=keys,
            fields=(
                FieldSchema(name="order_id", type="bigint", nullable=False),
                FieldSchema(name="created_at", type="timestamp with time zone", nullable=False),
            ),
        ),
        statistics=DatasetStatistics(
            row_count=rows,
            key_min=key_min,
            key_max=key_max,
            histograms=dict(histograms or {}),
        ),
    )


def even_histogram(count: int = 101, span: int = 1_000_000) -> Mapping[str, tuple[str, ...]]:
    step = span // (count - 1)
    return {"order_id": tuple(str(1 + i * step) for i in range(count))}


# --- histogram-based planning ---------------------------------------------


def test_partitions_are_derived_from_the_histogram() -> None:
    plan = plan_partitions(manifest(histograms=even_histogram()), target_partitions=10)
    assert plan.method is PartitionMethod.HISTOGRAM
    assert plan.count == 10


def test_bounds_are_deterministic() -> None:
    source = manifest(histograms=even_histogram())
    first = plan_partitions(source, target_partitions=20)
    second = plan_partitions(source, target_partitions=20)
    assert [(p.lo, p.hi) for p in first.partitions] == [(p.lo, p.hi) for p in second.partitions]


def test_bounds_do_not_move_when_the_table_grows() -> None:
    """Replanning against the same manifest version must not move a boundary.

    A partition whose bounds shift under a replay is a partition whose
    checkpoint means nothing.
    """
    pinned = manifest(histograms=even_histogram())
    grown = pinned.model_copy(
        update={"physical": pinned.physical.model_copy(update={"estimated_rows": 500_000_000})}
    )
    before = plan_partitions(pinned, target_partitions=10)
    after = plan_partitions(
        grown.model_copy(update={"statistics": pinned.statistics}), target_partitions=10
    )
    assert [p.lo for p in before.partitions] == [p.lo for p in after.partitions]


def test_outer_bounds_are_open() -> None:
    """Rows outside the observed key range must still be copied, not skipped."""
    plan = plan_partitions(manifest(histograms=even_histogram()), target_partitions=5)
    assert plan.partitions[0].lo is None
    assert plan.partitions[-1].hi is None


def test_partitions_are_contiguous_and_non_overlapping() -> None:
    plan = plan_partitions(manifest(histograms=even_histogram()), target_partitions=8)
    for earlier, later in pairwise(plan.partitions):
        assert earlier.hi == later.lo


def test_evenly_spread_cuts_produce_even_bucket_spans() -> None:
    """Each partition should span the same number of equi-depth buckets.

    Spacing cuts across the interior boundaries instead leaves the first and
    last partitions covering a different number of buckets than the rest.
    """
    plan = plan_partitions(manifest(histograms=even_histogram()), target_partitions=10)
    bounds = even_histogram()["order_id"]
    indices = [0, *(bounds.index(p.lo) for p in plan.partitions[1:]), len(bounds) - 1]
    spans = [b - a for a, b in pairwise(indices)]
    assert max(spans) - min(spans) <= 1


def test_rows_per_partition_sizes_the_plan() -> None:
    plan = plan_partitions(
        manifest(rows=1_000_000, histograms=even_histogram()), rows_per_partition=100_000
    )
    assert plan.count == 10


def test_requesting_more_partitions_than_the_sample_supports_is_flagged() -> None:
    """Better fewer, larger partitions than fabricated cut points."""
    small = {"order_id": ("1", "500", "1000")}
    plan = plan_partitions(manifest(histograms=small), target_partitions=50)
    assert plan.limited_by_statistics
    assert plan.count < 50


def test_exactly_one_sizing_argument_is_required() -> None:
    for kwargs in ({}, {"rows_per_partition": 10, "target_partitions": 5}):
        with pytest.raises(ValueError, match="exactly one"):
            plan_partitions(manifest(), **kwargs)  # type: ignore[arg-type]


# --- fallbacks when statistics are missing --------------------------------


def test_without_a_histogram_a_numeric_key_splits_uniformly() -> None:
    plan = plan_partitions(manifest(histograms={}), target_partitions=4)
    assert plan.method is PartitionMethod.UNIFORM
    assert plan.count == 4


def test_without_any_statistics_there_is_one_partition() -> None:
    """Missing information is not evidence of uniformity."""
    plan = plan_partitions(manifest(histograms={}, key_min=None, key_max=None), target_partitions=8)
    assert plan.method is PartitionMethod.SINGLE
    assert plan.count == 1
    assert plan.limited_by_statistics


def test_a_non_numeric_key_without_a_histogram_is_one_partition() -> None:
    plan = plan_partitions(
        manifest(histograms={}, key_min="alpha", key_max="omega"), target_partitions=8
    )
    assert plan.method is PartitionMethod.SINGLE


def test_a_composite_key_needs_an_explicit_column() -> None:
    with pytest.raises(ValueError, match="range partitioning needs exactly one"):
        plan_partitions(manifest(keys=("order_id", "created_at")), target_partitions=4)


# --- time partitioning -----------------------------------------------------


def daily_histogram(days: int = 10) -> Mapping[str, tuple[str, ...]]:
    return {"created_at": tuple(f"2026-01-{day:02d}T00:00:00+00:00" for day in range(1, days + 1))}


def test_time_partitions_follow_the_interval() -> None:
    plan = plan_time_partitions(
        manifest(histograms=daily_histogram()), column="created_at", interval=timedelta(days=1)
    )
    assert plan.method is PartitionMethod.TIME_INTERVAL
    # Nine interior day boundaries, plus the open ends.
    assert plan.count == 10


def test_time_partitions_align_to_the_calendar() -> None:
    """Daily partitions must line up with days, not with the first row."""
    histogram = {"created_at": ("2026-01-01T07:33:11+00:00", "2026-01-05T19:02:00+00:00")}
    plan = plan_time_partitions(
        manifest(histograms=histogram), column="created_at", interval=timedelta(days=1)
    )
    interior = [p.lo for p in plan.partitions[1:]]
    assert all(bound is not None and bound.endswith("T00:00:00+00:00") for bound in interior)


def test_time_partitions_are_not_rebalanced() -> None:
    """Asking for daily partitions means asking for days, uneven or not."""
    histogram = {"created_at": ("2026-01-01T00:00:00+00:00", "2026-01-04T00:00:00+00:00")}
    plan = plan_time_partitions(
        manifest(histograms=histogram), column="created_at", interval=timedelta(days=1)
    )
    assert plan.count == 4


def test_time_partitioning_requires_a_positive_interval() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        plan_time_partitions(
            manifest(histograms=daily_histogram()), column="created_at", interval=timedelta(0)
        )


def test_time_partitioning_without_statistics_is_one_partition() -> None:
    plan = plan_time_partitions(
        manifest(histograms={}), column="created_at", interval=timedelta(days=1)
    )
    assert plan.method is PartitionMethod.SINGLE
    assert plan.limited_by_statistics


def test_partition_ids_are_stable_and_sortable() -> None:
    plan = plan_partitions(manifest(histograms=even_histogram()), target_partitions=12)
    ids = [p.id for p in plan.partitions]
    assert ids == sorted(ids)
    assert ids[0] == "public.orders/00000"
