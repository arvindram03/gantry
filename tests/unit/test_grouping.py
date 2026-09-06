# SPDX-License-Identifier: Apache-2.0
"""How many partitions travel in one job.

The arithmetic decides a guarantee: the group is the checkpoint unit for kinds
that need one, so getting this wrong quietly coarsens what a crash costs.
"""

from __future__ import annotations

import pytest
from gantry.movement.grouping import (
    BEAM_COST,
    SQL_COST,
    JobCost,
    group_partitions,
)
from gantry.movement.partitioning import Partition, PartitionMethod


def partition(index: int, rows: int | None = 100_000) -> Partition:
    return Partition.model_validate(
        {
            "dataset": "public.orders",
            "index": index,
            "column": "order_id",
            "method": PartitionMethod.HISTOGRAM,
            "lo": str(index * 1000),
            "hi": str((index + 1) * 1000),
            "estimated_rows": rows,
        }
    )


class TestTheArithmetic:
    def test_a_cheaper_job_needs_fewer_rows_to_be_worth_starting(self) -> None:
        assert SQL_COST.rows_for_overhead(0.1) < BEAM_COST.rows_for_overhead(0.1)

    def test_tolerating_more_overhead_allows_smaller_jobs(self) -> None:
        """Which is the whole trade: smaller jobs mean finer checkpoints."""
        assert BEAM_COST.rows_for_overhead(0.5) < BEAM_COST.rows_for_overhead(0.1)

    def test_a_fraction_outside_the_open_unit_interval_is_refused(self) -> None:
        for bad in (0.0, 1.0, -0.1, 2.0):
            with pytest.raises(ValueError, match="between 0 and 1"):
                BEAM_COST.rows_for_overhead(bad)

    def test_the_measured_beam_cost_forces_coarse_groups(self) -> None:
        """Recorded as a fact, not an aspiration: at eleven seconds of startup
        a Beam job must carry well over a million rows before the startup is a
        tenth of it, which is why the checkpoint unit is the group."""
        assert BEAM_COST.rows_for_overhead(0.1) > 1_000_000


class TestGrouping:
    def test_partitions_are_gathered_until_the_group_is_worth_a_job(self) -> None:
        cost = JobCost(fixed_seconds=10.0, rows_per_second=1_000.0)  # 90k rows at a tenth
        groups = group_partitions(
            [partition(i, rows=30_000) for i in range(6)], cost=cost, overhead_fraction=0.1
        )
        assert [len(g) for g in groups] == [3, 3]

    def test_every_partition_travels_exactly_once(self) -> None:
        """A dropped partition is silent data loss and a duplicated one is
        wasted work, so this is checked rather than assumed."""
        parts = [partition(i, rows=7_000) for i in range(20)]
        grouped = [p for group in group_partitions(parts) for p in group]
        assert grouped == parts

    def test_order_is_preserved_so_a_group_is_still_a_range(self) -> None:
        """Partitions are contiguous key ranges; a group of adjacent ones is a
        range too, which is what makes verifying a group a question about one
        span rather than a scattered set."""
        parts = [partition(i, rows=1_000) for i in range(10)]
        for group in group_partitions(parts):
            indexes = [p.index for p in group]
            assert indexes == sorted(indexes)
            assert indexes == list(range(indexes[0], indexes[-1] + 1))

    def test_a_trailing_remainder_still_gets_a_job(self) -> None:
        cost = JobCost(fixed_seconds=10.0, rows_per_second=1_000.0)
        groups = group_partitions(
            [partition(i, rows=30_000) for i in range(4)], cost=cost, overhead_fraction=0.1
        )
        assert [len(g) for g in groups] == [3, 1], "the leftover partition must still run"

    def test_an_unsized_partition_counts_as_a_whole_group(self) -> None:
        """Guessing small would build a job that reads far more than intended;
        guessing large costs one extra job. The cheaper mistake is the one to
        make."""
        groups = group_partitions([partition(i, rows=None) for i in range(3)])
        assert [len(g) for g in groups] == [1, 1, 1]

    def test_no_partitions_is_no_groups(self) -> None:
        assert group_partitions([]) == ()
