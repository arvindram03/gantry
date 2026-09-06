# SPDX-License-Identifier: Apache-2.0
"""How many partitions travel in one job.

The arithmetic decides a guarantee: the group is the checkpoint unit for kinds
that need one, so getting this wrong quietly coarsens what a crash costs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from gantry.movement.grouping import (
    BEAM_COST,
    SQL_COST,
    JobCost,
    group_partitions,
)
from gantry.movement.model import ExecutionConfig
from gantry.movement.partitioning import Partition, PartitionMethod

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"
AT = datetime(2026, 9, 11, tzinfo=UTC)


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


class TestThePlanShape:
    """What the planner does with the grouping, which is where it becomes a
    guarantee rather than an arithmetic exercise."""

    def test_sql_gets_a_node_per_partition_and_beam_a_node_per_group(self) -> None:
        from gantry.jobs.model import JobKind
        from gantry.lifecycle.plan import NodeKind
        from gantry.movement.planner import compile_movement
        from gantry.spec import load_movement_spec

        spec = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml")
        sql_plan = compile_movement(spec.to_movement(), created_at=AT)

        beam = spec.to_movement().model_copy(
            update={"execution": ExecutionConfig(kind=JobKind.BEAM)}
        )
        beam_plan = compile_movement(beam, created_at=AT)

        assert any(n.kind is NodeKind.SNAPSHOT_PARTITION for n in sql_plan.nodes)
        assert not any(n.kind is NodeKind.SNAPSHOT_GROUP for n in sql_plan.nodes)
        assert any(n.kind is NodeKind.SNAPSHOT_GROUP for n in beam_plan.nodes)
        assert not any(n.kind is NodeKind.SNAPSHOT_PARTITION for n in beam_plan.nodes)

    def test_changing_the_job_kind_requires_a_replan(self) -> None:
        """The kind decides the checkpoint unit, so it is a guarantee: a plan
        compiled under one must not be resumed under the other."""
        from gantry.jobs.model import JobKind
        from gantry.spec import load_movement_spec

        spec = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml")
        sql = spec.to_movement()
        beam = sql.model_copy(update={"execution": ExecutionConfig(kind=JobKind.BEAM)})
        assert sql.guarantee_fingerprint() != beam.guarantee_fingerprint()

    def test_a_movement_written_before_the_field_existed_keeps_its_fingerprint(
        self,
    ) -> None:
        """Adding the field must not invalidate every stored plan. Only a
        non-default kind enters the payload."""
        from gantry.spec import load_movement_spec

        spec = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml")
        default = spec.to_movement()
        explicit = default.model_copy(update={"execution": ExecutionConfig()})
        assert default.guarantee_fingerprint() == explicit.guarantee_fingerprint()

    def test_a_group_node_carries_the_bounds_the_plan_recorded(self) -> None:
        """Read back at execution time, never recomputed — recomputing lets the
        group's membership shift under a replay."""
        import json

        from gantry.jobs.model import JobKind
        from gantry.lifecycle.plan import NodeKind
        from gantry.movement.planner import compile_movement
        from gantry.spec import load_movement_spec

        spec = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml")
        beam = spec.to_movement().model_copy(
            update={"execution": ExecutionConfig(kind=JobKind.BEAM)}
        )
        plan = compile_movement(beam, created_at=AT)
        groups = [n for n in plan.nodes if n.kind is NodeKind.SNAPSHOT_GROUP]
        assert groups
        for node in groups:
            recorded = json.loads(node.params["partitions"])
            assert recorded, "a group node must name its partitions"
            assert all("index" in entry for entry in recorded)
