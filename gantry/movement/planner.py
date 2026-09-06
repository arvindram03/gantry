# SPDX-License-Identifier: Apache-2.0
"""Compile a Movement into an execution plan.

Partition bounds come from a Dataset manifest, so a plan compiled without
manifests carries one snapshot node per dataset and a plan compiled with them
carries one node per partition. Both are valid plans; the second is the one
that actually runs, because a partition is the smallest independently
checkpointed unit of work.

Bounds are never recomputed during execution. They are fixed in the plan, and
the plan is content-addressed, so a replay resumes against exactly the
partitions its checkpoints refer to.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timedelta

from gantry.core.dataset import DatasetManifest
from gantry.core.operation import LifecycleStage, OperationType
from gantry.jobs.model import JobKind
from gantry.lifecycle.plan import NodeKind, PlanNode, PlanVersion, node_id
from gantry.movement.beamjob import unit_of
from gantry.movement.grouping import BEAM_COST, group_partitions
from gantry.movement.model import Movement, MovementDataset, MovementMode, PartitionStrategy
from gantry.movement.partitioning import Partition, plan_partitions, plan_time_partitions


def compile_movement(
    movement: Movement,
    *,
    created_at: datetime,
    version: int = 1,
    manifests: Mapping[str, DatasetManifest] | None = None,
) -> PlanVersion:
    """Build the deterministic plan for a Movement.

    `manifests` maps dataset name to the pinned manifest partitioning is
    derived from. Without it the plan is unpartitioned - useful for validating
    a spec before discovery has run.
    """
    nodes: list[PlanNode] = []

    discover = PlanNode(
        id=node_id(movement.name, NodeKind.DISCOVER),
        kind=NodeKind.DISCOVER,
        stage=LifecycleStage.PLAN,
    )
    nodes.append(discover)

    streaming = movement.mode is not MovementMode.SNAPSHOT
    start_cdc_id: str | None = None
    if streaming:
        # CDC starts before the snapshot so no change is missed in the gap
        # between capturing a position and reading the data at it.
        start_cdc = PlanNode(
            id=node_id(movement.name, NodeKind.START_CDC),
            kind=NodeKind.START_CDC,
            stage=LifecycleStage.EXECUTE,
            depends_on=(discover.id,),
        )
        nodes.append(start_cdc)
        start_cdc_id = start_cdc.id

    verify_ids: list[str] = []
    completion_ids: dict[str, tuple[str, ...]] = {}

    for dataset in movement.datasets:
        scope = dataset.name

        schema = PlanNode(
            id=node_id(movement.name, NodeKind.CREATE_SCHEMA, scope),
            kind=NodeKind.CREATE_SCHEMA,
            stage=LifecycleStage.EXECUTE,
            scope=scope,
            depends_on=(discover.id,),
        )

        # A dataset waits for everything it depends on to have finished moving.
        upstream = tuple(
            sorted(
                node
                for dependency in dataset.depends_on
                for node in completion_ids.get(dependency, ())
            )
        )

        snapshot_nodes = _snapshot_nodes(
            movement, dataset, schema.id, upstream, _partitions(dataset, manifests)
        )
        completion_ids[scope] = tuple(node.id for node in snapshot_nodes)

        verify = PlanNode(
            id=node_id(movement.name, NodeKind.VERIFY_DATASET, scope),
            kind=NodeKind.VERIFY_DATASET,
            stage=LifecycleStage.VERIFY,
            scope=scope,
            depends_on=tuple(sorted(node.id for node in snapshot_nodes)),
            params={"checks": ",".join(sorted(c.check.value for c in dataset.verification))},
        )
        nodes.append(schema)
        nodes.extend(snapshot_nodes)
        nodes.append(verify)
        verify_ids.append(verify.id)

    if streaming and start_cdc_id is not None:
        snapshot_ids = tuple(sorted(node for ids in completion_ids.values() for node in ids))
        apply_cdc = PlanNode(
            id=node_id(movement.name, NodeKind.APPLY_CDC),
            kind=NodeKind.APPLY_CDC,
            stage=LifecycleStage.EXECUTE,
            depends_on=(start_cdc_id, *snapshot_ids),
        )
        wait = PlanNode(
            id=node_id(movement.name, NodeKind.WAIT_FOR_LAG),
            kind=NodeKind.WAIT_FOR_LAG,
            stage=LifecycleStage.EXECUTE,
            depends_on=(apply_cdc.id,),
        )
        nodes.extend((apply_cdc, wait))
        verify_ids.append(wait.id)

    nodes.append(
        PlanNode(
            id=node_id(movement.name, NodeKind.FINALIZE),
            kind=NodeKind.FINALIZE,
            stage=LifecycleStage.RESULT,
            depends_on=tuple(sorted(verify_ids)),
        )
    )

    return PlanVersion(
        operation=movement.name,
        operation_type=OperationType.MOVEMENT,
        version=version,
        nodes=tuple(nodes),
        guarantee_fingerprint=movement.guarantee_fingerprint(),
        created_at=created_at,
    )


def _partitions(
    dataset: MovementDataset, manifests: Mapping[str, DatasetManifest] | None
) -> tuple[Partition, ...]:
    """Derive this dataset's partitions, if there is a manifest to derive from."""
    if manifests is None or dataset.partitioning is None:
        return ()
    manifest = manifests.get(dataset.name) or manifests.get(dataset.source)
    if manifest is None:
        return ()
    partitioning = dataset.partitioning
    if partitioning.strategy is PartitionStrategy.TIME_RANGE:
        if partitioning.interval_seconds is None:
            return ()
        return plan_time_partitions(
            manifest,
            column=partitioning.column,
            interval=timedelta(seconds=partitioning.interval_seconds),
        ).partitions

    return plan_partitions(
        manifest,
        column=partitioning.column,
        rows_per_partition=partitioning.rows_per_partition,
        target_partitions=None if partitioning.rows_per_partition else 1,
    ).partitions


def _snapshot_nodes(
    movement: Movement,
    dataset: MovementDataset,
    schema_id: str,
    upstream: tuple[str, ...],
    partitions: tuple[Partition, ...],
) -> tuple[PlanNode, ...]:
    """One node per partition, or a single node when bounds are not known yet."""
    depends_on = (schema_id, *upstream)

    if not partitions:
        # Bounds are not known yet, so there is one unit covering the whole
        # dataset. It still has to be the *kind* of node this Movement's
        # executor understands: a grouping kind that received a partition node
        # would be executed by the wrong path entirely.
        if movement.execution.kind is JobKind.SQL:
            return (
                PlanNode(
                    id=node_id(movement.name, NodeKind.SNAPSHOT_PARTITION, dataset.name),
                    kind=NodeKind.SNAPSHOT_PARTITION,
                    stage=LifecycleStage.EXECUTE,
                    scope=dataset.name,
                    depends_on=depends_on,
                    params=_snapshot_params(dataset),
                ),
            )
        return (
            PlanNode(
                id=node_id(movement.name, NodeKind.SNAPSHOT_GROUP, dataset.name),
                kind=NodeKind.SNAPSHOT_GROUP,
                stage=LifecycleStage.EXECUTE,
                scope=dataset.name,
                depends_on=depends_on,
                params=_snapshot_params(dataset)
                | {
                    "partitions": json.dumps(
                        [{"index": 0, "lo": None, "hi": None}], separators=(",", ":")
                    )
                },
            ),
        )

    if movement.execution.kind is JobKind.SQL:
        return tuple(
            PlanNode(
                id=node_id(movement.name, NodeKind.SNAPSHOT_PARTITION, partition.id),
                kind=NodeKind.SNAPSHOT_PARTITION,
                stage=LifecycleStage.EXECUTE,
                scope=partition.id,
                depends_on=depends_on,
                params=_snapshot_params(dataset)
                | {
                    "partition_index": str(partition.index),
                    "partition_method": partition.method.value,
                    **({"lo": partition.lo} if partition.lo is not None else {}),
                    **({"hi": partition.hi} if partition.hi is not None else {}),
                },
            )
            for partition in partitions
        )

    # A kind whose jobs are expensive to start groups partitions together, and
    # the group becomes the checkpoint unit. The grouping happens here, at plan
    # time, and not in the executor: the checkpoint covers a node, so a group
    # that is not a node cannot be checkpointed as one.
    groups = group_partitions(
        partitions,
        cost=BEAM_COST,
        overhead_fraction=movement.execution.overhead_fraction,
    )
    return tuple(
        PlanNode(
            id=node_id(movement.name, NodeKind.SNAPSHOT_GROUP, unit_of(group)),
            kind=NodeKind.SNAPSHOT_GROUP,
            stage=LifecycleStage.EXECUTE,
            scope=unit_of(group),
            depends_on=depends_on,
            params=_snapshot_params(dataset)
            | {
                "partition_method": group[0].method.value,
                # The bounds the plan recorded, verbatim. Read back rather than
                # recomputed at execution time: recomputing would let a
                # partition move under a replay, and its checkpoint would then
                # refer to work that no longer exists.
                "partitions": json.dumps(
                    [{"index": part.index, "lo": part.lo, "hi": part.hi} for part in group],
                    separators=(",", ":"),
                ),
            },
        )
        for group in groups
    )


def _snapshot_params(dataset: MovementDataset) -> dict[str, str]:
    params = {"write_mode": dataset.write_mode.value}
    if dataset.partitioning is not None:
        params["partition_strategy"] = dataset.partitioning.strategy.value
        params["partition_column"] = dataset.partitioning.column
    return params
