"""Compile a Movement into an execution plan.

Partition-level nodes are not produced here: partition bounds come from
discovery and profiling, which have not run at plan time. The plan carries a
per-dataset snapshot node that expands into partitions once bounds exist.
"""

from __future__ import annotations

from datetime import datetime

from gantry.core.operation import LifecycleStage, OperationType
from gantry.lifecycle.plan import NodeKind, PlanNode, PlanVersion, node_id
from gantry.movement.model import Movement, MovementDataset, MovementMode


def compile_movement(movement: Movement, *, created_at: datetime, version: int = 1) -> PlanVersion:
    """Build the deterministic plan for a Movement."""
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
    for dataset in movement.datasets:
        scope = dataset.name

        schema = PlanNode(
            id=node_id(movement.name, NodeKind.CREATE_SCHEMA, scope),
            kind=NodeKind.CREATE_SCHEMA,
            stage=LifecycleStage.EXECUTE,
            scope=scope,
            depends_on=(discover.id,),
        )

        # Dataset dependencies become plan edges: a dataset's snapshot waits
        # for the snapshots of everything it depends on.
        upstream = tuple(
            node_id(movement.name, NodeKind.SNAPSHOT_PARTITION, dependency)
            for dependency in sorted(dataset.depends_on)
        )
        snapshot = PlanNode(
            id=node_id(movement.name, NodeKind.SNAPSHOT_PARTITION, scope),
            kind=NodeKind.SNAPSHOT_PARTITION,
            stage=LifecycleStage.EXECUTE,
            scope=scope,
            depends_on=(schema.id, *upstream),
            params=_snapshot_params(dataset),
        )

        verify = PlanNode(
            id=node_id(movement.name, NodeKind.VERIFY_DATASET, scope),
            kind=NodeKind.VERIFY_DATASET,
            stage=LifecycleStage.VERIFY,
            scope=scope,
            depends_on=(snapshot.id,),
            params={"checks": ",".join(sorted(c.check.value for c in dataset.verification))},
        )
        nodes.extend((schema, snapshot, verify))
        verify_ids.append(verify.id)

    if streaming and start_cdc_id is not None:
        snapshot_ids = tuple(
            node_id(movement.name, NodeKind.SNAPSHOT_PARTITION, dataset.name)
            for dataset in movement.datasets
        )
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


def _snapshot_params(dataset: MovementDataset) -> dict[str, str]:
    params = {"write_mode": dataset.write_mode.value}
    if dataset.partitioning is not None:
        params["partition_strategy"] = dataset.partitioning.strategy.value
        params["partition_column"] = dataset.partitioning.column
    return params
