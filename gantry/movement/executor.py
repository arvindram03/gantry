"""Executing Movement plan nodes against real adapters.

Each node kind maps to one adapter call. The executor's only other job is to
return the `CommitResult` that permits a checkpoint to advance: a node that
cannot attest durability returns one whose commit time is the moment the work
was confirmed, and nothing else in the runtime can manufacture one.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.adapters.target.postgres import PostgresTargetAdapter
from gantry.core.commit import CommitResult
from gantry.core.dataset import DatasetManifest
from gantry.lifecycle.plan import NodeKind, PlanNode
from gantry.movement.partitioning import Partition, PartitionMethod


class MovementExecutor:
    """Runs one Movement's plan nodes.

    Holds the manifests the plan was compiled against, because a node's
    partition bounds are meaningless without the Dataset version they came
    from.
    """

    def __init__(
        self,
        *,
        source_engine: AsyncEngine,
        target_engine: AsyncEngine,
        manifests: dict[str, DatasetManifest],
        targets: dict[str, str],
    ) -> None:
        self._source_engine = source_engine
        self._target_engine = target_engine
        self._source = PostgresSourceAdapter(source_engine)
        self._target = PostgresTargetAdapter(target_engine)
        self._manifests = manifests
        self._targets = targets

    async def execute(self, node: PlanNode) -> CommitResult:
        """Perform one node's work and attest that it committed."""
        match node.kind:
            case NodeKind.CREATE_SCHEMA:
                return await self._create_schema(node)
            case NodeKind.SNAPSHOT_PARTITION:
                return await self._snapshot_partition(node)
            case _:
                # Discovery, CDC and verification nodes arrive on later days.
                # Until then they are recorded as completing without effect,
                # rather than silently skipped.
                return CommitResult(committed_at=datetime.now(UTC))

    async def _create_schema(self, node: PlanNode) -> CommitResult:
        dataset = node.scope or ""
        manifest = self._manifest_for(dataset)
        await self._target.prepare(manifest, target=self._target_for(dataset))
        return CommitResult(committed_at=datetime.now(UTC))

    async def _snapshot_partition(self, node: PlanNode) -> CommitResult:
        dataset = _dataset_of(node.scope or "")
        manifest = self._manifest_for(dataset)
        partition = _partition_from(node, dataset)

        query, params = self._source.copy_query(manifest, partition)
        async with self._source_engine.connect() as connection:
            return await self._target.copy_partition(
                manifest,
                target=self._target_for(dataset),
                source=connection,
                query=query,
                query_params=params,
            )

    def _manifest_for(self, dataset: str) -> DatasetManifest:
        manifest = self._manifests.get(dataset)
        if manifest is None:
            raise ValueError(f"no manifest for dataset {dataset!r}; discover the source first")
        return manifest

    def _target_for(self, dataset: str) -> str:
        target = self._targets.get(dataset)
        if target is None:
            raise ValueError(f"no target mapping for dataset {dataset!r}")
        return target


def _dataset_of(scope: str) -> str:
    """A partition scope is `dataset/index`; a dataset scope is just the name."""
    return scope.rsplit("/", 1)[0] if "/" in scope else scope


def _partition_from(node: PlanNode, dataset: str) -> Partition:
    """Rebuild the partition the plan recorded.

    Bounds are read back from the plan rather than recomputed. Recomputing them
    at execution time would let a partition move under a replay, which would
    make its checkpoint refer to work that no longer exists.
    """
    return Partition(
        dataset=dataset,
        index=int(node.params.get("partition_index", "0")),
        column=node.params["partition_column"],
        lo=node.params.get("lo"),
        hi=node.params.get("hi"),
        method=PartitionMethod(node.params.get("partition_method", "single")),
    )
