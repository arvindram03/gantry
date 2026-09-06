# SPDX-License-Identifier: Apache-2.0
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
from gantry.jobs import Runner, run_to_completion
from gantry.jobs.packaging import sql_client_packaging
from gantry.jobs.runners import DockerRunner
from gantry.lifecycle.plan import NodeKind, PlanNode
from gantry.movement.jobdsn import JobConnections
from gantry.movement.partitioning import Partition, PartitionMethod
from gantry.movement.sqljob import SOURCE_DSN, TARGET_DSN, compile_snapshot_job, parse_commit


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
        operation: str,
        manifests: dict[str, DatasetManifest],
        targets: dict[str, str],
        connections: JobConnections | None = None,
        runner: Runner | None = None,
    ) -> None:
        self._source_engine = source_engine
        self._target_engine = target_engine
        self._source = PostgresSourceAdapter(source_engine)
        self._target = PostgresTargetAdapter(target_engine)
        self._operation = operation
        self._manifests = manifests
        self._targets = targets
        # How a job reaches the databases, which is not how this process
        # reaches them: the job runs somewhere else. Derived from the engines
        # when unset, which is right whenever the job shares this process's
        # view of the network and loudly wrong when it does not - the job fails
        # to connect, so no commit is attested and no checkpoint advances.
        self._connections = connections or JobConnections.from_env(
            source_engine=source_engine, target_engine=target_engine
        )
        self._runner = runner or DockerRunner(
            secrets={
                SOURCE_DSN: self._connections.source,
                TARGET_DSN: self._connections.target,
            }
        )

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

        job = compile_snapshot_job(
            self._operation,
            manifest,
            partition,
            target=self._target_for(dataset),
            packaging=sql_client_packaging(
                secrets=(SOURCE_DSN, TARGET_DSN), network=self._connections.network
            ),
        )
        return parse_commit(await run_to_completion(self._runner, job))

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
