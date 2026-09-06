# SPDX-License-Identifier: Apache-2.0
"""Executing Movement plan nodes against real adapters.

Each node kind maps to one adapter call. The executor's only other job is to
return the `CommitResult` that permits a checkpoint to advance: a node that
cannot attest durability returns one whose commit time is the moment the work
was confirmed, and nothing else in the runtime can manufacture one.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.adapters.target.postgres import PostgresTargetAdapter
from gantry.core.commit import CommitResult
from gantry.core.dataset import DatasetManifest
from gantry.jobs import Runner, run_to_completion
from gantry.jobs.packaging import beam_packaging, sql_client_packaging, verification_packaging
from gantry.jobs.runners import DockerRunner
from gantry.lifecycle.plan import NodeKind, PlanNode
from gantry.movement.beamjob import JDBC_SECRETS, IcebergSink, JdbcSink, unit_of
from gantry.movement.beamjob import compile_snapshot_job as beam_compile_snapshot_job
from gantry.movement.iceberg import ensure_group
from gantry.movement.jobdsn import JobConnections
from gantry.movement.partitioning import Partition, PartitionMethod
from gantry.movement.predicate import bounds
from gantry.movement.sqljob import SOURCE_DSN, TARGET_DSN, compile_snapshot_job, parse_commit
from gantry.verification.checksum import compute_checksum
from gantry.verification.iceberg import compile_checksum_job


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
        destination: str = "postgres",
        warehouse: str | None = None,
    ) -> None:
        self._source_engine = source_engine
        self._target_engine = target_engine
        self._source = PostgresSourceAdapter(source_engine)
        self._target = PostgresTargetAdapter(target_engine)
        self._operation = operation
        # Which kind of thing the data is being written into. `postgres` writes
        # through JDBC with an upsert; `iceberg` appends, and therefore needs the
        # verify-first dance in `gantry.movement.iceberg` to survive a replay.
        self._destination = destination
        self._warehouse = warehouse
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
            case NodeKind.SNAPSHOT_GROUP:
                return await self._snapshot_group(node)
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
        committed = parse_commit(await run_to_completion(self._runner, job))
        return committed.model_copy(update={"job": job.content_hash})

    async def _snapshot_group(self, node: PlanNode) -> CommitResult:
        """Several partitions in one job, checkpointed only once verified.

        A Beam pipeline reports no per-row counts a submitter can read: reaching
        `DONE` says it finished, not that the rows are right, and a checkpoint
        may not advance on that alone. So the verification *is* the attestation
        here — the group's checksums must agree on both sides before this
        returns a `CommitResult`, and returning one is the only thing that lets
        a checkpoint move.

        A group that does not verify raises. The task fails, the lease expires,
        and the group runs again — which is safe because the write is an upsert,
        and correct because nothing recorded progress over rows that were never
        confirmed.
        """
        dataset = _dataset_of_group(node)
        manifest = self._manifest_for(dataset)
        partitions = _partitions_from_group(node, dataset)
        target = self._target_for(dataset)

        moved_by: str | None = None
        if self._destination == "iceberg":
            agreed, moved_by = await self._move_into_iceberg(manifest, partitions, target=target)
        else:
            job = beam_compile_snapshot_job(
                self._operation,
                manifest,
                partitions,
                sink=JdbcSink(table=target),
                packaging=beam_packaging(secrets=JDBC_SECRETS, network=self._connections.network),
            )
            await run_to_completion(self._runner, job)
            moved_by = job.content_hash
            agreed = await self._verify_group(manifest, partitions, target=target)
        return CommitResult(
            # Deliberately zero. Beam reports no counts, and inventing them from
            # the verification would be a different number wearing the same
            # name: the checksum says the sides agree, not how many rows this
            # job wrote.
            rows_inserted=0,
            rows_updated=0,
            rows_unchanged=agreed,
            job=moved_by,
            committed_at=datetime.now(UTC),
        )

    async def _move_into_iceberg(
        self, manifest: DatasetManifest, partitions: Sequence[Partition], *, target: str
    ) -> tuple[int, str]:
        """Move a group into Iceberg, and survive being asked to do it twice.

        Iceberg's write appends, so the move is guarded by a check of what the
        target already holds. The source's checksum is computed in its own
        engine; the target's by a job, because an Iceberg table has no engine to
        ask and streaming its rows here to add them up is the one thing this
        design forbids.
        """
        if self._warehouse is None:
            raise ValueError(
                "an iceberg destination needs a warehouse location; "
                "none was configured for this executor"
            )

        predicate = _group_predicate(manifest, partitions)
        expected = await compute_checksum(
            self._source_engine, manifest, manifest.name, predicate=predicate, params={}
        )
        mounts = ((self._warehouse, self._warehouse),)

        move = beam_compile_snapshot_job(
            self._operation,
            manifest,
            partitions,
            sink=IcebergSink(table=target, warehouse=self._warehouse),
            packaging=beam_packaging(
                secrets=JDBC_SECRETS, network=self._connections.network, mounts=mounts
            ),
        )
        verify = compile_checksum_job(
            self._operation,
            manifest,
            warehouse=self._warehouse,
            table=target,
            unit=unit_of(partitions),
            packaging=verification_packaging(mounts=mounts),
        )
        outcome = await ensure_group(self._runner, verify=verify, move=move, expected=expected)
        return outcome.checksum.rows, move.content_hash

    async def _verify_group(
        self, manifest: DatasetManifest, partitions: Sequence[Partition], *, target: str
    ) -> int:
        """Compare both sides over the group's key range. Returns the row count.

        Computed inside each engine, so what crosses the wire is one checksum
        and one count per side rather than any rows.
        """
        predicate = _group_predicate(manifest, partitions)
        source_side = await compute_checksum(
            self._source_engine, manifest, manifest.name, predicate=predicate, params={}
        )
        target_side = await compute_checksum(
            self._target_engine, manifest, target, predicate=predicate, params={}
        )
        if source_side != target_side:
            raise GroupVerificationError(
                f"group {unit_of(partitions)} did not verify: "
                f"source {source_side.describe()}, target {target_side.describe()}"
            )
        return source_side.rows

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


def _group_predicate(manifest: DatasetManifest, partitions: Sequence[Partition]) -> str:
    """The group's key range, as one SQL predicate."""
    return " OR ".join(f"({bounds(manifest, part)})" for part in partitions)


class GroupVerificationError(Exception):
    """A group's sides disagreed, so nothing may be checkpointed for it."""


def _dataset_of_group(node: PlanNode) -> str:
    """A group scope is a comma-separated list of `dataset/index` ids."""
    first = (node.scope or "").split(",")[0]
    return _dataset_of(first)


def _partitions_from_group(node: PlanNode, dataset: str) -> tuple[Partition, ...]:
    """Rebuild the partitions the plan recorded for this group.

    Read back rather than recomputed, for the same reason a single partition is:
    recomputing at execution time would let the group's membership shift under a
    replay, and its checkpoint would then cover work that no longer exists.
    """
    recorded = json.loads(node.params["partitions"])
    method = PartitionMethod(node.params.get("partition_method", "single"))
    column = node.params["partition_column"]
    return tuple(
        Partition(
            dataset=dataset,
            index=int(entry["index"]),
            column=column,
            lo=entry.get("lo"),
            hi=entry.get("hi"),
            method=method,
        )
        for entry in recorded
    )


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
