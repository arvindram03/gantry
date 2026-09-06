# SPDX-License-Identifier: Apache-2.0
"""A generated Beam job, actually run.

Requires Docker, the dev stack, and the `gantry/beam` image built from
`docker/beam/Dockerfile` — which carries a JRE and pre-staged JARs because
`apache_beam.io.jdbc` is a cross-language transform. Skipped if the image is
absent rather than pulling 4.65 GB behind someone's back.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.core.dataset import DatasetManifest
from gantry.jobs.execute import JobFailedError, run_to_completion
from gantry.jobs.packaging import DEFAULT_BEAM_IMAGE, beam_packaging
from gantry.jobs.runners import DockerRunner
from gantry.lifecycle.plan import LifecycleStage, NodeKind, PlanNode
from gantry.movement.beamjob import COMMIT_MARKER, JDBC_SECRETS, compile_snapshot_job
from gantry.movement.executor import GroupVerificationError, MovementExecutor
from gantry.movement.jobdsn import JobConnections
from gantry.movement.partitioning import Partition, PartitionMethod
from gantry.state.database import create_engine, transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

NETWORK = os.environ.get("GANTRY_JOB_NETWORK", "gantry-dev_default")
SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)
SECRETS = {
    "GANTRY_SOURCE_JDBC": "jdbc:postgresql://gantry-pg-source:5432/gantry",
    "GANTRY_TARGET_JDBC": "jdbc:postgresql://gantry-pg-target:5432/gantry",
    "GANTRY_SOURCE_USER": "gantry",
    "GANTRY_SOURCE_PASSWORD": "gantry",
    "GANTRY_TARGET_USER": "gantry",
    "GANTRY_TARGET_PASSWORD": "gantry",
}

TABLE = "public.beamjob_probe"
ROWS = 2_000


def require_image() -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    found = subprocess.run(
        ["docker", "image", "inspect", DEFAULT_BEAM_IMAGE],
        capture_output=True,
        check=False,
    )
    if found.returncode != 0:
        pytest.skip(f"{DEFAULT_BEAM_IMAGE} is not built; see docker/beam/Dockerfile")


@pytest.fixture
async def sides() -> AsyncIterator[tuple[AsyncEngine, AsyncEngine]]:
    require_image()
    source = create_engine(SOURCE_URL)
    target = create_engine(TARGET_URL)
    try:
        for engine in (source, target):
            async with transaction(engine) as connection:
                await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
                await connection.execute(
                    text(
                        f"CREATE TABLE {TABLE} (id bigint PRIMARY KEY, label text,"
                        " amount numeric(12,2) NOT NULL, source_lsn bigint NOT NULL DEFAULT 0)"
                    )
                )
        async with transaction(source) as connection:
            await connection.execute(
                text(
                    f"INSERT INTO {TABLE} SELECT g, 'row' || g, g * 1.5, 0 "
                    f"FROM generate_series(1, {ROWS}) g"
                )
            )
            await connection.execute(text(f"ANALYZE {TABLE}"))
        yield source, target
    finally:
        for engine in (source, target):
            async with transaction(engine) as connection:
                await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await engine.dispose()


def _group_node(bounds: list[tuple[int, str | None, str | None]]) -> PlanNode:
    """A SNAPSHOT_GROUP node shaped exactly as the planner emits one."""
    return PlanNode(
        id="probe-group",
        kind=NodeKind.SNAPSHOT_GROUP,
        stage=LifecycleStage.EXECUTE,
        scope=",".join(f"{TABLE}/{index:05d}" for index, _, _ in bounds),
        params={
            "partition_column": "id",
            "partition_method": PartitionMethod.HISTOGRAM.value,
            "partitions": json.dumps([{"index": i, "lo": lo, "hi": hi} for i, lo, hi in bounds]),
        },
    )


async def manifest_of(source: AsyncEngine) -> DatasetManifest:
    return {m.name: m for m in await PostgresSourceAdapter(source).discover()}[TABLE]


def partition(index: int, lo: str | None, hi: str | None) -> Partition:
    return Partition(
        dataset=TABLE,
        index=index,
        column="id",
        method=PartitionMethod.HISTOGRAM,
        lo=lo,
        hi=hi,
    )


async def run_beam(
    source: AsyncEngine, partitions: list[Partition], *, snapshot_lsn: int | None = None
) -> str:
    job = compile_snapshot_job(
        "beamjob-probe",
        await manifest_of(source),
        partitions,
        target=TABLE,
        snapshot_lsn=snapshot_lsn,
        packaging=beam_packaging(secrets=JDBC_SECRETS, network=NETWORK),
    )
    runner = DockerRunner(secrets=SECRETS)
    return await run_to_completion(runner, job, poll_interval=0.2)


async def target_rows(target: AsyncEngine) -> int:
    async with transaction(target) as connection:
        return int((await connection.execute(text(f"SELECT count(*) FROM {TABLE}"))).scalar_one())


async def test_a_generated_pipeline_moves_its_partitions(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """One job, several partitions — which is what Beam's startup cost forces."""
    source, target = sides
    output = await run_beam(source, [partition(0, "1", "1001"), partition(1, "1001", "2001")])
    assert COMMIT_MARKER in output
    assert await target_rows(target) == ROWS


async def test_running_the_same_beam_job_twice_changes_nothing(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Beam's own JDBC write is a plain INSERT, so this fails outright unless
    the generated upsert is doing the work."""
    source, target = sides
    parts = [partition(0, "1", "1001")]
    await run_beam(source, parts)
    await run_beam(source, parts)
    assert await target_rows(target) == 1000


async def test_a_beam_job_stamps_the_snapshot_position(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Stamped in the read, because Beam moves rows opaquely and no later point
    still knows what the position was."""
    source, target = sides
    await run_beam(source, [partition(0, "1", "101")], snapshot_lsn=4242)
    async with transaction(target) as connection:
        positions = (
            (await connection.execute(text(f"SELECT DISTINCT source_lsn FROM {TABLE}")))
            .scalars()
            .all()
        )
    assert list(positions) == [4242]


async def test_a_bad_row_is_a_job_failure_not_a_runner_failure(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The distinction Day 3 exists for.

    A row the target refuses will be refused again, so it must not be reported
    as an interrupted job — that is how a bad row gets retried until something
    gives up. The inverse mistake quarantines a node for an infrastructure
    hiccup.
    """
    source, target = sides
    async with transaction(target) as connection:
        await connection.execute(
            text(f"ALTER TABLE {TABLE} ADD CONSTRAINT tiny_only CHECK (amount < 2)")
        )

    with pytest.raises(JobFailedError):
        await run_beam(source, [partition(0, "1", "1001")])


async def test_a_beam_snapshot_refuses_to_overwrite_a_newer_row(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The guard that is load-bearing for Beam in a way it is not for a script.

    A pipeline's bundles are unordered by design and may be retried, so "the
    snapshot wrote it second" carries no information about which value is
    current. Without the guard a partition copied slowly enough silently undoes
    changes the stream already applied, and the row looks consistent afterwards.
    """
    source, target = sides
    async with transaction(target) as connection:
        await connection.execute(
            text(
                f"INSERT INTO {TABLE} SELECT g, 'from-cdc', g * 1.5, 5000 "
                f"FROM generate_series(1, 100) g"
            )
        )

    await run_beam(source, [partition(0, "1", "1001")], snapshot_lsn=1)

    async with transaction(target) as connection:
        survivors = (
            await connection.execute(text(f"SELECT count(*) FROM {TABLE} WHERE label = 'from-cdc'"))
        ).scalar_one()
        moved = await target_rows(target)

    assert survivors == 100, "the snapshot overwrote rows newer than its own position"
    assert moved == 1000, "and the rows it was entitled to write are still there"


async def test_a_group_is_checkpointed_only_after_it_verifies(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The rule that makes `DONE` safe to act on.

    A Beam pipeline reports no counts a submitter can read, so reaching DONE
    says it finished and nothing about whether the rows are right. The executor
    therefore verifies the group itself, and the CommitResult - the only thing
    that lets a checkpoint advance - is produced by that verification.
    """
    source, target = sides
    executor = MovementExecutor(
        source_engine=source,
        target_engine=target,
        operation="beam-group-probe",
        manifests={TABLE: await manifest_of(source)},
        targets={TABLE: TABLE},
        connections=JobConnections(
            source=SECRETS["GANTRY_SOURCE_JDBC"],
            target=SECRETS["GANTRY_TARGET_JDBC"],
            network=NETWORK,
        ),
        runner=DockerRunner(secrets=SECRETS),
    )

    result = await executor.execute(_group_node([(0, "1", "1001"), (1, "1001", "2001")]))
    assert result.rows_unchanged == ROWS, "the verification counts what it compared"
    assert await target_rows(target) == ROWS


async def test_a_group_that_does_not_verify_is_not_checkpointed(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """No CommitResult, so nothing can record progress over the group.

    The disagreement is a row the target holds and the source does not, which
    an upsert cannot repair — so the job keeps succeeding and the verification
    keeps refusing, which is the behaviour that matters. A partially-committed
    Beam group looks like this from the outside: bundles land independently, so
    the target can hold rows that no longer reconcile with the source.
    """
    source, target = sides
    executor = MovementExecutor(
        source_engine=source,
        target_engine=target,
        operation="beam-group-probe",
        manifests={TABLE: await manifest_of(source)},
        targets={TABLE: TABLE},
        connections=JobConnections(
            source=SECRETS["GANTRY_SOURCE_JDBC"],
            target=SECRETS["GANTRY_TARGET_JDBC"],
            network=NETWORK,
        ),
        runner=DockerRunner(secrets=SECRETS),
    )
    node = _group_node([(0, "1", "1001")])
    await executor.execute(node)

    # Remove a row from the source. The target keeps it, and an upsert has no
    # way to delete it, so the job goes on succeeding while the two sides go on
    # disagreeing.
    async with transaction(source) as connection:
        await connection.execute(text(f"DELETE FROM {TABLE} WHERE id = 5"))

    with pytest.raises(GroupVerificationError) as raised:
        await executor.execute(node)
    assert "did not verify" in str(raised.value)
