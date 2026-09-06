# SPDX-License-Identifier: Apache-2.0
"""A generated SQL job, actually run.

The unit tests check the script's *shape*. This checks that it works — which is
a different question, and the one that matters, because every interesting bug
found on Day 0 produced a script that looked correct.

The escaping test here is the point of the file. A partition bound is a value
out of the customer's data, written into SQL, written into a shell script. Two
layers, and asserting on the escaped bytes tests arithmetic rather than safety.
Running a hostile bound against a real database tests safety.

Requires Docker and the dev stack.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.core.dataset import DatasetManifest
from gantry.jobs import JobState
from gantry.jobs.packaging import sql_client_packaging
from gantry.jobs.runners import DockerRunner
from gantry.lifecycle.plan import LifecycleStage, NodeKind, PlanNode
from gantry.movement.executor import MovementExecutor
from gantry.movement.jobdsn import JobConnections
from gantry.movement.partitioning import Partition, PartitionMethod
from gantry.movement.sqljob import (
    SOURCE_DSN,
    TARGET_DSN,
    compile_snapshot_job,
    parse_commit,
)
from gantry.state.database import create_engine, transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

NETWORK = os.environ.get("GANTRY_DOCKER_NETWORK", "gantry-dev_default")
# As seen from inside the network, not from the host.
INSIDE_SOURCE = "postgresql://gantry:gantry@gantry-pg-source:5432/gantry"
INSIDE_TARGET = "postgresql://gantry:gantry@gantry-pg-target:5432/gantry"

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)

TABLE = "public.sqljob_probe"
ROWS = 5_000


@pytest.fixture
async def sides() -> AsyncIterator[tuple[AsyncEngine, AsyncEngine]]:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    source = create_engine(SOURCE_URL)
    target = create_engine(TARGET_URL)

    async def build() -> None:
        async with transaction(source) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await connection.execute(
                text(
                    f"CREATE TABLE {TABLE} ("
                    "  id bigint PRIMARY KEY, label text, amount numeric(12,2) NOT NULL,"
                    "  source_lsn bigint NOT NULL DEFAULT 0)"
                )
            )
            await connection.execute(
                text(
                    f"INSERT INTO {TABLE} SELECT g, 'row' || g, g * 1.5, 0 "
                    f"FROM generate_series(1, {ROWS}) g"
                )
            )
            await connection.execute(text(f"ANALYZE {TABLE}"))
        async with transaction(target) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await connection.execute(
                text(
                    f"CREATE TABLE {TABLE} ("
                    "  id bigint PRIMARY KEY, label text, amount numeric(12,2) NOT NULL,"
                    "  source_lsn bigint NOT NULL DEFAULT 0)"
                )
            )

    try:
        await build()
        yield source, target
    finally:
        for engine in (source, target):
            async with transaction(engine) as connection:
                await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await engine.dispose()


def runner() -> DockerRunner:
    return DockerRunner(secrets={SOURCE_DSN: INSIDE_SOURCE, TARGET_DSN: INSIDE_TARGET})


async def manifest_of(source: AsyncEngine) -> DatasetManifest:
    found = {m.name: m for m in await PostgresSourceAdapter(source).discover()}
    return found[TABLE]


def partition(index: int, lo: str | None, hi: str | None) -> Partition:
    return Partition(
        dataset=TABLE,
        index=index,
        column="id",
        method=PartitionMethod.HISTOGRAM,
        lo=lo,
        hi=hi,
    )


async def run_job_with_output(
    source: AsyncEngine, part: Partition, **overrides: object
) -> tuple[JobState, str]:
    job = compile_snapshot_job(
        "sqljob-probe",
        await manifest_of(source),
        part,
        target=TABLE,
        packaging=sql_client_packaging(secrets=(SOURCE_DSN, TARGET_DSN), network=NETWORK),
        **overrides,  # type: ignore[arg-type]
    )
    engine = runner()
    handle = await engine.submit(job)
    try:
        deadline = asyncio.get_running_loop().time() + 60
        while asyncio.get_running_loop().time() < deadline:
            status = await engine.poll(handle)
            if status.state.terminal:
                if status.state is JobState.FAILED:
                    print(status.detail)
                return status.state, await engine.logs(handle)
            await asyncio.sleep(0.1)
        raise AssertionError("the job never finished")
    finally:
        await engine._run(["docker", "rm", "-f", handle.id])


async def run_job(source: AsyncEngine, part: Partition, **overrides: object) -> JobState:
    state, _ = await run_job_with_output(source, part, **overrides)
    return state


async def target_rows(target: AsyncEngine) -> int:
    async with transaction(target) as connection:
        return int((await connection.execute(text(f"SELECT count(*) FROM {TABLE}"))).scalar_one())


async def test_a_generated_job_moves_its_partition(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    source, target = sides
    assert await run_job(source, partition(0, "1", "1001")) is JobState.SUCCEEDED
    assert await target_rows(target) == 1000


async def test_partitions_do_not_overlap_or_leave_gaps(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    source, target = sides
    for index, (lo, hi) in enumerate([("1", "2001"), ("2001", "4001"), ("4001", None)]):
        assert await run_job(source, partition(index, lo, hi)) is JobState.SUCCEEDED
    assert await target_rows(target) == ROWS


async def test_running_the_same_job_twice_changes_nothing(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Idempotence, which every guarantee above this rests on. Run twice with
    the container removed in between, so it genuinely re-executes rather than
    being adopted."""
    source, target = sides
    part = partition(0, "1", "1001")
    assert await run_job(source, part) is JobState.SUCCEEDED
    assert await run_job(source, part) is JobState.SUCCEEDED
    assert await target_rows(target) == 1000


async def test_a_hostile_partition_bound_cannot_break_out(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The reason this file exists.

    A bound is customer data written into SQL written into a shell script. The
    payload is shaped to close the `COPY (...)` subquery it lands inside, so
    that a missing escape genuinely executes the DROP rather than merely
    producing a parse error — an injection test that can only ever yield a
    syntax error proves nothing.

    Note the assertion is against the *source*: the bound goes into the
    source-side COPY, so that is the table an injection would drop.
    """
    source, _ = sides
    payload = f"1' AS bigint)) TO STDOUT; DROP TABLE {TABLE}; --"
    await run_job(source, partition(0, payload, None))

    async with transaction(source) as connection:
        still_there = (
            await connection.execute(text(f"SELECT to_regclass('{TABLE}') IS NOT NULL"))
        ).scalar_one()
    assert still_there, "the generated script executed injected SQL"


async def test_a_failed_partition_does_not_undo_a_committed_one(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The per-partition commit boundary.

    This is the guarantee the plan claims for `sql` jobs: partition-granular,
    with the commit boundary Gantry's own. One partition commits, the next
    fails; the committed one must survive, and the failed one must leave
    nothing. Anything less and a resume would have to re-do arbitrary work.

    (`--single-transaction` itself is proven by the tests above: without it the
    `ON COMMIT DROP` staging table cannot survive to the merge, and they fail.)
    """
    source, target = sides
    assert await run_job(source, partition(0, "1", "1001")) is JobState.SUCCEEDED
    assert await target_rows(target) == 1000

    # Reject only what the second partition carries, so the first stays valid.
    async with transaction(target) as connection:
        await connection.execute(
            text(f"ALTER TABLE {TABLE} ADD CONSTRAINT small_only CHECK (amount < 1600)")
        )

    assert await run_job(source, partition(1, "1001", "2001")) is JobState.FAILED
    assert await target_rows(target) == 1000, (
        "a failed partition must neither commit its own rows nor disturb a committed one"
    )


async def test_the_job_reports_what_it_committed(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The counts a checkpoint rests on.

    The executor may not advance a checkpoint without a CommitResult, and a
    container reports only an exit code. These are the same three numbers the
    in-process relay derived, so a replay must show up as unchanged rather than
    as work done again.
    """
    source, _ = sides
    part = partition(0, "1", "1001")

    state, output = await run_job_with_output(source, part)
    assert state is JobState.SUCCEEDED
    first = parse_commit(output)
    assert (first.rows_inserted, first.rows_updated, first.rows_unchanged) == (1000, 0, 0)

    state, output = await run_job_with_output(source, part)
    assert state is JobState.SUCCEEDED
    replay = parse_commit(output)
    assert replay.rows_inserted == 0, "a replay must not report new rows"
    assert replay.rows_updated == 1000


async def test_a_snapshot_behind_the_stream_reports_rows_it_declined(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Rows the merge refuses to overwrite are unchanged, not lost.

    This is the count that keeps a slow snapshot from silently undoing the
    change stream: the rows it declined must be visible as declined.
    """
    source, target = sides
    async with transaction(target) as connection:
        await connection.execute(
            text(
                f"INSERT INTO {TABLE} SELECT g, 'newer' || g, g * 1.5, 500 "
                f"FROM generate_series(1, 100) g"
            )
        )

    _, output = await run_job_with_output(source, partition(0, "1", "1001"), snapshot_lsn=1)
    result = parse_commit(output)
    assert result.rows_unchanged == 100, "the 100 newer rows must be declined, not overwritten"
    assert result.rows_inserted == 900


async def test_the_snapshot_stamps_rows_with_its_own_position(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """A snapshot's rows are as of the snapshot's position.

    Not as of whatever the source row happened to carry. Without the stamp the
    target understates how current it is, and every change between the slot and
    the snapshot gets re-applied by the stream - which the handoff tests catch
    only indirectly, as a change that should have been refused being accepted.
    """
    source, target = sides
    await run_job(source, partition(0, "1", "101"), snapshot_lsn=7777)

    async with transaction(target) as connection:
        positions = (
            (await connection.execute(text(f"SELECT DISTINCT source_lsn FROM {TABLE}")))
            .scalars()
            .all()
        )
    assert list(positions) == [7777], "rows must carry the snapshot's position, not the source's"


async def test_the_commit_names_the_job_that_produced_it(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Provenance, not bookkeeping.

    A job body is a readable artifact retained on purpose; this is what ties
    the rows that arrived to the exact thing that moved them. The hash is
    opaque to everything above, which is what lets the worker record which job
    ran without learning what kind it was.
    """
    source, _ = sides
    executor = MovementExecutor(
        source_engine=source,
        target_engine=create_engine(TARGET_URL),
        operation="sqljob-provenance",
        manifests={TABLE: await manifest_of(source)},
        targets={TABLE: TABLE},
        connections=JobConnections(source=INSIDE_SOURCE, target=INSIDE_TARGET, network=NETWORK),
    )
    node = PlanNode(
        id="probe-partition",
        kind=NodeKind.SNAPSHOT_PARTITION,
        stage=LifecycleStage.EXECUTE,
        scope=f"{TABLE}/00000",
        params={
            "partition_column": "id",
            "partition_method": PartitionMethod.HISTOGRAM.value,
            "partition_index": "0",
            "lo": "1",
            "hi": "1001",
        },
    )
    result = await executor.execute(node)
    assert result.rows_inserted == 1000
    assert result.job is not None and result.job.startswith("sha256:"), (
        "a commit must name the job that produced it"
    )
