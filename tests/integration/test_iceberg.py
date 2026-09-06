# SPDX-License-Identifier: Apache-2.0
"""Postgres to Iceberg, verified.

The reach claim, and the only place it is actually tested. Requires Docker, the
dev stack, the `gantry/beam` image (which writes) and the `gantry/verify` image
(which reads it back and computes a checksum).

The warehouse is mounted at the same absolute path inside the container as
outside it, because Iceberg metadata records absolute locations. In production
both sides name one `s3://` URI and the question does not arise; on a laptop
with a local filesystem it has to be arranged.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.adapters.target.iceberg import UnsupportedTypeError, iceberg_schema
from gantry.core import DatasetManifest
from gantry.jobs.model import Job, JobKind
from gantry.jobs.packaging import ContainerPackaging
from gantry.jobs.runners import DockerRunner
from gantry.movement.iceberg import GroupOutcome, ensure_group
from gantry.state.database import create_engine, transaction
from gantry.verification.checksum import compute_checksum
from gantry.verification.iceberg import checksum_script, parse_checksum
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

NETWORK = os.environ.get("GANTRY_JOB_NETWORK", "gantry-dev_default")
SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
WAREHOUSE = Path(os.environ.get("GANTRY_ICEBERG_WAREHOUSE", "/tmp/gantry-warehouse"))
BEAM_IMAGE = "gantry/beam:2.76.0"
VERIFY_IMAGE = "gantry/verify:dev"

TABLE = "public.iceberg_probe"
ICEBERG_TABLE = "gantry.iceberg_probe"
ROWS = 1_000
DDL = (
    "id bigint PRIMARY KEY, label text, amount numeric(12,2), "
    "ratio double precision, source_lsn bigint NOT NULL DEFAULT 0"
)


def require(image: str) -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    found = subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=False)
    if found.returncode != 0:
        pytest.skip(f"{image} is not built; see docker/")


@pytest.fixture
async def source() -> AsyncIterator[AsyncEngine]:
    require(BEAM_IMAGE)
    require(VERIFY_IMAGE)
    # Blocking filesystem calls in an async fixture, deliberately: this is
    # setup, nothing else is running, and a thread pool would only obscure it.
    WAREHOUSE.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240
    shutil.rmtree(WAREHOUSE / "gantry", ignore_errors=True)
    engine = create_engine(SOURCE_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await connection.execute(text(f"CREATE TABLE {TABLE} ({DDL})"))
            await connection.execute(
                text(
                    f"INSERT INTO {TABLE} SELECT g, 'row' || g, g * 1.5, g / 7.0, 0 "
                    f"FROM generate_series(1, {ROWS}) g"
                )
            )
            await connection.execute(text(f"ANALYZE {TABLE}"))
        yield engine
    finally:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
        await engine.dispose()
        shutil.rmtree(WAREHOUSE / "gantry", ignore_errors=True)


async def manifest_of(engine: AsyncEngine) -> DatasetManifest:
    return {m.name: m for m in await PostgresSourceAdapter(engine).discover()}[TABLE]


BEAM_JOB = f'''
import os, apache_beam as beam
from apache_beam.transforms.managed import ICEBERG, Write
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.options.pipeline_options import PipelineOptions

with beam.Pipeline(options=PipelineOptions(["--runner=DirectRunner"])) as p:
    (p | ReadFromJdbc(
            table_name="iceberg_probe", driver_class_name="org.postgresql.Driver",
            jdbc_url=os.environ["SRC"], username="gantry", password="gantry",
            query='SELECT "id","label","amount","ratio","source_lsn" FROM {TABLE}')
       | Write(ICEBERG, config={{
            "table": "{ICEBERG_TABLE}", "catalog_name": "local",
            "catalog_properties": {{"type": "hadoop", "warehouse": "file://{WAREHOUSE}"}}}}))
print("BEAM OK")
'''


def run_in(image: str, script: str, name: str, *, network: str | None = None) -> str:
    path = WAREHOUSE / name
    path.write_text(script)
    argv = ["docker", "run", "--rm"]
    if network:
        argv += ["--network", network, "-e", "SRC=jdbc:postgresql://gantry-pg-source:5432/gantry"]
    argv += ["-v", f"{WAREHOUSE}:{WAREHOUSE}", image, "python", str(path)]
    done = subprocess.run(argv, capture_output=True, text=True, check=False)
    assert done.returncode == 0, f"{image} failed:\n{done.stderr[-2000:]}"
    return done.stdout


async def test_a_movement_lands_in_iceberg_and_verifies(source: AsyncEngine) -> None:
    """The reach payoff, and the verification that makes it worth anything.

    The two checksums are computed by entirely different code over entirely
    different storage — one by PostgreSQL over its own heap, one by a job over
    Parquet — and must agree exactly.
    """
    manifest = await manifest_of(source)

    assert "BEAM OK" in run_in(BEAM_IMAGE, BEAM_JOB, "move.py", network=NETWORK)

    output = run_in(
        VERIFY_IMAGE,
        checksum_script(manifest, warehouse=str(WAREHOUSE), table=ICEBERG_TABLE),
        "verify.py",
    )
    in_iceberg = parse_checksum(output)
    in_postgres = await compute_checksum(source, manifest, TABLE, predicate="TRUE", params={})

    assert in_iceberg.rows == ROWS
    assert in_iceberg == in_postgres, (
        f"source {in_postgres.describe()} != target {in_iceberg.describe()}"
    )


async def test_verification_notices_when_iceberg_holds_something_else(
    source: AsyncEngine,
) -> None:
    """A checksum that always agrees proves nothing.

    The source is changed after the move, so the two sides genuinely differ and
    the comparison has to say so.
    """
    manifest = await manifest_of(source)
    assert "BEAM OK" in run_in(BEAM_IMAGE, BEAM_JOB, "move.py", network=NETWORK)

    async with transaction(source) as connection:
        await connection.execute(text(f"UPDATE {TABLE} SET label = 'changed' WHERE id = 7"))

    in_iceberg = parse_checksum(
        run_in(
            VERIFY_IMAGE,
            checksum_script(manifest, warehouse=str(WAREHOUSE), table=ICEBERG_TABLE),
            "verify.py",
        )
    )
    in_postgres = await compute_checksum(source, manifest, TABLE, predicate="TRUE", params={})
    assert in_iceberg != in_postgres, "a one-row difference went unnoticed"


async def test_a_schema_iceberg_cannot_hold_fails_before_anything_moves(
    source: AsyncEngine,
) -> None:
    """Prepare is where this belongs. Row forty million is not."""
    async with transaction(source) as connection:
        await connection.execute(text(f"ALTER TABLE {TABLE} ADD COLUMN payload jsonb"))

    manifest = await manifest_of(source)
    with pytest.raises(UnsupportedTypeError, match="payload"):
        iceberg_schema(manifest)


def _job(name: str, body: str, image: str, *, network: str | None = None) -> Job:
    """A job shaped as the generators produce them, mounting the warehouse.

    The warehouse is mounted at the same path inside as out, because Iceberg
    metadata records absolute locations.
    """
    return Job(
        operation="iceberg-probe",
        kind=JobKind.SQL,
        unit=name,
        body=body,
        packaging=ContainerPackaging(
            image=image,
            network=network,
            secrets=("SRC",) if network else (),
            mounts=((str(WAREHOUSE), str(WAREHOUSE)),),
            interpreter=("python", "-c"),
        ),
        generated_at=datetime.now(UTC),
    )


async def test_a_replayed_group_does_not_duplicate_rows(source: AsyncEngine) -> None:
    """The Day 5 fallout, fixed.

    Iceberg's write is an append, so running the move twice adds the rows twice
    — measured at 200 rows becoming 400. Gantry replays whenever a worker dies
    between committing and checkpointing, so an appending target would turn the
    recovery path into the corruption path.

    `ensure_group` verifies first and moves only when the target does not
    already hold the group. This runs the whole thing twice and asserts the
    second pass did no work and changed nothing.
    """
    manifest = await manifest_of(source)
    expected = await compute_checksum(source, manifest, TABLE, predicate="TRUE", params={})
    runner = DockerRunner(secrets={"SRC": "jdbc:postgresql://gantry-pg-source:5432/gantry"})
    verify_body = checksum_script(manifest, warehouse=str(WAREHOUSE), table=ICEBERG_TABLE)

    async def once() -> GroupOutcome:
        return await ensure_group(
            runner,
            verify=_job("verify", verify_body, VERIFY_IMAGE),
            move=_job("move", BEAM_JOB, BEAM_IMAGE, network=NETWORK),
            expected=expected,
        )

    first = await once()
    assert first.moved, "the first pass must actually move the data"
    assert first.checksum == expected

    second = await once()
    assert not second.moved, "the replay must skip the move entirely"
    assert second.checksum == expected
    assert second.checksum.rows == ROWS, "the rows were appended twice"
