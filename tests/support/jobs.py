# SPDX-License-Identifier: Apache-2.0
"""Running one partition as a job, for tests that used to call the relay.

The guarantees these tests cover — a partition moves, a replay changes nothing,
the bulk path and the row path agree — did not change when the executor stopped
relaying bytes. Only the thing that performs the move did, so the tests point
here instead.
"""

from __future__ import annotations

import os

from gantry.core.commit import CommitResult
from gantry.core.dataset import DatasetManifest
from gantry.jobs.execute import run_to_completion
from gantry.jobs.packaging import sql_client_packaging
from gantry.jobs.runners import DockerRunner
from gantry.movement.partitioning import Partition
from gantry.movement.sqljob import SOURCE_DSN, TARGET_DSN, compile_snapshot_job, parse_commit

NETWORK = os.environ.get("GANTRY_JOB_NETWORK", "gantry-dev_default")
INSIDE_SOURCE = os.environ.get(
    "GANTRY_JOB_SOURCE_DSN", "postgresql://gantry:gantry@gantry-pg-source:5432/gantry"
)
INSIDE_TARGET = os.environ.get(
    "GANTRY_JOB_TARGET_DSN", "postgresql://gantry:gantry@gantry-pg-target:5432/gantry"
)


async def move_partition(
    manifest: DatasetManifest,
    partition: Partition,
    *,
    target: str,
    operation: str = "test-movement",
    snapshot_lsn: int | None = None,
) -> CommitResult:
    """Move one partition and return what it committed."""
    job = compile_snapshot_job(
        operation,
        manifest,
        partition,
        target=target,
        snapshot_lsn=snapshot_lsn,
        packaging=sql_client_packaging(secrets=(SOURCE_DSN, TARGET_DSN), network=NETWORK),
    )
    runner = DockerRunner(secrets={SOURCE_DSN: INSIDE_SOURCE, TARGET_DSN: INSIDE_TARGET})
    return parse_commit(await run_to_completion(runner, job, poll_interval=0.1))
