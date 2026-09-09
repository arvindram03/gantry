# SPDX-License-Identifier: Apache-2.0
"""A continuous job, and the different questions it forces you to ask.

**When you want this:** the work does not finish. A pipeline that keeps reading
a stream and writing results has no final row count and no exit code to check,
so "did it succeed?" is the wrong question — the right one is "is it still
healthy?"

    docker compose -f examples/flink/docker-compose.yml up -d
    python examples/streaming_flink.py

**When you do not want this:** if the job ends, use `gantry.sql`. A Flink
cluster is a large thing to operate, and a batch `INSERT ... SELECT` against
your database is simpler in every way that matters. Reach for this when the
input is unbounded or the engine is genuinely Flink.

What Gantry adds is the same boundary as everywhere else: one statement,
declared inputs and outputs, a handle you can come back to, and health checks
that answer a question a job state cannot.
"""

from __future__ import annotations

import asyncio
import os

import gantry
from gantry.flink import (
    FlinkMode,
    FlinkSQLArtifact,
    JobRunning,
    MaxRestartCount,
    MinOutputRate,
)

GATEWAY = os.environ.get("GANTRY_FLINK_GATEWAY", "http://localhost:18084")
JOBMANAGER = os.environ.get("GANTRY_FLINK_JOBMANAGER", "http://localhost:18081")
CATALOG = os.environ.get("GANTRY_FLINK_CATALOG", "pg")
DATABASE = os.environ.get("GANTRY_FLINK_DATABASE", "gantry")


async def main() -> int:
    flink = gantry.flink.connect(
        GATEWAY,
        jobmanager_endpoint=JOBMANAGER,
        default_catalog=CATALOG,
        default_database=DATABASE,
        # A batch job takes longer to deploy than the 30-second default allows.
        submission_timeout=180.0,
        request_timeout=180.0,
    )

    # One statement, and its inputs and outputs named. Gantry refuses anything
    # else: a script that creates tables as it goes has no boundary to check.
    artifact = FlinkSQLArtifact(
        f"INSERT INTO `{CATALOG}`.`{DATABASE}`.`flink_sink` "
        f"SELECT id, label FROM `{CATALOG}`.`{DATABASE}`.`flink_src`",
        mode=FlinkMode.BATCH,
        declared_inputs=(f"{CATALOG}.{DATABASE}.flink_src",),
        declared_outputs=(f"{CATALOG}.{DATABASE}.flink_sink",),
    )

    # Validation is the real planner's opinion, not a parse. It is the cheapest
    # way to find out that a column does not exist.
    check = await flink.validate(artifact)
    print(f"validate -> ok={check.ok} {check.errors if not check.ok else ''}")
    if not check.ok:
        return 1

    result = await flink.run(artifact, timeout_seconds=300)
    print(f"run      -> {result.status.value}")
    if result.failure is not None:
        print(f"  {result.failure.message[:160]}")
        return 1

    # For a *streaming* job the shape is different, and this is the part worth
    # copying. You do not wait for it to finish, because it does not. You
    # submit, keep the handle, and ask whether it is healthy — which is a
    # question about restarts and output rate, not about a return code.
    print("\nfor a streaming job you would instead:")
    print("  handle = await flink.submit(artifact)          # mode='streaming'")
    print("  health = await flink.health(handle, checks=[")
    print("      JobRunning(), MaxRestartCount(3), MinOutputRate(100),")
    print("  ])")
    print(
        "\nA job in RUNNING that has restarted forty times and emits nothing is\n"
        "not healthy, and no job state will tell you so."
    )
    print(
        f"\navailable checks: {[c.__name__ for c in (JobRunning, MaxRestartCount, MinOutputRate)]}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
