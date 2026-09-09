# SPDX-License-Identifier: Apache-2.0
"""A long-running job, and the different question it forces you to ask.

**The situation.** A job on the cluster is doing real work — a wide join over
the orders table, feeding the reporting database. It will run for minutes. There
is no exit code to check yet, and "did it succeed?" is the wrong question. The
right one is **"is it still healthy?"**, and that is a question about restarts
and progress rather than about a return value.

    docker compose -f examples/flink/docker-compose.yml up -d
    psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
    python examples/streaming_flink.py

**When you do not want this.** If the job finishes quickly, use `gantry.batch`
and check its output — `batch_flink.py`. Health checks are for work that
outlives your attention.

**One honest limit.** A genuinely unbounded source — Kafka, CDC — needs its
table definition to survive between sessions, and Flink's SQL Gateway keeps
tables per session unless you run a metastore. Gantry's boundary is one INSERT
over tables that already exist, so an unbounded demo would need a Hive metastore
alongside the cluster. That is a lot of moving parts to show a health check, so
this example uses a long-running bounded job instead. Everything below behaves
identically against a Kafka source; only the source changes.
"""

from __future__ import annotations

import asyncio
import os

import gantry

GATEWAY = os.environ.get("GANTRY_FLINK_GATEWAY", "http://localhost:8083")
JOBMANAGER = os.environ.get("GANTRY_FLINK_JOBMANAGER", "http://localhost:8081")
CATALOG = os.environ.get("GANTRY_FLINK_CATALOG", "pg")
DATABASE = os.environ.get("GANTRY_FLINK_DATABASE", "gantry")

SOURCE = "analytics.orders"
SINK = "reporting.orders_replica"

# Every order joined to every other order from the same customer: about eight
# million pairs. Enough work that the job is still going when you look at it,
# which is the whole premise.
HEAVY = (
    f"INSERT INTO `{SINK}` "
    f"SELECT o.order_id, o.customer_id, o.region, o.amount "
    f"FROM `{SOURCE}` o JOIN `{SOURCE}` p ON o.customer_id = p.customer_id "
    f"WHERE p.status = 'refunded'"
)


async def main() -> int:
    stream = gantry.stream.connect(
        "flink",
        endpoint=GATEWAY,
        jobmanager_endpoint=JOBMANAGER,
        default_catalog=CATALOG,
        default_database=DATABASE,
        submission_timeout=180,
        request_timeout=180,
    )

    job = stream.job(
        inputs=[SOURCE],
        outputs=[SINK],
        checks=[
            # Still running, and not quietly thrashing. A job in RUNNING that
            # has restarted forty times is not healthy, and no job state says so.
            gantry.verify.running(),
            gantry.verify.restart_count(max=3),
        ],
        # The bound on how long to wait for the contract to be met. Flink
        # registers a job's metrics a moment after it reaches RUNNING, so
        # "healthy" is not knowable on the first look.
        timeout=120,
    )

    print("submitting a job that will run for a while")
    result = await job(HEAVY)
    print(f"  {result.status.value}")
    for check in result.verification.checks if result.verification else ():
        print(f"    {check.name:16s} ok={check.ok!s:6s} actual={check.actual}")

    if result.handle is None:
        print(f"  {result.failure.message[:120] if result.failure else ''}")
        return 1

    print(f"\n  job id: {result.handle.native_id}")
    print("  This handle outlives the process. A supervisor, a dashboard, or a")
    print("  person on call can poll it without holding anything open.")

    # Ask again, the way a monitor would.
    health = await job.health(result.handle)
    print(f"\n  healthy now: {health.healthy}")
    print(f"  restarts:    {health.metrics.restart_count}")

    # And end it, because this one exists only to be looked at.
    ended = await job.cancel(result.handle)
    print(f"  cancelled -> {ended.state.value}")

    print(
        "\nWhat this buys you: the job reached a state you declared acceptable\n"
        "before anything downstream was told it had started. `DONE` would have\n"
        "told you the job ended. Nothing would have told you it was well."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
