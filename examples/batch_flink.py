# SPDX-License-Identifier: Apache-2.0
"""A nightly replication job on a cluster, verified before anyone depends on it.

**The situation.** The reporting database needs a copy of yesterday's orders.
It runs on Flink because that is what the platform team operates, it runs
unattended at 3am, and the first person to notice a bad run is whoever opens the
dashboard at 9.

`DONE` from a batch job means the job finished. It does not mean the rows are
there — a filter that matched nothing finishes just as cleanly as one that
matched everything. So the job's exit is not the signal; the destination is.

    docker compose -f examples/flink/docker-compose.yml up -d
    psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
    python examples/batch_flink.py

For the agent-facing version of the same machinery — where a model writes the
SQL and three of its four attempts are refused — see `warehouse_rollup.py`.
"""

from __future__ import annotations

import asyncio
import os

import gantry

GATEWAY = os.environ.get("GANTRY_FLINK_GATEWAY", "http://localhost:18084")
JOBMANAGER = os.environ.get("GANTRY_FLINK_JOBMANAGER", "http://localhost:18081")
CATALOG = os.environ.get("GANTRY_FLINK_CATALOG", "pg")
DATABASE = os.environ.get("GANTRY_FLINK_DATABASE", "gantry")

ORDERS = "analytics.orders"
REPLICA = "reporting.orders_replica"


def q(name: str) -> str:
    """A Flink identifier for a JDBC-catalog table.

    The catalog exposes a PostgreSQL table schema-qualified, as one identifier
    that contains a dot: `analytics.orders`. Quoting it as a single part is not
    a style choice — split into `analytics`.`orders` it names a database and a
    table that do not exist. The session's default catalog and database supply
    the rest.
    """
    return f"`{name}`"


async def main() -> int:
    batch = gantry.batch.connect(
        "flink",
        endpoint=GATEWAY,
        jobmanager_endpoint=JOBMANAGER,
        default_catalog=CATALOG,
        default_database=DATABASE,
        # A batch job takes longer to deploy than the 30-second default allows.
        submission_timeout=180,
        request_timeout=180,
    )

    replicate = batch.job(
        inputs=[ORDERS],
        outputs=[REPLICA],
        checks=[
            gantry.verify.output_exists(),
            # The bound that makes this worth running unattended. An empty
            # replica and a full one both finish; only one is a good night.
            gantry.verify.row_count(min=100_000),
        ],
        timeout=600,
    )

    print("replicating paid orders to the reporting database")
    result = await replicate(
        f"INSERT INTO {q(REPLICA)} "
        f"SELECT order_id, customer_id, region, amount "
        f"FROM {q(ORDERS)} WHERE status = 'paid'"
    )
    print(f"  {result.status.value}  ->  {result.uri}")
    for check in result.verification.checks if result.verification else ():
        print(f"    {check.name:16s} ok={check.ok!s:6s} actual={check.actual}")

    if result.failure is not None:
        print(f"  {result.failure.message[:120]}")
        return 1

    print(
        "\nThe row-count bound is the whole point of running this through\n"
        "Gantry rather than a cron entry. A WHERE clause that stops matching —\n"
        "because a status value was renamed upstream, say — produces a job that\n"
        "succeeds every night and a dashboard that quietly empties."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
