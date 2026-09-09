# SPDX-License-Identifier: Apache-2.0
"""An agent builds a revenue rollup, and it gets checked before anyone uses it.

**The situation.** There is an `analytics.orders` table with 200,000 rows in
the warehouse. Somebody — an analyst, a scheduled agent, a chat request — wants
revenue broken down by region, written to a table the dashboard reads. The SQL
is written by a model. The table is read by people who will act on it.

That is the whole problem this library exists for: the SQL is not trusted, and
the result is.

    docker compose -f examples/flink/docker-compose.yml up -d
    python examples/warehouse_rollup.py

**What the operator decides** (this file): which tables may be read, which table
may be written, what must be true of the result, and how long it may take.

**What the agent decides**: the SQL, and nothing else.

Four things happen below, and three of them are refusals — which is roughly the
ratio you should expect in production.
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
ROLLUP = "reporting.revenue_by_region"
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
        submission_timeout=180,
        request_timeout=180,
    )

    # The contract. Everything restrictive is decided here, once, by code that
    # holds the credentials — not by the model that writes the query.
    rollup = batch.job(
        inputs=[ORDERS],
        outputs=[ROLLUP],
        checks=[
            gantry.verify.output_exists(),
            # Three regions are expected. A rollup with none is a broken query;
            # a rollup with thousands means the grouping key is wrong.
            gantry.verify.row_count(min=1, max=10),
        ],
        timeout=300,
    )

    print("1. the query an agent proposed")
    result = await rollup(
        f"INSERT INTO {q(ROLLUP)} "
        f"SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue "
        f"FROM {q(ORDERS)} WHERE status = 'paid' GROUP BY region"
    )
    print(f"   {result.status.value}  ->  {result.uri}")
    for check in result.verification.checks if result.verification else ():
        print(f"     {check.name:16s} ok={check.ok!s:6s} actual={check.actual}")

    print("\n2. the same query, but grouped by the wrong column")
    print("   Valid SQL. The engine will run it happily. It produces 5,000 rows")
    print("   in a table the dashboard expects to hold three.")
    wrong = await rollup(
        f"INSERT INTO {q(ROLLUP)} "
        f"SELECT CAST(customer_id AS STRING), COUNT(*), SUM(amount) "
        f"FROM {q(ORDERS)} GROUP BY customer_id"
    )
    print(f"   {wrong.status.value}")
    for check in wrong.verification.checks if wrong.verification else ():
        print(f"     {check.name:16s} ok={check.ok!s:6s} actual={check.actual}")

    print("\n3. a query that writes somewhere it was not asked to")
    stray = await rollup(
        f"INSERT INTO {q(REPLICA)} SELECT order_id, customer_id, region, amount FROM {q(ORDERS)}"
    )
    print(f"   {stray.status.value}: {stray.failure.message if stray.failure else ''}")

    print("\n4. a query the planner refuses")
    broken = await rollup(
        f"INSERT INTO {q(ROLLUP)} SELECT region, COUNT(*), SUM(no_such_column) "
        f"FROM {q(ORDERS)} GROUP BY region"
    )
    print(f"   {broken.status.value}: {(broken.failure.message if broken.failure else '')[:90]}")

    print(
        "\nNote what separates 1 from 2. Both are valid SQL and both ran to\n"
        "completion on a real cluster. Only one produced a table worth reading,\n"
        "and no exit code could have told you which."
    )
    print(
        "\nAnd note the honest limit of 2: the job had already written those\n"
        "5,003 rows by the time the check ran. Verification tells you the table\n"
        "is not fit to publish; it does not un-write it. Point the job at a\n"
        "staging table and promote it only on a passing check if that matters —\n"
        "the check is the gate, not the guard."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
