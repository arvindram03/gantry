# SPDX-License-Identifier: Apache-2.0
"""An agent answering questions about a MySQL database, and building one table.

**The situation.** Support wants to ask questions of the customer database in
English, and occasionally wants a rollup left behind for a dashboard. The
database is the production one. The agent is not going to be given a MySQL
connection.

What it gets is two tools. `query_sql` reads `analytics`, one hundred rows at a
time, thirty seconds at a time, and cannot write. `materialize_sql` creates one
table in `reporting`, from `analytics`, and only if the destination does not
already exist. Both are configured here, in code the agent never sees.

    docker compose -f examples/mysql/docker-compose.yml up -d --wait
    python examples/mysql_customers.py

MySQL specifics worth knowing, all of which Gantry handles rather than exposes:
a "schema" is a database, `max_execution_time` bounds `SELECT` and not writes
(so the timeout is also enforced by killing the query), and `EXPLAIN` cannot
describe DDL (so a materialization is validated with `PREPARE`).
"""

from __future__ import annotations

import asyncio
import os

import gantry

URL = os.environ.get("GANTRY_MYSQL_URL", "mysql://gantry:gantry@127.0.0.1:3306/analytics")

SEED = (
    "CREATE DATABASE IF NOT EXISTS analytics",
    "CREATE DATABASE IF NOT EXISTS reporting",
    "DROP TABLE IF EXISTS reporting.plan_totals",
    # Dropped rather than created-if-missing: the example owns this table, and
    # an older one of a different shape would fail the insert below.
    "DROP TABLE IF EXISTS analytics.customers",
    "CREATE TABLE analytics.customers ("
    "  id INT PRIMARY KEY, plan VARCHAR(32), status VARCHAR(16), region VARCHAR(32))",
    "INSERT INTO analytics.customers VALUES"
    " (1,'free','active','emea'),(2,'pro','active','emea'),(3,'pro','active','apac'),"
    " (4,'free','churned','amer'),(5,'enterprise','active','amer')",
)


async def seed() -> None:
    """Arrange the example's data with a connection Gantry knows nothing about."""
    import aiomysql
    from gantry.sql.adapters.mysql import _connect_kwargs

    config = _connect_kwargs({"url": URL})
    config.pop("db", None)
    connection = await aiomysql.connect(**config)
    async with connection.cursor() as cursor:
        for statement in SEED:
            await cursor.execute(statement)
    connection.close()


async def main() -> int:
    await seed()
    db = gantry.sql.connect("mysql", url=URL)

    # Configured once, here. The agent gets the tools, not the connection.
    ask = db.query(schemas=["analytics"], max_rows=100, timeout=30)
    build = db.materialize(
        sources=["analytics.*"],
        destinations=["reporting.*"],
        verify=[gantry.verify.destination_exists(), gantry.verify.row_count(min=1)],
    )
    tools = [ask.tool(), build.tool()]
    print("tools handed to the agent:", [tool.name for tool in tools])
    print()

    print("1. a question it was meant to ask")
    answer = await ask.tool().invoke(
        {
            "sql": "SELECT plan, COUNT(*) AS customers FROM analytics.customers "
            "WHERE status = 'active' GROUP BY plan ORDER BY customers DESC"
        }
    )
    print(f"   {answer.status.value}")
    for row in answer.inline.rows if answer.inline else ():
        print(f"     {row[0]:12} {row[1]}")

    print()
    print("2. a write, through the read-only tool")
    refused = await ask.tool().invoke({"sql": "DELETE FROM analytics.customers"})
    print(f"   {refused.status.value}: {refused.failure.message if refused.failure else ''}")

    print()
    print("3. a table it was not granted")
    outside = await ask.tool().invoke({"sql": "SELECT * FROM mysql.user"})
    print(f"   {outside.status.value}: {outside.failure.message if outside.failure else ''}")

    print()
    print("4. the rollup it was asked for")
    built = await build.tool().invoke(
        {
            "sql": "CREATE TABLE reporting.plan_totals AS "
            "SELECT plan, COUNT(*) AS customers FROM analytics.customers GROUP BY plan"
        }
    )
    print(f"   {built.status.value} -> {built.uri}")
    for check in built.verification.checks if built.verification else ():
        print(f"     {check.name:20} ok={check.ok} actual={check.actual}")

    print()
    print("5. the same rollup again — materialization is create-only")
    repeat = await build.tool().invoke(
        {
            "sql": "CREATE TABLE reporting.plan_totals AS "
            "SELECT plan, COUNT(*) AS customers FROM analytics.customers GROUP BY plan"
        }
    )
    print(f"   {repeat.status.value}: {repeat.failure.message if repeat.failure else ''}")

    print()
    print("6. a rollup written somewhere it was not granted")
    elsewhere = await build.tool().invoke(
        {"sql": "CREATE TABLE analytics.plan_totals AS SELECT plan FROM analytics.customers"}
    )
    print(f"   {elsewhere.status.value}: {elsewhere.failure.message if elsewhere.failure else ''}")

    print()
    print("The agent wrote every statement above. Four of the six were refused,")
    print("and none of the refusals depended on the agent cooperating.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
