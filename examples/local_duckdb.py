# SPDX-License-Identifier: Apache-2.0
"""Analysis on an exported file, with nothing to install and nothing to connect to.

**The situation.** Someone has dropped a Parquet export of the orders table in
a directory — a nightly dump, a data-request extract, a file pulled from S3 —
and an agent should answer questions about it. No server, no credentials, no
network.

    pip install "gantry[duckdb]"
    python examples/local_duckdb.py

It is also the cheapest way to see what the governance layer does before
pointing it at a warehouse: everything here behaves the same against
PostgreSQL, and the file is built for you.

The point of this example is the **read-only boundary**. Gantry does not merely
promise not to write; it refuses to admit a read-only query unless the adapter
can enforce read-only in the engine. For DuckDB that means opening the database
read-only, and a connection that did not is turned away rather than trusted.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import duckdb
import gantry


def build(workspace: Path) -> Path:
    """Write a Parquet export, then a DuckDB database that reads it.

    This is the shape the example is about: the data arrives as files, and the
    engine is something you open over them rather than a service you connect
    to.
    """
    export = workspace / "orders.parquet"
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE orders AS
        SELECT i                                              AS order_id,
               (i % 5000) + 1                                 AS customer_id,
               ['emea','amer','apac'][1 + (i % 3)]            AS region,
               ['web','mobile','partner'][1 + (i % 3)]        AS channel,
               CASE WHEN i % 97 = 0 THEN 'refunded'
                    WHEN i % 89 = 0 THEN 'failed'
                    ELSE 'paid' END                           AS status,
               ((i * 37) % 50000) / 100.0 + 1                 AS amount
        FROM range(1, 200001) t(i)
        """
    )
    connection.execute(f"COPY orders TO '{export}' (FORMAT parquet)")
    connection.close()

    path = workspace / "warehouse.duckdb"
    warehouse = duckdb.connect(str(path))
    warehouse.execute("CREATE SCHEMA analytics")
    warehouse.execute(f"CREATE TABLE analytics.orders AS SELECT * FROM '{export}'")
    warehouse.close()
    return path


async def main() -> int:
    workspace = Path(tempfile.mkdtemp())
    path = build(workspace)
    print(f"exported 200,000 orders to {workspace.name}/orders.parquet\n")

    # `read_only=True` here is not a hint. Without it the adapter cannot hold a
    # read-only session open, and Gantry refuses every read-only query rather
    # than running one it cannot vouch for.
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True)
    query = db.query(read_only=True, schemas=("analytics",), max_rows=10, timeout=15)

    for question, sql in {
        "revenue by region, paid only": (
            "SELECT region, count(*) AS orders, round(sum(amount), 2) AS revenue "
            "FROM analytics.orders WHERE status = 'paid' "
            "GROUP BY region ORDER BY revenue DESC"
        ),
        "refund rate by channel": (
            "SELECT channel, "
            "  round(100.0 * count(*) FILTER (WHERE status = 'refunded') / count(*), 3) AS pct "
            "FROM analytics.orders GROUP BY channel ORDER BY pct DESC"
        ),
    }.items():
        result = await query(sql)
        print(f"Q: {question}  [{result.status.value}]")
        if result.inline is not None:
            for row in result.inline.rows[:3]:
                print(f"   {row}")
        print()

    tool = query.tool()
    print(f"agent tool: {tool.name}, inputs {sorted(tool.input_schema['properties'])}")
    refused = await tool.invoke(sql="DROP TABLE analytics.orders")
    print(f"  DROP -> {refused.status.value}: {refused.failure.message}")

    # And the case worth understanding: a *writable* connection cannot serve
    # read-only queries, because the guarantee is the engine's and not a
    # promise made here.
    writable = gantry.sql.connect("duckdb", path=str(workspace / "other.duckdb"))
    unenforceable = await writable.query(read_only=True)("SELECT 1")
    print(f"\nsame query, writable connection -> {unenforceable.status.value}")
    print(f"  {unenforceable.failure.message}")
    print("  Read-only is enforced by the engine, so an engine that cannot")
    print("  enforce it does not get to be trusted with it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
