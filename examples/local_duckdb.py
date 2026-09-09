# SPDX-License-Identifier: Apache-2.0
"""Analysis on a local file, with nothing to install and nothing to connect to.

**When you want this:** you have a Parquet/CSV/DuckDB file and you want an
agent to answer questions about it. No server, no credentials, no network. It is
also the cheapest way to see what the governance layer does before pointing it
at a real database — everything here behaves the same way against PostgreSQL.

    pip install "gantry[duckdb]"
    python examples/local_duckdb.py

The point of this example is the *read-only* boundary. Gantry does not merely
promise not to write; it refuses to admit a read-only query unless the adapter
can enforce read-only in the engine. For DuckDB that means opening the file
read-only, and a connection that did not is turned away rather than trusted.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import duckdb
import gantry


def build(path: Path) -> None:
    """A small dataset to ask questions about."""
    connection = duckdb.connect(str(path))
    connection.execute("CREATE SCHEMA analytics")
    connection.execute(
        "CREATE TABLE analytics.payments AS "
        "SELECT i AS id, i % 7 AS customer_id, i * 1.5 AS amount "
        "FROM range(1, 501) t(i)"
    )
    connection.close()


async def main() -> int:
    workspace = Path(tempfile.mkdtemp())
    path = workspace / "warehouse.duckdb"
    build(path)

    # `read_only=True` here is not a hint. Without it the adapter cannot hold a
    # read-only session open, and Gantry refuses every read-only query rather
    # than running one it cannot vouch for.
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True)
    query = db.query(read_only=True, schemas=("analytics",), max_rows=10, timeout=15)

    result = await query(
        "SELECT customer_id, count(*) AS payments, sum(amount) AS total "
        "FROM analytics.payments GROUP BY 1 ORDER BY total DESC"
    )
    print(f"query -> {result.status.value}")
    if result.inline is not None:
        print(f"  {result.inline.columns}")
        for row in result.inline.rows[:3]:
            print(f"  {row}")

    tool = query.tool()
    print(f"\nagent tool: {tool.name}, inputs {sorted(tool.input_schema['properties'])}")
    refused = await tool.invoke(sql="DROP TABLE analytics.payments")
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
