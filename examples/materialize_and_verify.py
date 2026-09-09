# SPDX-License-Identifier: Apache-2.0
"""Letting an agent build a table, and checking it before you believe it.

**When you want this:** an agent should produce a derived table — a rollup, a
feature table, a scratch result someone will act on — and you need something
better than "the SQL ran without error" before anyone uses it.

    pip install "gantry[duckdb]"
    python examples/materialize_and_verify.py

Two different questions get two different answers here, and keeping them apart
is the whole idea:

- **Did the statement run?** The engine answers that. A `CREATE TABLE AS` that
  matched nothing succeeds; so does one that produced a single null row.
- **Is the result usable?** Gantry answers that, by checking the destination
  afterwards against what you said you expected.

The policy is also narrower than "can write". It names which tables may be read
and which may be created, so an agent that writes valid SQL against the wrong
table is refused rather than obeyed.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import duckdb
import gantry
from gantry import verify


def build(path: Path) -> None:
    connection = duckdb.connect(str(path))
    connection.execute("CREATE SCHEMA analytics")
    connection.execute("CREATE SCHEMA agent_scratch")
    connection.execute(
        "CREATE TABLE analytics.payments AS "
        "SELECT i AS id, i % 7 AS customer_id, i * 1.5 AS amount "
        "FROM range(1, 501) t(i)"
    )
    connection.close()


async def main() -> int:
    path = Path(tempfile.mkdtemp()) / "warehouse.duckdb"
    build(path)
    db = gantry.sql.connect("duckdb", path=str(path))

    # What may be read, what may be created, and what must be true afterwards.
    # All decided here, by you — none of it reachable from the agent's tool.
    materialize = db.materialize(
        sources=("analytics.payments",),
        destinations=("agent_scratch.customer_totals",),
        verify=(
            verify.destination_exists(),
            verify.row_count(min=1),
            verify.required_columns(["customer_id", "total"]),
        ),
    )

    good = await materialize(
        "CREATE TABLE agent_scratch.customer_totals AS "
        "SELECT customer_id, sum(amount) AS total "
        "FROM analytics.payments GROUP BY customer_id"
    )
    print(f"build the rollup -> {good.status.value}")
    for check in good.verification.checks:
        print(f"  {check.name:20s} ok={check.ok}  actual={check.actual}")

    # A statement that runs perfectly well and produces nothing useful. The
    # engine is happy; the row-count check is not, and the result carries that
    # rather than a success anyone would act on.
    empty = db.materialize(
        sources=("analytics.payments",),
        destinations=("agent_scratch.no_rows",),
        verify=(verify.destination_exists(), verify.row_count(min=1)),
    )
    thin = await empty(
        "CREATE TABLE agent_scratch.no_rows AS "
        "SELECT customer_id, sum(amount) AS total FROM analytics.payments "
        "WHERE customer_id = -1 GROUP BY customer_id"
    )
    print(f"\na query that matched nothing -> {thin.status.value}")
    for check in thin.verification.checks:
        print(f"  {check.name:20s} ok={check.ok}  actual={check.actual}")
    print("  The SQL was valid and the engine succeeded. The table is empty.")
    print("  That difference is what verification is for.")

    # And the policy boundary: correct SQL, wrong destination.
    stray = await materialize(
        "CREATE TABLE analytics.payments_v2 AS SELECT * FROM analytics.payments"
    )
    print(f"\nwriting outside the declared destination -> {stray.status.value}")
    print(f"  {stray.failure.message}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
