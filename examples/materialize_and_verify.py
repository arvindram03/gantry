# SPDX-License-Identifier: Apache-2.0
"""An agent builds a feature table, and it is checked before a model trains on it.

**The situation.** Somebody needs per-customer features for a churn model:
order count, total spend, refund rate, days since last order. The SQL is written
by an agent. The table is read by a training job that will not notice if it is
wrong — it will just produce a worse model, next week, quietly.

    pip install "data-gantry[duckdb]"
    python examples/materialize_and_verify.py

Two different questions get two different answers, and keeping them apart is the
whole idea:

- **Did the statement run?** The engine answers that. A `CREATE TABLE AS` whose
  join matched nothing succeeds. So does one that silently dropped 90% of the
  customers.
- **Is the result usable?** Gantry answers that, by checking the destination
  against what you said you expected.

The policy is also narrower than "may write". It names which tables may be read
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

CUSTOMERS = 5_000


def build(path: Path) -> None:
    """A warehouse with orders and customers, as an export would leave it."""
    connection = duckdb.connect(str(path))
    connection.execute("CREATE SCHEMA analytics")
    connection.execute("CREATE SCHEMA feature_store")
    connection.execute(
        f"""
        CREATE TABLE analytics.customers AS
        SELECT i AS customer_id,
               ['free','team','business','enterprise'][1 + (i % 4)] AS plan
        FROM range(1, {CUSTOMERS + 1}) t(i)
        """
    )
    connection.execute(
        """
        CREATE TABLE analytics.orders AS
        SELECT i                                   AS order_id,
               -- Only the first 4,000 customers ever order. The other 1,000
               -- signed up and never bought anything, which is both realistic
               -- and the entire population a churn model cares about.
               (i % 4000) + 1                      AS customer_id,
               CASE WHEN i % 97 = 0 THEN 'refunded' ELSE 'paid' END AS status,
               ((i * 37) % 50000) / 100.0 + 1      AS amount,
               DATE '2026-01-01' + TO_DAYS(CAST(i % 90 AS INTEGER)) AS placed_on
        FROM range(1, 200001) t(i)
        """
    )
    connection.close()


FEATURES = """
CREATE TABLE feature_store.customer_features AS
SELECT c.customer_id,
       c.plan,
       count(o.order_id)                                     AS orders,
       coalesce(sum(o.amount), 0)                            AS lifetime_value,
       coalesce(avg(CASE WHEN o.status = 'refunded' THEN 1.0 ELSE 0.0 END), 0) AS refund_rate,
       max(o.placed_on)                                      AS last_order_on
FROM analytics.customers c
LEFT JOIN analytics.orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.plan
"""

# The same intent, written with an inner join. Every customer who has never
# ordered silently disappears — which is exactly the population a churn model
# most needs. It runs perfectly.
FEATURES_INNER_JOIN = FEATURES.replace("LEFT JOIN", "JOIN").replace(
    "feature_store.customer_features", "feature_store.customer_features_v2"
)


async def main() -> int:
    path = Path(tempfile.mkdtemp()) / "warehouse.duckdb"
    build(path)
    db = gantry.sql.connect("duckdb", path=str(path))

    # The contract: what may be read, what may be created, and what must be
    # true afterwards. Decided here, by you — none of it reachable from the
    # agent's tool.
    checks = (
        verify.destination_exists(),
        # One row per customer. Not "some rows" — the number that makes this
        # table correct rather than merely present.
        verify.row_count(min=CUSTOMERS, max=CUSTOMERS),
        verify.required_columns(["customer_id", "plan", "orders", "lifetime_value", "refund_rate"]),
    )

    materialize = db.materialize(
        sources=("analytics.customers", "analytics.orders"),
        destinations=("feature_store.customer_features",),
        verify=checks,
    )
    good = await materialize(FEATURES)
    print(f"1. features, built with a LEFT JOIN -> {good.status.value}")
    for check in good.verification.checks:
        print(f"     {check.name:20s} ok={check.ok!s:6s} actual={check.actual}")

    # The same query with an inner join. Valid SQL, engine succeeds, and every
    # customer with no orders is gone.
    strict = db.materialize(
        sources=("analytics.customers", "analytics.orders"),
        destinations=("feature_store.customer_features_v2",),
        verify=checks,
    )
    thin = await strict(FEATURES_INNER_JOIN)
    print(f"\n2. the same features, built with an inner join -> {thin.status.value}")
    for check in thin.verification.checks:
        print(f"     {check.name:20s} ok={check.ok!s:6s} actual={check.actual}")
    print("   The engine succeeded. Customers who never ordered are missing —")
    print("   the exact population a churn model is trying to find.")

    # And the policy boundary: correct SQL, wrong destination.
    stray = await materialize("CREATE TABLE analytics.orders_v2 AS SELECT * FROM analytics.orders")
    print(f"\n3. writing outside the declared destination -> {stray.status.value}")
    print(f"     {stray.failure.message}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
