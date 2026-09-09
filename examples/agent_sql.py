# SPDX-License-Identifier: Apache-2.0
"""A governed SQL tool for an agent, against PostgreSQL.

Runs against local PostgreSQL, Neon, or Supabase — the same code, a different
provider name and URL. The provider preset decides the dialect and driver;
nothing else in this file changes.

    python examples/agent_sql.py                       # local, from GANTRY_DATABASE_URL
    GANTRY_PROVIDER=neon     python examples/agent_sql.py
    GANTRY_PROVIDER=supabase python examples/agent_sql.py

Needs `pip install "gantry[postgres]"`.

The shape worth copying is the split between two audiences:

- **You** configure the connection and the policy. That is application code,
  and it holds the credential.
- **The agent** gets `query.tool()` — one input, `sql`. It cannot widen the
  policy, reach another schema, or see the connection string, because none of
  those are arguments it can pass.

That is the whole idea. The agent writes SQL; it does not decide what SQL is
allowed to do.
"""

from __future__ import annotations

import asyncio
import os

import gantry

# ---------------------------------------------------------------- connecting

LOCAL_URL = "postgresql://gantry:gantry@localhost:15432/gantry"


def connect() -> gantry.sql.SQLConnection:
    """One connection, three hosting options.

    `postgres`, `neon` and `supabase` are separate provider presets that share
    the PostgreSQL dialect and driver. They are named separately so a target
    can be identified in logs and so a preset can grow provider-specific
    behaviour without changing calling code.
    """
    provider = os.environ.get("GANTRY_PROVIDER", "postgres")
    url = os.environ.get("GANTRY_DATABASE_URL", LOCAL_URL)

    if provider == "neon":
        # Neon terminates TLS and requires it. A compute that has scaled to
        # zero takes a few seconds to wake, so the first connection is slow
        # rather than broken — give it room before deciding it failed.
        return gantry.sql.connect("neon", url=url, ssl="require", timeout=30)

    if provider == "supabase":
        # TLS on every endpoint. Nothing else is needed: on the transaction
        # pooler (`:6543`) the adapter turns off asyncpg's statement cache for
        # you, because a transaction-pooled backend is often not the one that
        # prepared the statement. Pass `statement_cache_size` yourself if you
        # have measured your own deployment and disagree.
        return gantry.sql.connect("supabase", url=url, ssl="require", timeout=30)

    return gantry.sql.connect("postgres", url=url)


# ------------------------------------------------------------------ the tool


def build_tool(db: gantry.sql.SQLConnection) -> object:
    """The narrow thing an agent is handed.

    Everything restrictive is decided here, once, by code that holds the
    credentials. Note what the policy is made of: a schema allow-list, a
    denied table, a row cap and a timeout. Those are the four knobs that
    actually matter in production.

    `read_only=True` is enforced by a PostgreSQL read-only transaction as well
    as by classification, so a statement that slips past the parser still
    cannot write.
    """
    query = db.query(
        read_only=True,
        # The agent may read the warehouse, and nothing else in the database.
        schemas=("analytics",),
        # Belt and braces: even if `analytics_pii` were added to the schema
        # list by mistake, this table stays out of reach.
        denied_tables=("analytics_pii.customer_contacts",),
        # An answer needs a few hundred rows. A model asking for 200,000 has
        # misunderstood the question, and a row cap is cheaper than finding
        # out downstream.
        max_rows=200,
        timeout=15,
    )
    return query.tool()


# --------------------------------------------------------------- the example

# What someone might actually ask an analytics agent, and what it writes.
QUESTIONS = {
    "revenue by region, paid orders only": (
        "SELECT region, count(*) AS orders, round(sum(amount), 2) AS revenue "
        "FROM analytics.orders WHERE status = 'paid' "
        "GROUP BY region ORDER BY revenue DESC"
    ),
    "which plans refund the most": (
        "SELECT c.plan, "
        "       count(*) FILTER (WHERE o.status = 'refunded') AS refunds, "
        "       count(*) AS orders, "
        "       round(100.0 * count(*) FILTER (WHERE o.status = 'refunded') / count(*), 2) AS pct "
        "FROM analytics.orders o "
        "JOIN analytics.customers c ON c.customer_id = o.customer_id "
        "GROUP BY c.plan ORDER BY pct DESC"
    ),
    "last 7 days by channel": (
        "SELECT channel, count(*) AS orders "
        "FROM analytics.orders "
        "WHERE placed_at >= now() - interval '7 days' "
        "GROUP BY channel ORDER BY orders DESC"
    ),
}

# The ones that must not run. Each is a plausible thing a model produces.
REFUSALS = {
    "reads the PII table": "SELECT email FROM analytics_pii.customer_contacts LIMIT 5",
    "deletes rows it was asked to count": ("DELETE FROM analytics.orders WHERE status = 'failed'"),
    "reaches outside the warehouse": "SELECT * FROM pg_catalog.pg_user",
    "smuggles a second statement": (
        "SELECT count(*) FROM analytics.orders; DROP TABLE analytics.orders"
    ),
    "is not valid SQL at all": "SELEC region FROM analytics.orders",
}


async def main() -> int:
    db = connect()
    print(f"connected: provider={db.provider} dialect={db.dialect}\n")

    # 1. What is there? An agent needs the schema before it can write SQL, and
    #    this is metadata only — no table contents are read.
    schema = await db.describe()
    for table in schema.tables:
        if table.schema not in {"analytics", "analytics_pii"}:
            continue
        columns = ", ".join(c.name for c in table.columns)
        print(f"  {table.schema}.{table.name} ({columns})")
    print("\n  Note analytics_pii is visible to `describe()` and unreadable by")
    print("  the tool. Knowing a table exists is not permission to read it.\n")

    tool = build_tool(db)
    print(f"tool: {tool.name}, agent may pass {sorted(tool.input_schema['properties'])}\n")

    # 2. Real questions.
    for question, sql in QUESTIONS.items():
        answer = await tool.invoke(sql=sql)
        print(f"Q: {question}")
        if answer.inline is None:
            print(f"   {answer.status.value}: {answer.failure.message if answer.failure else ''}")
            continue
        print(f"   {answer.inline.columns}")
        for row in answer.inline.rows[:3]:
            print(f"   {row}")
        print()

    # 3. And the refusals, which are the reason to use this at all. Each is
    #    refused before the database is touched.
    print("refused:")
    for label, sql in REFUSALS.items():
        outcome = await tool.invoke(sql=sql)
        reason = outcome.failure.message if outcome.failure else "(no reason given)"
        print(f"  {label:34s} {outcome.status.value:9s} {reason}")

    print(
        "\nThe last one is worth noticing: unparseable SQL is classified UNKNOWN\n"
        "and refused rather than tried. A parser that guesses is a parser that\n"
        "eventually guesses wrong about a DELETE."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
