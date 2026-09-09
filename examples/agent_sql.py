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

    Everything restrictive is decided here, once. `read_only=True` is enforced
    by a PostgreSQL read-only transaction as well as by classification, so a
    statement that slips past the parser still cannot write.
    """
    query = db.query(
        read_only=True,
        schemas=("analytics",),
        denied_tables=("analytics.customer_pii",),
        max_rows=200,
        timeout=15,
    )
    return query.tool()


# --------------------------------------------------------------- the example


async def main() -> int:
    db = connect()
    print(f"connected: provider={db.provider} dialect={db.dialect}\n")

    # 1. What is there? An agent needs the schema before it can write SQL, and
    #    this is metadata only — no table contents are read.
    schema = await db.describe()
    for table in schema.tables:
        if table.schema != "analytics":
            continue
        columns = ", ".join(f"{c.name} {c.type}" for c in table.columns)
        print(f"  {table.schema}.{table.name}  ({columns})")
    print()

    tool = build_tool(db)
    print(f"tool: {tool.name}")
    print(f"agent may pass: {sorted(tool.input_schema['properties'])}\n")

    # 2. The agent asks a question. This is the only thing it controls.
    answer = await tool.invoke(
        sql=(
            "SELECT customer_id, count(*) AS payments, sum(amount) AS total "
            "FROM analytics.payments GROUP BY 1 ORDER BY total DESC"
        )
    )
    print(f"query -> {answer.status.value}")
    if answer.inline is not None:
        print(f"  columns: {answer.inline.columns}")
        for row in answer.inline.rows[:3]:
            print(f"  {row}")
        if answer.inline.truncated:
            print("  (truncated by policy — the bound is reported, not hidden)")
    print()

    # 3. And the refusals, which are the reason to use this at all. Each is
    #    refused before the database is touched.
    refusals = {
        "writes": "DELETE FROM analytics.payments WHERE amount < 100",
        "a denied table": "SELECT * FROM analytics.customer_pii",
        "another schema": "SELECT * FROM public.internal_ledger",
        "stacked statements": "SELECT 1; DROP TABLE analytics.payments",
        "sql it cannot parse": "SELEC 1",
    }
    for label, sql in refusals.items():
        outcome = await tool.invoke(sql=sql)
        reason = outcome.failure.message if outcome.failure else "(no reason given)"
        print(f"  {label:20s} {outcome.status.value:9s} {reason}")

    print(
        "\nNote the last one: unparseable SQL is classified UNKNOWN and refused "
        "rather than tried.\nA parser that guesses is a parser that eventually "
        "guesses wrong about a DELETE."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
