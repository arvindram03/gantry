# SPDX-License-Identifier: Apache-2.0
"""The tools an agent uses before and after it writes a statement.

Against a real PostgreSQL, because the claim being tested is that a schema
handed to a model omits the tables the policy denies — and a stub would agree
with whatever this code did.
"""

from __future__ import annotations

import os

import gantry
import pytest
from gantry.sql.introspection import check_tool, describe_tool, status_tool
from gantry.sql.policy import SQLPolicy

URL = os.environ.get("GANTRY_TEST_POSTGRES_URL", "postgresql://gantry:gantry@localhost:5432/gantry")


async def _reachable() -> bool:
    import asyncpg  # type: ignore[import-untyped]

    try:
        connection = await asyncpg.connect(URL, timeout=5)
    except Exception:
        return False
    await connection.close()
    return True


async def _db(policy: gantry.Policy | None = None) -> gantry.sql.SQLConnection:
    if not await _reachable():
        reason = f"no PostgreSQL at {URL}"
        if os.environ.get("GANTRY_REQUIRE_LIVE") == "1":
            pytest.fail(reason)
        pytest.skip(reason)
    return gantry.sql.connect("postgres", url=URL, policy=policy)


# --------------------------------------------------------------- describe


async def test_describe_without_a_policy_still_shows_everything() -> None:
    """The shape a person configuring access needs, unchanged."""
    db = await _db()
    schema = await db.describe()
    names = {f"{table.schema}.{table.name}" for table in schema.tables}
    assert "analytics.orders" in names
    assert "analytics_pii.customer_contacts" in names


async def test_a_denied_table_is_not_described_to_an_agent() -> None:
    """The bug this exists for.

    A denied table is usually denied because its structure is the sensitive
    part. Describing it hands over the column names, the types and the fact
    that it exists — around the deny rule rather than through it.
    """
    db = await _db()
    bounds = SQLPolicy(denied_tables=["analytics_pii.customer_contacts"])

    schema = await db.describe(policy=bounds)

    names = {f"{table.schema}.{table.name}" for table in schema.tables}
    assert "analytics.orders" in names
    assert "analytics_pii.customer_contacts" not in names


async def test_a_schema_outside_the_allow_list_is_not_described() -> None:
    db = await _db()
    schema = await db.describe(policy=SQLPolicy(allowed_schemas=["analytics"]))

    assert {table.schema for table in schema.tables} == {"analytics"}
    assert "analytics_pii" not in schema.schemas


async def test_the_reusable_policy_narrows_the_schema_too() -> None:
    """Both layers, because both layers decide a query.

    A connection whose policy allows one table should not describe the rest:
    the agent would be invited to reference what it cannot read.
    """
    policy = gantry.Policy(
        name="only-orders",
        rules=[gantry.allow.query(name="orders", sources=["analytics.orders"])],
    )
    db = await _db(policy)

    schema = await db.describe(policy=SQLPolicy())

    names = {f"{table.schema}.{table.name}" for table in schema.tables}
    assert names == {"analytics.orders"}, names


async def test_the_tool_returns_columns_an_agent_can_write_sql_against() -> None:
    db = await _db()
    tool = describe_tool(db, policy=SQLPolicy(allowed_schemas=["analytics"]))

    answer = await tool.invoke({})

    tables = {table["name"]: table for table in answer["tables"]}
    assert "analytics.orders" in tables
    assert tables["analytics.orders"]["columns"], "a schema with no columns helps nobody"


# ------------------------------------------------------------------ check


async def test_check_says_what_a_statement_touches_without_running_it() -> None:
    policy = gantry.Policy(
        name="reads-analytics",
        rules=[gantry.allow.query(name="reads", sources=["analytics.*"])],
    )
    db = await _db(policy)
    tool = check_tool(db)

    allowed = await tool.invoke({"sql": "SELECT region FROM analytics.orders"})
    refused = await tool.invoke({"sql": "SELECT * FROM pg_catalog.pg_user"})

    assert allowed["allowed"] is True
    assert allowed["reads"] == ["analytics.orders"]
    assert refused["allowed"] is False
    assert refused["codes"] == ["NO_MATCHING_ALLOW"]
    assert refused["because"], "a refusal with no reason is not worth returning"


async def test_check_declines_to_guess_when_there_is_no_policy() -> None:
    """Saying "allowed" because nothing was configured would be the worst answer."""
    db = await _db()
    answer = await check_tool(db).invoke({"sql": "SELECT 1 FROM analytics.orders"})

    assert answer["allowed"] is None
    assert answer["reads"] == ["analytics.orders"]


async def test_check_runs_nothing(tmp_path: object) -> None:
    """The whole promise of the tool."""
    policy = gantry.Policy(
        name="reads",
        rules=[gantry.allow.query(name="reads", sources=["analytics.*"])],
    )
    db = await _db(policy)
    store = gantry.runs.MemoryRunStore()
    gantry.runs.configure(store)

    await check_tool(db).invoke({"sql": "SELECT region FROM analytics.orders"})

    assert not store.recent(limit=10), "checking a statement recorded a run"


# ----------------------------------------------------------------- status


async def test_status_reports_a_run_someone_else_recorded() -> None:
    """The counterpart of a parked run: the agent has only the id."""
    store = gantry.runs.MemoryRunStore()
    gantry.runs.configure(store)
    db = await _db(
        gantry.Policy(
            name="reads",
            rules=[gantry.allow.query(name="reads", sources=["analytics.*"])],
        )
    )

    run = await db.query(schemas=["analytics"])("SELECT region FROM analytics.orders LIMIT 1")
    answer = await status_tool().invoke({"run_id": run.id})

    assert answer["run_id"] == run.id
    assert answer["status"] == run.status.value
    assert answer["ok"] is True


async def test_status_says_so_when_the_id_is_not_one() -> None:
    gantry.runs.configure(gantry.runs.MemoryRunStore())
    answer = await status_tool().invoke({"run_id": "run_nope"})

    assert answer["status"] == "UNKNOWN"
    assert "run_nope" in answer["error"]
