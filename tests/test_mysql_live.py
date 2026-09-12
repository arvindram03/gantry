# SPDX-License-Identifier: Apache-2.0
"""The MySQL adapter against a real MySQL.

Everything here is something a stub answers correctly by construction and a
server does not. Three of them were wrong in the first draft of the adapter and
only the server said so: the row-count metadata key, the missing destination
reference, and the assumption that `max_execution_time` bounds a write.

Skipped unless a server is reachable, so a clone without one still passes.
Point it somewhere with `GANTRY_TEST_MYSQL_URL`, and set `GANTRY_REQUIRE_LIVE=1`
in a job that brings one up so a skip fails loudly instead.

    docker run -d --name mysql -e MYSQL_ROOT_PASSWORD=gantry \\
        -e MYSQL_DATABASE=analytics -e MYSQL_USER=gantry \\
        -e MYSQL_PASSWORD=gantry -p 3306:3306 mysql:8
"""

from __future__ import annotations

import asyncio
import os
import time

import gantry
import pytest
from gantry.failure import FailureKind
from gantry.runs.status import RunStatus

from _live import require_live_or_skip

URL = os.environ.get("GANTRY_TEST_MYSQL_URL", "mysql://gantry:gantry@127.0.0.1:3306/analytics")
REPORTING = os.environ.get("GANTRY_TEST_MYSQL_REPORTING", "reporting")

SEED = (
    "CREATE DATABASE IF NOT EXISTS analytics",
    f"CREATE DATABASE IF NOT EXISTS {REPORTING}",
    # Dropped rather than created-if-missing. These tests own this table, and a
    # leftover of a different shape — examples/mysql_customers.py builds one
    # with an extra column — otherwise fails the insert with "column count
    # doesn't match value count".
    "DROP TABLE IF EXISTS analytics.customers",
    "CREATE TABLE analytics.customers (id INT PRIMARY KEY, plan VARCHAR(32), status VARCHAR(16))",
    "INSERT INTO analytics.customers VALUES "
    "(1,'free','active'),(2,'pro','active'),(3,'free','churned')",
)


async def _raw() -> object:
    """A connection that is not Gantry's, for arranging and for checking after."""
    import importlib

    from gantry.sql.adapters.mysql import _connect_kwargs

    aiomysql = importlib.import_module("aiomysql")
    config = _connect_kwargs({"url": URL})
    config.pop("db", None)
    try:
        return await aiomysql.connect(**config)
    except Exception:
        require_live_or_skip(f"no MySQL at {URL.rsplit('@', 1)[-1]}")
        raise


async def _seed() -> object:
    connection = await _raw()
    async with connection.cursor() as cursor:  # type: ignore[attr-defined]
        for statement in SEED:
            await cursor.execute(statement)
    return connection


async def _scalar(connection: object, sql: str) -> object:
    async with connection.cursor() as cursor:  # type: ignore[attr-defined]
        await cursor.execute(sql)
        row = await cursor.fetchone()
    return None if row is None else row[0]


@pytest.fixture
async def db() -> gantry.sql.SQLConnection:
    pytest.importorskip("aiomysql")
    connection = await _seed()
    connection.close()  # type: ignore[attr-defined]
    return gantry.sql.connect("mysql", url=URL)


async def test_describe_reads_a_real_information_schema(db: gantry.sql.SQLConnection) -> None:
    """A MySQL schema is a database, and its catalog column says nothing.

    `information_schema.tables.table_catalog` is the literal string `def` on
    every row, so reporting it as a catalog would invent a level of naming that
    does not exist here.
    """
    schema = await db.describe()

    assert "analytics" in schema.schemas
    assert schema.catalogs == ()
    customers = next(t for t in schema.tables if t.name == "customers")
    assert customers.schema == "analytics"
    assert customers.catalog is None
    assert {column.name for column in customers.columns} == {"id", "plan", "status"}
    assert customers.kind == "base table"


async def test_a_governed_query_returns_bounded_rows(db: gantry.sql.SQLConnection) -> None:
    query = db.query(schemas=["analytics"], max_rows=2)

    result = await query("SELECT id, plan FROM analytics.customers ORDER BY id")

    assert result.status is RunStatus.ACCEPTED
    assert result.inline is not None
    assert len(result.rows) == 2
    assert result.truncated is True
    assert result.columns == ("id", "plan")


async def test_a_write_is_refused_before_it_reaches_the_server(
    db: gantry.sql.SQLConnection,
) -> None:
    query = db.query(schemas=["analytics"])

    result = await query("DELETE FROM analytics.customers")

    assert result.status is RunStatus.POLICY_REJECTED
    assert result.handle is None, "a handle would mean it was submitted"
    connection = await _raw()
    try:
        assert await _scalar(connection, "SELECT count(*) FROM analytics.customers") == 3
    finally:
        connection.close()  # type: ignore[attr-defined]


async def test_the_read_only_transaction_catches_what_the_classifier_would_not() -> None:
    """Defence in depth, measured by going around the classifier.

    The adapter is handed a write directly under a read-only policy. MySQL
    answers 1792, "cannot execute statement in a READ ONLY transaction", which
    is a policy refusal rather than an engine fault — and nothing is written.
    """
    pytest.importorskip("aiomysql")
    from gantry.context import Context
    from gantry.sql.adapters.mysql import MySQLAdapter
    from gantry.sql.policy import SQLPolicy
    from gantry.sql.target import SQLTarget

    connection = await _seed()
    connection.close()  # type: ignore[attr-defined]
    target = SQLTarget("mysql", "mysql", "aiomysql", {"url": URL})
    adapter = MySQLAdapter(target)
    context = Context(metadata={"gantry.sql.policy": SQLPolicy(read_only=True)})

    handle = await adapter.submit(
        "INSERT INTO analytics.customers VALUES (99, 'x', 'y')", target, context
    )
    result = await adapter.result(handle)

    assert not result.ok
    assert result.failure is not None
    assert result.failure.native_code == "1792"
    assert result.failure.kind is FailureKind.POLICY_REJECTED
    connection = await _raw()
    try:
        assert (
            await _scalar(connection, "SELECT count(*) FROM analytics.customers WHERE id = 99") == 0
        )
    finally:
        connection.close()  # type: ignore[attr-defined]


async def test_materialize_creates_verifies_and_then_refuses_to_repeat(
    db: gantry.sql.SQLConnection,
) -> None:
    connection = await _raw()
    async with connection.cursor() as cursor:  # type: ignore[attr-defined]
        await cursor.execute(f"DROP TABLE IF EXISTS {REPORTING}.active_customers")
    connection.close()  # type: ignore[attr-defined]

    build = db.materialize(
        sources=["analytics.*"],
        destinations=[f"{REPORTING}.*"],
        verify=[gantry.verify.destination_exists(), gantry.verify.row_count(min=1, max=10)],
    )
    sql = (
        f"CREATE TABLE {REPORTING}.active_customers AS "
        "SELECT id, plan FROM analytics.customers WHERE status = 'active'"
    )

    result = await build(sql)

    assert result.status is RunStatus.ACCEPTED, result.failure
    assert result.uri == f"mysql://{REPORTING}/active_customers"
    checks = {
        check.name: check for check in (result.verification.checks if result.verification else ())
    }
    assert checks["destination_exists"].ok
    # Reads `rows` from the table metadata; `row_count` was the wrong key and
    # produced "destination row count is unavailable" against a real server.
    assert checks["row_count"].ok and checks["row_count"].actual == 2

    repeat = await build(sql)
    assert repeat.status is RunStatus.POLICY_REJECTED
    assert "already exists" in (repeat.failure.message if repeat.failure else "")


async def test_a_destination_outside_the_policy_is_refused(db: gantry.sql.SQLConnection) -> None:
    build = db.materialize(sources=["analytics.*"], destinations=[f"{REPORTING}.*"])

    result = await build("CREATE TABLE analytics.sneaky AS SELECT id FROM analytics.customers")

    assert result.status is RunStatus.POLICY_REJECTED
    connection = await _raw()
    try:
        exists = await _scalar(
            connection,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='analytics' AND table_name='sneaky'",
        )
        assert exists == 0
    finally:
        connection.close()  # type: ignore[attr-defined]


async def test_the_timeout_stops_the_server_not_just_the_waiting() -> None:
    """`max_execution_time` covers `SELECT` and nothing else.

    A `CREATE TABLE ... AS SELECT` ran to completion under a 200ms limit on
    MySQL 8.4, so the adapter also holds a deadline and issues `KILL QUERY` when
    it expires. This asserts the query is gone from the server afterwards, not
    merely that the call returned: abandoning a statement while MySQL keeps
    running it is the failure mode worth testing for.
    """
    pytest.importorskip("aiomysql")
    digits = (
        "(SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4"
        " UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9)"
    )
    connection = await _seed()
    async with connection.cursor() as cursor:  # type: ignore[attr-defined]
        await cursor.execute("DROP TABLE IF EXISTS analytics.slow_source")
        # 2,500 rows self-joined on an inequality: around three million output
        # rows, and measured at roughly five seconds unbounded. Deliberately not
        # SLEEP() — MySQL interrupts SLEEP and lets the statement carry on, so a
        # killed SLEEP-based write still creates its table and proves nothing.
        await cursor.execute(
            "CREATE TABLE analytics.slow_source AS SELECT n.id FROM ("
            f"  SELECT a.n + b.n*10 + c.n*100 + d.n*1000 AS id"
            f"  FROM {digits} a, {digits} b, {digits} c, {digits} d"
            ") n WHERE n.id < 2500"
        )
        await cursor.execute(f"DROP TABLE IF EXISTS {REPORTING}.slow_copy")
    connection.close()  # type: ignore[attr-defined]

    db = gantry.sql.connect("mysql", url=URL)
    build = db.materialize(sources=["analytics.*"], destinations=[f"{REPORTING}.*"], timeout=1.0)

    started = time.monotonic()
    result = await build(
        f"CREATE TABLE {REPORTING}.slow_copy AS "
        "SELECT a.id AS x, b.id AS y FROM analytics.slow_source a "
        "JOIN analytics.slow_source b ON a.id < b.id"
    )
    elapsed = time.monotonic() - started

    assert result.status is not RunStatus.ACCEPTED
    assert result.failure is not None
    assert "exceeded" in result.failure.message
    assert elapsed < 20, f"the deadline did not bound the call: {elapsed:.1f}s"

    connection = await _raw()
    try:
        # Poll rather than sleep a fixed interval. `KILL QUERY` marks the
        # statement killed immediately, but MySQL still has to unwind a
        # part-built table of several million rows, and how long that takes is
        # a property of the machine rather than of the thing being tested.
        running = 1
        for _ in range(150):
            counted = await _scalar(
                connection,
                "SELECT count(*) FROM information_schema.processlist "
                "WHERE info LIKE '%slow_source%' AND id <> CONNECTION_ID()",
            )
            running = int(counted) if isinstance(counted, int) else 0
            if running == 0:
                break
            await asyncio.sleep(0.1)
        assert running == 0, "the statement was still running 15s after the timeout returned"
        left_behind = await _scalar(
            connection,
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema='{REPORTING}' AND table_name='slow_copy'",
        )
        assert left_behind == 0, "a half-built destination survived the timeout"
    finally:
        async with connection.cursor() as cursor:  # type: ignore[attr-defined]
            await cursor.execute("DROP TABLE IF EXISTS analytics.slow_source")
        connection.close()  # type: ignore[attr-defined]


async def test_native_validation_rejects_before_anything_runs(
    db: gantry.sql.SQLConnection,
) -> None:
    """`PREPARE`, not `EXPLAIN`: MySQL cannot EXPLAIN the DDL a materialization is."""
    build = db.materialize(sources=["analytics.*"], destinations=[f"{REPORTING}.*"])

    result = await build(
        f"CREATE TABLE {REPORTING}.from_nowhere AS SELECT id FROM analytics.no_such_table"
    )

    assert result.status is not RunStatus.ACCEPTED
    connection = await _raw()
    try:
        exists = await _scalar(
            connection,
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema='{REPORTING}' AND table_name='from_nowhere'",
        )
        assert exists == 0
    finally:
        connection.close()  # type: ignore[attr-defined]
