# SPDX-License-Identifier: Apache-2.0
"""Stats only against real PostgreSQL and MySQL.

What is claimed: a query answers the agent's own SQL, joins and subqueries
included, when everything it returns is a group key or an aggregate; the whole
answer is refused when any group came from fewer distinct records than the
minimum, counted by primary key across a join so that repeating one record
does not make a crowd; a minimum of 0 checks the statement only; what returns
a row's own value is refused before it runs; and it composes with redaction.
"""

from __future__ import annotations

import os
from datetime import datetime

import gantry
import pytest
from gantry.runs.status import RunStatus
from gantry.sql.redaction import Redaction
from gantry.sql.stats_only import COUNTED, StatsOnly

from _live import require_live_or_skip

PG_URL = os.environ.get(
    "GANTRY_TEST_POSTGRES_URL", "postgresql://gantry:gantry@localhost:5432/gantry"
)
MYSQL_URL = os.environ.get(
    "GANTRY_TEST_MYSQL_URL", "mysql://gantry:gantry@127.0.0.1:3306/analytics"
)

#: 30 paid, 12 failed, 3 refunded.
STATUSES = ["paid"] * 30 + ["failed"] * 12 + ["refunded"] * 3
#: 43 people: 30 free, 10 pro, 3 team. Every fourth is inactive: 10 of them.
PEOPLE = [
    (n, "free" if n <= 30 else "pro" if n <= 40 else "team", n % 4 != 0, f"p{n}@example.com")
    for n in range(1, 44)
]
#: Orders belong to people 1 to 20 only.
OWNERS = 20


def _orders() -> list[tuple[object, ...]]:
    return [
        (
            index + 1,
            index % OWNERS + 1,
            status,
            100 + index,
            datetime(2026, 1 if index % 2 == 0 else 2, 1 + index % 27, 12, 0),
        )
        for index, status in enumerate(STATUSES)
    ]


async def _pg() -> gantry.sql.SQLConnection:
    import importlib

    pytest.importorskip("asyncpg")
    asyncpg = importlib.import_module("asyncpg")
    try:
        raw = await asyncpg.connect(PG_URL, timeout=5)
    except Exception:
        require_live_or_skip("no PostgreSQL to test stats only against")
        raise
    try:
        await raw.execute("DROP SCHEMA IF EXISTS stats_demo CASCADE")
        await raw.execute("CREATE SCHEMA stats_demo")
        await raw.execute(
            "CREATE TABLE stats_demo.people (id int PRIMARY KEY, plan text, active boolean, "
            "email text)"
        )
        await raw.execute(
            "CREATE TABLE stats_demo.orders (id int PRIMARY KEY, person_id int, status text, "
            "total int, placed_at timestamp)"
        )
        await raw.executemany("INSERT INTO stats_demo.people VALUES ($1, $2, $3, $4)", PEOPLE)
        await raw.executemany(
            "INSERT INTO stats_demo.orders VALUES ($1, $2, $3, $4, $5)", _orders()
        )
    finally:
        await raw.close()
    return gantry.sql.connect("postgres", url=PG_URL)


def _rows(run: gantry.Run) -> list[list[object]]:
    assert run.status is RunStatus.ACCEPTED, (run.status, run.failure)
    assert not any(column.startswith(COUNTED) for column in run.columns), run.columns
    return [list(row) for row in run.rows]


def _small(run: gantry.Run) -> None:
    assert run.status is RunStatus.POLICY_REJECTED, (run.status, run.failure)
    assert run.failure is not None and "fewer than" in run.failure.message
    assert run.rows == ()


async def test_how_many_people_are_not_active() -> None:
    query = (await _pg()).query(schemas=["stats_demo"], stats_only=StatsOnly(10))

    inactive = await query("SELECT count(*) AS inactive FROM stats_demo.people WHERE NOT active")
    no_orders = await query(
        "SELECT count(*) FROM stats_demo.people p WHERE NOT EXISTS "
        "(SELECT 1 FROM stats_demo.orders o WHERE o.person_id = p.id)"
    )
    by_plan = await query(
        "WITH buyers AS (SELECT DISTINCT person_id FROM stats_demo.orders) "
        "SELECT plan, count(*) FROM stats_demo.people "
        "WHERE id NOT IN (SELECT person_id FROM buyers) GROUP BY plan"
    )

    assert _rows(inactive) == [[10]] and inactive.columns == ("inactive",)
    assert _rows(no_orders) == [[len(PEOPLE) - OWNERS]]
    _small(by_plan)  # 10 free, 10 pro and 3 team without orders: team is too small


async def test_the_minimum_refuses_the_whole_answer_and_zero_checks_only() -> None:
    db = await _pg()
    by_status = "SELECT status, count(*), avg(total) FROM stats_demo.orders GROUP BY 1 ORDER BY 1"
    one = "SELECT count(*), avg(total) FROM stats_demo.orders WHERE id = 1"

    _small(await db.query(schemas=["stats_demo"], stats_only=StatsOnly(10))(by_status))
    _small(await db.query(schemas=["stats_demo"], stats_only=StatsOnly(10))(one))
    three = _rows(await db.query(schemas=["stats_demo"], stats_only=StatsOnly(3))(by_status))
    unchecked = _rows(await db.query(schemas=["stats_demo"], stats_only=StatsOnly(0))(one))

    assert [(row[0], row[1]) for row in three] == [("failed", 12), ("paid", 30), ("refunded", 3)]
    assert unchecked[0][0] == 1


async def test_joins_count_records_not_rows() -> None:
    query = (await _pg()).query(schemas=["stats_demo"], stats_only=StatsOnly(10))

    by_plan = await query(
        "SELECT p.plan, count(*), count(DISTINCT p.id) FROM stats_demo.orders o "
        "JOIN stats_demo.people p ON p.id = o.person_id GROUP BY p.plan"
    )
    # 11 orders from inactive people, placed by only 5 of them.
    by_activity = await query(
        "SELECT p.active, count(*) FROM stats_demo.orders o "
        "JOIN stats_demo.people p ON p.id = o.person_id GROUP BY p.active"
    )
    # One order repeated once for each of 43 people: 43 rows, one record.
    repeated = await query(
        "SELECT avg(o.total), count(*) FROM stats_demo.orders o "
        "CROSS JOIN stats_demo.people p WHERE o.id = 1"
    )

    assert _rows(by_plan) == [["free", len(STATUSES), OWNERS]]
    _small(by_activity)
    _small(repeated)


async def test_what_returns_a_row_s_value_is_refused_before_it_runs() -> None:
    query = (await _pg()).query(schemas=["stats_demo"], stats_only=StatsOnly(2))
    table = "stats_demo.orders"

    for sql, said in {
        f"SELECT * FROM {table}": "`*`",
        f"SELECT status, total FROM {table} GROUP BY status": "neither grouped nor aggregated",
        f"SELECT max(total) FROM {table}": "max returns a row's own value",
        f"SELECT sum(CASE WHEN id = 1 THEN total ELSE 0 END) FROM {table}": "plain column",
        f"SELECT count(*) FILTER (WHERE id = 1) FROM {table}": "FILTER",
        f"SELECT row_number() OVER () FROM {table}": "window",
        f"SELECT count(*) FROM (SELECT * FROM {table}) x": "is tables",
        f"WITH x AS (SELECT * FROM {table}) SELECT count(*) FROM x": "is a CTE",
        f"SELECT count(*) FROM {table}; DELETE FROM {table}": "one statement",
    }.items():
        run = await query(sql)
        assert run.status is RunStatus.POLICY_REJECTED, (sql, run.status)
        assert run.failure is not None and said in run.failure.message, (sql, run.failure)


async def test_with_redaction_too() -> None:
    db = await _pg()
    query = db.query(
        schemas=["stats_demo"],
        stats_only=StatsOnly(10),
        redaction=Redaction({"stats_demo.people.email": "hash"}, key="test-key-0123456789abcdef"),
    )

    people = _rows(await query("SELECT count(DISTINCT email) FROM stats_demo.people"))
    by_email = await query("SELECT count(*) FROM stats_demo.people WHERE email = 'p1@example.com'")

    assert people == [[len(PEOPLE)]], "a hashed column still counts each person once"
    _small(by_email)  # the WHERE tests the mask: nobody matches, a group of none


async def test_on_mysql_too() -> None:
    import importlib

    from gantry.sql.adapters.mysql import _connect_kwargs

    pytest.importorskip("aiomysql")
    aiomysql = importlib.import_module("aiomysql")
    config = {**_connect_kwargs({"url": MYSQL_URL}), "autocommit": True}
    try:
        raw = await aiomysql.connect(**config)
    except Exception:
        require_live_or_skip("no MySQL to test stats only against")
        raise
    try:
        async with raw.cursor() as cursor:
            for statement in (
                "DROP TABLE IF EXISTS stats_orders",
                "DROP TABLE IF EXISTS stats_people",
                "CREATE TABLE stats_people (id INT PRIMARY KEY, plan VARCHAR(8), "
                "active BOOLEAN, email VARCHAR(64))",
                "CREATE TABLE stats_orders (id INT PRIMARY KEY, person_id INT, "
                "status VARCHAR(16), total INT, placed_at DATETIME)",
            ):
                await cursor.execute(statement)
            await cursor.executemany("INSERT INTO stats_people VALUES (%s, %s, %s, %s)", PEOPLE)
            await cursor.executemany(
                "INSERT INTO stats_orders VALUES (%s, %s, %s, %s, %s)", _orders()
            )
    finally:
        raw.close()
    db = gantry.sql.connect("mysql", url=MYSQL_URL)
    schema = str(db._default_schema())
    query = db.query(schemas=[schema], stats_only=StatsOnly(10))

    by_month = await query(
        f"SELECT DATE_FORMAT(placed_at, '%Y-%m') AS month, count(*) FROM {schema}.stats_orders "
        "GROUP BY month ORDER BY month"
    )
    by_plan = await query(
        f"SELECT p.plan, count(*) FROM {schema}.stats_orders o JOIN {schema}.stats_people p "
        "ON p.id = o.person_id GROUP BY p.plan"
    )
    repeated = await query(
        f"SELECT avg(o.total) FROM {schema}.stats_orders o "
        f"CROSS JOIN {schema}.stats_people p WHERE o.id = 1"
    )

    assert _rows(by_month) == [["2026-01", 23], ["2026-02", 22]]
    assert _rows(by_plan) == [["free", len(STATUSES)]]
    _small(repeated)
