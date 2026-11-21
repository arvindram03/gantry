# SPDX-License-Identifier: Apache-2.0
"""Column redaction against real PostgreSQL and MySQL.

What is claimed: a redacted column's real value never comes back from a
governed query, however the statement reaches it (`*`, an alias, a function,
a CTE, a derived table, a correlated subquery) and a WHERE on it tests the
mask, not the value; a hashed column gives the same token for the same value,
so joins and distinct counts still work; what cannot be followed (a whole row,
a table the connection does not list, a function that runs SQL from a string)
is refused; the key never appears in a run; a write may set a redacted column
but not read one, and shows it masked in what it returns; and a
materialization that reads one is refused.
"""

from __future__ import annotations

import json
import os
import re
from typing import cast

import gantry
import pytest
from gantry.runs.status import RunStatus
from gantry.sql.redaction import HIDDEN, Redaction

from _live import require_live_or_skip

PG_URL = os.environ.get(
    "GANTRY_TEST_POSTGRES_URL", "postgresql://gantry:gantry@localhost:5432/gantry"
)
MYSQL_URL = os.environ.get(
    "GANTRY_TEST_MYSQL_URL", "mysql://gantry:gantry@127.0.0.1:3306/analytics"
)

KEY = "test-key-0123456789abcdef"
TOKEN = re.compile(r"^h_[0-9a-f]{16}$")

PEOPLE = [
    (1, "ada@example.com", "pro", 120_000),
    (2, "bo@example.com", "free", 80_000),
    (3, "ada@example.com", "free", 95_000),
    (4, "cy@example.com", "pro", 101_000),
]
EMAILS = {row[1] for row in PEOPLE}
SALARIES = {str(row[3]) for row in PEOPLE}


def _redaction(schema: str) -> Redaction:
    return Redaction({f"{schema}.people.email": "hash", f"{schema}.people.salary": "hide"}, key=KEY)


def _leaks(run: gantry.Run) -> list[str]:
    """Any real email or salary, or the key, anywhere in what the run says."""
    said = json.dumps(run.as_tool_result(), default=str)
    found = [value for value in (*EMAILS, *SALARIES, KEY) if value in said]
    stored = gantry.runs.get(run.id)
    if stored is not None and KEY in json.dumps(stored.as_tool_result(), default=str):
        found.append("key in the stored run")
    return found


def _rows(run: gantry.Run) -> list[dict[str, object]]:
    assert run.status is RunStatus.ACCEPTED, (run.status, run.failure)
    return [dict(zip(run.columns, row, strict=True)) for row in run.rows]


# ------------------------------------------------------------- PostgreSQL


async def _pg() -> gantry.sql.SQLConnection:
    import importlib

    pytest.importorskip("asyncpg")
    asyncpg = importlib.import_module("asyncpg")
    try:
        raw = await asyncpg.connect(PG_URL, timeout=5)
    except Exception:
        require_live_or_skip("no PostgreSQL to test redaction against")
        raise
    try:
        await raw.execute("DROP SCHEMA IF EXISTS redact_demo CASCADE")
        await raw.execute("CREATE SCHEMA redact_demo")
        await raw.execute(
            "CREATE TABLE redact_demo.people "
            "(id int PRIMARY KEY, email text, plan text, salary int)"
        )
        await raw.execute(
            "CREATE TABLE redact_demo.orders (id int PRIMARY KEY, person_id int, total int)"
        )
        await raw.execute(
            "CREATE VIEW redact_demo.contacts AS "
            "SELECT id, email AS contact FROM redact_demo.people"
        )
        await raw.executemany("INSERT INTO redact_demo.people VALUES ($1, $2, $3, $4)", PEOPLE)
        await raw.executemany(
            "INSERT INTO redact_demo.orders VALUES ($1, $2, $3)",
            [(10, 1, 50), (11, 3, 70), (12, 2, 20)],
        )
    finally:
        await raw.close()
    return gantry.sql.connect("postgres", url=PG_URL)


async def test_every_way_of_reading_a_redacted_column_reads_the_mask() -> None:
    db = await _pg()
    query = db.query(schemas=["redact_demo"], redaction=_redaction("redact_demo"))

    statements = [
        "SELECT * FROM redact_demo.people ORDER BY id",
        "SELECT p.* FROM redact_demo.people p ORDER BY id",
        "SELECT email AS e, lower(email) AS l, substr(email, 1, 3) AS s, salary + 0 AS pay "
        "FROM redact_demo.people",
        "WITH x AS (SELECT email, salary FROM redact_demo.people) SELECT * FROM x",
        "SELECT t.e FROM (SELECT email AS e FROM redact_demo.people) t",
        "SELECT o.id, (SELECT p.email FROM redact_demo.people p WHERE p.id = o.person_id) "
        "FROM redact_demo.orders o",
        "SELECT email FROM redact_demo.people UNION ALL SELECT plan FROM redact_demo.people",
        "SELECT concat(email, '!'), md5(email) FROM redact_demo.people",
    ]
    for sql in statements:
        run = await query(sql)
        assert run.status is RunStatus.ACCEPTED, (sql, run.failure)
        assert _leaks(run) == [], (sql, _leaks(run))

    everything = _rows(await query("SELECT * FROM redact_demo.people ORDER BY id"))
    assert [row["plan"] for row in everything] == ["pro", "free", "free", "pro"]
    assert all(TOKEN.match(str(row["email"])) for row in everything), everything
    assert {row["salary"] for row in everything} == {HIDDEN}


async def test_a_where_on_a_redacted_column_tests_the_mask() -> None:
    db = await _pg()
    query = db.query(schemas=["redact_demo"], redaction=_redaction("redact_demo"))

    by_email = await query("SELECT id FROM redact_demo.people WHERE email = 'ada@example.com'")
    by_prefix = await query("SELECT count(*) FROM redact_demo.people WHERE email LIKE 'a%'")
    by_salary = await query("SELECT id FROM redact_demo.people WHERE salary > 100000")
    ordered = _rows(await query("SELECT id FROM redact_demo.people ORDER BY salary, id"))

    assert _rows(by_email) == []
    assert _rows(by_prefix) == [{"count": 0}]
    assert _rows(by_salary) == []
    assert [row["id"] for row in ordered] == [1, 2, 3, 4], "ordered by a mask, not by pay"


async def test_a_hashed_column_still_joins_counts_and_groups() -> None:
    db = await _pg()
    query = db.query(schemas=["redact_demo"], redaction=_redaction("redact_demo"))

    distinct = _rows(await query("SELECT count(DISTINCT email) AS people FROM redact_demo.people"))
    grouped = _rows(
        await query(
            "SELECT email, count(*) AS n FROM redact_demo.people GROUP BY email ORDER BY n DESC"
        )
    )
    first = _rows(await query("SELECT email FROM redact_demo.people WHERE id = 1"))
    again = _rows(await query("SELECT email FROM redact_demo.people WHERE id = 3"))

    assert distinct == [{"people": len(EMAILS)}]
    assert grouped[0]["n"] == 2, "ada is in two rows, under one token"
    assert first == again, "the same value gives the same token"


async def test_what_cannot_be_followed_is_refused() -> None:
    db = await _pg()
    query = db.query(schemas=["redact_demo", "pg_catalog"], redaction=_redaction("redact_demo"))

    for sql, said in [
        ("SELECT row_to_json(p) FROM redact_demo.people p", "row_to_json"),
        ("SELECT p::text FROM redact_demo.people p", "whole row"),
        ("SELECT p FROM redact_demo.people p", "whole row"),
        (
            "SELECT query_to_xml('SELECT email FROM redact_demo.people', true, true, '')",
            "query_to_xml",
        ),
        ("SELECT most_common_vals FROM pg_catalog.pg_stats", "not a table"),
        ("SELECT 1; SELECT email FROM redact_demo.people", "one statement"),
    ]:
        run = await query(sql)
        assert run.status is RunStatus.POLICY_REJECTED, (sql, run.status)
        assert run.failure is not None and said in run.failure.message, (sql, run.failure)
        assert _leaks(run) == []


async def test_the_key_never_reaches_a_run_even_in_an_error() -> None:
    db = await _pg()
    query = db.query(schemas=["redact_demo"], redaction=_redaction("redact_demo"))

    failed = await query("SELECT email::int FROM redact_demo.people")

    assert failed.status is not RunStatus.ACCEPTED
    assert _leaks(failed) == []
    assert gantry.runs.get(failed.id) is not None


async def test_a_write_finds_rows_by_the_token_a_query_showed() -> None:
    db = await _pg()
    redaction = _redaction("redact_demo")
    query = db.query(schemas=["redact_demo"], redaction=redaction)
    write = db.write(destinations=["redact_demo.*"], sources=["redact_demo.*"], redaction=redaction)

    # One call reads; a later, separate one writes with what it was shown.
    [shown] = _rows(await query("SELECT email FROM redact_demo.people WHERE id = 1"))
    token = shown["email"]
    upgraded = await write(f"UPDATE redact_demo.people SET plan = 'team' WHERE email = '{token}'")
    renamed = await write(
        f"UPDATE redact_demo.people SET email = 'ada@new.example' WHERE email = '{token}' "
        "AND id = 3 RETURNING *"
    )
    guessed = await write("UPDATE redact_demo.people SET plan = 'x' WHERE email = 'bo@example.com'")
    plans = _rows(
        await db.query(schemas=["redact_demo"])(
            "SELECT id, plan FROM redact_demo.people ORDER BY id"
        )
    )

    assert upgraded.status is RunStatus.ACCEPTED, upgraded.failure
    assert upgraded.rows_affected == 2, "ada is in two rows"
    assert renamed.status is RunStatus.ACCEPTED, renamed.failure
    assert renamed.rows_affected == 1
    returned = dict(zip(renamed.columns, renamed.rows[0], strict=True))
    assert returned["email"] == HIDDEN and returned["salary"] == HIDDEN
    assert guessed.rows_affected == 0, "a value is not a token"
    assert [row["plan"] for row in plans] == ["team", "free", "team", "pro"]
    for run in (upgraded, renamed, guessed):
        assert _leaks(run) == []


async def test_a_write_may_not_copy_a_redacted_value_or_match_a_hidden_one() -> None:
    db = await _pg()
    write = db.write(
        destinations=["redact_demo.*"],
        sources=["redact_demo.*"],
        redaction=_redaction("redact_demo"),
    )

    for sql, said in [
        ("UPDATE redact_demo.people SET plan = email WHERE id = 1", "copies"),
        (
            "UPDATE redact_demo.people SET plan = (SELECT email FROM redact_demo.people "
            "WHERE id = 2) WHERE id = 1",
            "copies",
        ),
        ("UPDATE redact_demo.people SET plan = 'x' WHERE id = 1 RETURNING email", "copies"),
        ("UPDATE redact_demo.people SET plan = 'x' WHERE salary > 100000", "use the row's id"),
    ]:
        run = await write(sql)
        assert run.status is RunStatus.POLICY_REJECTED, (sql, run.status)
        assert run.failure is not None and said in run.failure.message, (sql, run.failure)

    preview = await write.dry_run(
        "UPDATE redact_demo.people SET plan = 'team' WHERE id = 4", preview=5
    )
    assert preview["would_change"] == 1
    columns = cast("list[str]", preview["columns"])
    rows = cast("list[list[object]]", preview["rows"])
    shown = dict(zip(columns, rows[0], strict=True))
    assert shown["email"] == HIDDEN and shown["plan"] == "team"


async def test_a_materialization_that_reads_a_redacted_column_is_refused() -> None:
    db = await _pg()
    build = db.materialize(
        sources=["redact_demo.*"],
        destinations=["redact_demo.*"],
        redaction=_redaction("redact_demo"),
    )

    refused = await build(
        "CREATE TABLE redact_demo.copied AS SELECT id, email FROM redact_demo.people"
    )
    starred = await build("CREATE TABLE redact_demo.starred AS SELECT * FROM redact_demo.people")
    allowed = await build(
        "CREATE TABLE redact_demo.plans AS SELECT id, plan FROM redact_demo.people"
    )

    for run in (refused, starred):
        assert run.status is RunStatus.POLICY_REJECTED, run.failure
        assert run.failure is not None and "redact_demo.people.email" in run.failure.message
    assert allowed.status is RunStatus.ACCEPTED, allowed.failure


async def test_a_view_is_read_only_once_someone_has_reviewed_it() -> None:
    db = await _pg()
    unreviewed = db.query(schemas=["redact_demo"], redaction=_redaction("redact_demo"))
    reviewed = db.query(
        schemas=["redact_demo"],
        redaction=Redaction(
            {"redact_demo.people.email": "hash", "redact_demo.contacts.contact": "hash"},
            key=KEY,
            views=("redact_demo.contacts",),
        ),
    )

    # The view shows email as `contact`, a column the people pattern does not name.
    refused = await unreviewed("SELECT contact FROM redact_demo.contacts")
    joined = await unreviewed(
        "SELECT count(*) FROM redact_demo.people p JOIN redact_demo.contacts c ON c.id = p.id"
    )
    allowed = _rows(await reviewed("SELECT contact FROM redact_demo.contacts ORDER BY id"))

    for run in (refused, joined):
        assert run.status is RunStatus.POLICY_REJECTED, run.failure
        assert run.failure is not None and "is a view" in run.failure.message
    assert all(TOKEN.match(str(row["contact"])) for row in allowed)


async def test_a_hashed_token_is_hmac_sha256_of_the_value() -> None:
    import hashlib
    import hmac

    db = await _pg()
    query = db.query(schemas=["redact_demo"], redaction=_redaction("redact_demo"))

    [row] = _rows(await query("SELECT email FROM redact_demo.people WHERE id = 1"))

    expected = hmac.new(KEY.encode(), b"ada@example.com", hashlib.sha256).hexdigest()[:16]
    assert row["email"] == f"h_{expected}"


# ------------------------------------------------------------------ MySQL


async def _mysql() -> gantry.sql.SQLConnection:
    import importlib

    from gantry.sql.adapters.mysql import _connect_kwargs

    pytest.importorskip("aiomysql")
    aiomysql = importlib.import_module("aiomysql")
    config = {**_connect_kwargs({"url": MYSQL_URL}), "autocommit": True}
    try:
        raw = await aiomysql.connect(**config)
    except Exception:
        require_live_or_skip("no MySQL to test redaction against")
        raise
    try:
        async with raw.cursor() as cursor:
            await cursor.execute("DROP TABLE IF EXISTS people")
            await cursor.execute(
                "CREATE TABLE people (id INT PRIMARY KEY, email VARCHAR(64), "
                "plan VARCHAR(16), salary INT)"
            )
            await cursor.executemany("INSERT INTO people VALUES (%s, %s, %s, %s)", PEOPLE)
    finally:
        raw.close()
    return gantry.sql.connect("mysql", url=MYSQL_URL)


async def test_on_mysql_too() -> None:
    db = await _mysql()
    schema = str(db._default_schema())
    query = db.query(schemas=[schema], redaction=_redaction(schema))

    everything = _rows(await query("SELECT * FROM people ORDER BY id"))
    by_email = await query("SELECT id FROM people WHERE email = 'ada@example.com'")
    distinct = _rows(await query("SELECT count(DISTINCT email) AS people FROM people"))
    whole = await query(f"SELECT * FROM {schema}.people p WHERE p.id = 1")

    assert all(TOKEN.match(str(row["email"])) for row in everything), everything
    assert {row["salary"] for row in everything} == {HIDDEN}
    assert [row["plan"] for row in everything] == ["pro", "free", "free", "pro"]
    assert _rows(by_email) == []
    assert distinct == [{"people": len(EMAILS)}]
    assert whole.status is RunStatus.ACCEPTED and _leaks(whole) == []
    import hashlib
    import hmac

    expected = hmac.new(KEY.encode(), b"ada@example.com", hashlib.sha256).hexdigest()[:16]
    assert everything[0]["email"] == f"h_{expected}", "the same token as PostgreSQL gives"

    write = db.write(destinations=[f"{schema}.*"], redaction=_redaction(schema))
    token = everything[0]["email"]
    upgraded = await write(f"UPDATE {schema}.people SET plan = 'team' WHERE email = '{token}'")
    copied = await write(f"UPDATE {schema}.people SET plan = email WHERE id = 1")
    assert upgraded.rows_affected == 2, upgraded.failure
    assert copied.status is RunStatus.POLICY_REJECTED
