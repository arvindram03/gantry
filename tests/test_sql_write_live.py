# SPDX-License-Identifier: Apache-2.0
"""Governed writes and the table scanner, against real databases.

The claims here are about what an engine does: that a statement over its limit
leaves the rows where they were, that a command tag counts what it says it
counts, and that a table named in the second position of a `FROM` list is one
the policy gets asked about. A stub would agree with whatever this code did.

The scanner tests are the reason this file exists in the shape it does. Each
statement in them was admitted, under a policy allowing only `analytics.*`,
and returned rows from another schema.
"""

from __future__ import annotations

import os

import gantry
import pytest
from gantry import Policy, allow, deny
from gantry.failure import FailureKind
from gantry.runs.status import RunStatus

from _live import require_live_or_skip

POSTGRES = os.environ.get(
    "GANTRY_TEST_POSTGRES_URL", "postgresql://gantry:gantry@localhost:5432/gantry"
)
MYSQL = os.environ.get("GANTRY_TEST_MYSQL_URL", "mysql://gantry:gantry@127.0.0.1:3306/analytics")


# ------------------------------------------------------------------ postgres


def _never_ran(run: object) -> None:
    """A refused run has no execution: one "RUNNING" since submission reads as work in flight."""
    stored = gantry.runs.get(run.id)  # type: ignore[attr-defined]
    assert stored is not None
    assert stored.execution is None, stored.execution


async def _postgres() -> object:
    asyncpg = pytest.importorskip("asyncpg")
    try:
        return await asyncpg.connect(POSTGRES, timeout=5)
    except Exception:
        require_live_or_skip(f"no PostgreSQL at {POSTGRES.rsplit('@', 1)[-1]}")
        raise


@pytest.fixture
async def raw() -> object:
    """A connection that is not Gantry's: to arrange, and to check afterwards."""
    connection = await _postgres()
    for statement in (
        "CREATE SCHEMA IF NOT EXISTS write_scratch",
        "CREATE SCHEMA IF NOT EXISTS write_private",
        "DROP TABLE IF EXISTS write_scratch.notes",
        "DROP TABLE IF EXISTS write_scratch.archive",
        "DROP TABLE IF EXISTS write_private.secrets",
        "CREATE TABLE write_scratch.notes (id int PRIMARY KEY, body text)",
        "CREATE TABLE write_scratch.archive (id int PRIMARY KEY, body text)",
        "CREATE TABLE write_private.secrets (id int PRIMARY KEY, token text)",
        "INSERT INTO write_scratch.notes SELECT g, 'n' FROM generate_series(1, 50) g",
        "INSERT INTO write_private.secrets VALUES (1, 'hunter2'), (2, 'swordfish')",
    ):
        await connection.execute(statement)  # type: ignore[attr-defined]
    yield connection
    await connection.close()  # type: ignore[attr-defined]


async def _count(raw: object, table: str, where: str = "true") -> int:
    return int(await raw.fetchval(f"SELECT count(*) FROM {table} WHERE {where}"))  # type: ignore[attr-defined]


def _db(policy: Policy | None = None) -> gantry.sql.SQLConnection:
    return gantry.sql.connect("postgres", url=POSTGRES, policy=policy)


async def test_postgres_counts_what_a_write_changed(raw: object) -> None:
    write = _db().write(destinations=["write_scratch.*"])

    updated = await write("UPDATE write_scratch.notes SET body = 'seen' WHERE id <= 3")
    inserted = await write("INSERT INTO write_scratch.notes VALUES (100, 'a'), (101, 'b')")
    deleted = await write("DELETE FROM write_scratch.notes WHERE id = 50")

    assert (updated.rows_affected, inserted.rows_affected, deleted.rows_affected) == (3, 2, 1)
    assert all(run.status is RunStatus.ACCEPTED for run in (updated, inserted, deleted))
    assert await _count(raw, "write_scratch.notes", "body = 'seen'") == 3
    assert await _count(raw, "write_scratch.notes") == 51


async def test_postgres_rolls_back_a_write_over_its_limit(raw: object) -> None:
    """Forty rows asked for, ten allowed, fifty still there."""
    write = _db().write(destinations=["write_scratch.*"], max_rows_affected=10)

    run = await write("DELETE FROM write_scratch.notes WHERE id <= 40")

    assert run.status is RunStatus.EXECUTION_FAILED
    assert run.failure is not None
    assert run.failure.kind is FailureKind.ROW_LIMIT_EXCEEDED
    assert "40 rows" in run.failure.message
    assert await _count(raw, "write_scratch.notes") == 50


async def test_postgres_rolls_back_a_returning_write_over_its_limit(raw: object) -> None:
    """The cursor reads one row past the limit; the statement is undone whole."""
    write = _db().write(destinations=["write_scratch.*"], max_rows_affected=10)

    over = await write("DELETE FROM write_scratch.notes WHERE id <= 40 RETURNING id")
    under = await write("DELETE FROM write_scratch.notes WHERE id <= 4 RETURNING id, body")

    assert over.status is RunStatus.EXECUTION_FAILED
    assert under.status is RunStatus.ACCEPTED
    assert under.rows_affected == 4
    assert under.columns == ("id", "body")
    assert {row[0] for row in under.rows} == {1, 2, 3, 4}
    assert await _count(raw, "write_scratch.notes") == 46


async def test_postgres_runs_an_upsert_as_the_insert_it_is(raw: object) -> None:
    """`DO UPDATE` is a clause, not a second write to a table called SET."""
    write = _db().write(destinations=["write_scratch.*"])

    run = await write(
        "INSERT INTO write_scratch.notes (id, body) VALUES (1, 'again'), (200, 'new') "
        "ON CONFLICT (id) DO UPDATE SET body = excluded.body"
    )

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert run.rows_affected == 2
    assert await _count(raw, "write_scratch.notes", "body IN ('again', 'new')") == 2


async def test_postgres_writes_text_that_reads_like_sql(raw: object) -> None:
    write = _db().write(destinations=["write_scratch.*"])

    run = await write(
        "UPDATE write_scratch.notes SET body = 'update from Bob: delete from review, "
        "moved into done' WHERE id = 1"
    )

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert await _count(raw, "write_scratch.notes", "body LIKE 'update from Bob%'") == 1


async def test_postgres_refuses_a_write_that_reads_what_it_was_not_given(raw: object) -> None:
    write = _db().write(destinations=["write_scratch.*"])

    for sql in (
        "INSERT INTO write_scratch.archive SELECT id, token FROM write_private.secrets",
        "UPDATE write_scratch.notes n SET body = s.token FROM write_private.secrets s "
        "WHERE n.id = s.id",
        "INSERT INTO write_scratch.archive "
        "SELECT n.id, s.token FROM write_scratch.notes n, write_private.secrets s",
    ):
        run = await write(sql)
        assert run.status is RunStatus.POLICY_REJECTED, sql
        assert run.failure is not None
        _never_ran(run)
        assert "write_private.secrets" in run.failure.message

    assert await _count(raw, "write_scratch.archive") == 0
    assert await _count(raw, "write_scratch.notes", "body <> 'n'") == 0


async def test_postgres_refuses_a_data_modifying_cte_as_two_writes(raw: object) -> None:
    """PostgreSQL would run both halves. One write changes one table."""
    write = _db().write(destinations=["write_scratch.*"])

    run = await write(
        "WITH gone AS (DELETE FROM write_scratch.notes WHERE id <= 2 RETURNING *) "
        "INSERT INTO write_scratch.archive SELECT * FROM gone"
    )

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None
    assert "write_scratch.notes, write_scratch.archive" in run.failure.message
    _never_ran(run)
    assert await _count(raw, "write_scratch.notes") == 50


async def test_postgres_holds_a_write_to_both_the_operation_and_the_policy(raw: object) -> None:
    policy = Policy(
        "scratch-only",
        [
            allow.write(destinations=["write_scratch.*"]),
            deny.write(destinations=["write_scratch.archive"], name="archive-is-frozen"),
        ],
    )
    write = _db(policy).write(destinations=["write_scratch.*"])

    frozen = await write("INSERT INTO write_scratch.archive VALUES (1, 'x')")
    open_ = await write("INSERT INTO write_scratch.notes VALUES (300, 'x')")

    assert frozen.status is RunStatus.POLICY_REJECTED
    assert frozen.admission is not None
    assert frozen.admission.matched_rules == ("archive-is-frozen",)
    assert open_.status is RunStatus.ACCEPTED
    assert await _count(raw, "write_scratch.archive") == 0


# ------------------------------------------ the read path, where it was open


def _reader() -> gantry.sql.SQLQuery:
    """Bounds and a policy that both say: `write_scratch`, and nothing else."""
    policy = Policy("scratch-reads", [allow.query(sources=["write_scratch.*"])])
    return _db(policy).query(schemas=["write_scratch"], max_rows=5)


@pytest.mark.parametrize(
    "sql",
    [
        # One table per FROM was all the scanner saw.
        "SELECT s.* FROM write_scratch.notes n, write_private.secrets s",
        # Nor the first table inside a parenthesised join.
        "SELECT * FROM (write_private.secrets CROSS JOIN write_scratch.notes)",
        # A parenthesis in a string moved the depth of what followed it.
        "SELECT s.* FROM write_scratch.notes n JOIN write_scratch.archive a ON a.body = '(' , "
        "write_private.secrets s",
        # No FROM at all: the table is named by a string.
        "SELECT table_to_xml('write_private.secrets', true, false, '')",
        # And the string can be assembled, so reading it proves nothing.
        "SELECT query_to_xml('select * fr' || 'om write_private.secrets', true, false, '')",
        "SELECT \"table_to_xml\"('write_private.secrets', true, false, '')",
    ],
)
async def test_a_read_cannot_reach_past_the_policy(raw: object, sql: str) -> None:
    """Every one of these returned `hunter2` before the scanner followed the list."""
    run = await _reader()(sql)

    assert run.status is RunStatus.POLICY_REJECTED, f"{sql} -> {run.rows}"
    assert not run.rows
    _never_ran(run)
    assert "hunter2" not in str(run.as_tool_result())


async def test_the_reads_that_should_work_still_do(raw: object) -> None:
    """The fix names more tables; it must not start refusing the allowed ones."""
    query = _reader()

    for sql in (
        "SELECT n.id FROM write_scratch.notes n, write_scratch.archive a",
        "SELECT * FROM (write_scratch.notes n CROSS JOIN write_scratch.archive a)",
        "SELECT id FROM write_scratch.notes WHERE body IN ('a', 'b') ORDER BY id, body",
    ):
        run = await query(sql)
        assert run.status is RunStatus.ACCEPTED, f"{sql} -> {run.failure}"


# --------------------------------------------------------------------- mysql


async def _mysql() -> object:
    import importlib

    pytest.importorskip("aiomysql")
    from gantry.sql.adapters.mysql import _connect_kwargs

    aiomysql = importlib.import_module("aiomysql")
    config = _connect_kwargs({"url": MYSQL})
    config.pop("db", None)
    config["autocommit"] = True
    try:
        return await aiomysql.connect(**config)
    except Exception:
        require_live_or_skip(f"no MySQL at {MYSQL.rsplit('@', 1)[-1]}")
        raise


@pytest.fixture
async def mysql() -> object:
    connection = await _mysql()
    async with connection.cursor() as cursor:  # type: ignore[attr-defined]
        for statement in (
            "DROP TABLE IF EXISTS analytics.write_notes",
            "DROP TABLE IF EXISTS analytics.write_other",
            "CREATE TABLE analytics.write_notes (id INT PRIMARY KEY, body VARCHAR(64))",
            "CREATE TABLE analytics.write_other (id INT PRIMARY KEY, body VARCHAR(64))",
            "INSERT INTO analytics.write_notes VALUES "
            + ", ".join(f"({index}, 'n')" for index in range(1, 51)),
            "INSERT INTO analytics.write_other VALUES (1, 'o'), (2, 'o')",
        ):
            await cursor.execute(statement)
    yield connection
    connection.close()  # type: ignore[attr-defined]


async def _mysql_count(connection: object, where: str = "1 = 1") -> int:
    async with connection.cursor() as cursor:  # type: ignore[attr-defined]
        await cursor.execute(f"SELECT COUNT(*) FROM analytics.write_notes WHERE {where}")
        row = await cursor.fetchone()
    return int(row[0])


async def test_mysql_counts_and_bounds_a_write(mysql: object) -> None:
    db = gantry.sql.connect("mysql", url=MYSQL)
    write = db.write(destinations=["analytics.write_*"], max_rows_affected=10)

    changed = await write("UPDATE analytics.write_notes SET body = 'seen' WHERE id <= 3")
    over = await write("DELETE FROM analytics.write_notes WHERE id <= 40")

    assert changed.status is RunStatus.ACCEPTED, changed.failure
    assert changed.rows_affected == 3
    assert over.status is RunStatus.EXECUTION_FAILED
    assert over.failure is not None
    assert over.failure.kind is FailureKind.ROW_LIMIT_EXCEEDED
    assert "40 rows" in over.failure.message
    assert await _mysql_count(mysql) == 50, "an InnoDB write over its limit is rolled back"
    assert await _mysql_count(mysql, "body = 'seen'") == 3


async def test_mysql_counts_rows_changed_not_rows_matched(mysql: object) -> None:
    """Setting a row to the value it has is not a change, and is not counted."""
    db = gantry.sql.connect("mysql", url=MYSQL)
    write = db.write(destinations=["analytics.write_*"])

    run = await write("UPDATE analytics.write_notes SET body = 'n' WHERE id <= 5")

    assert run.status is RunStatus.ACCEPTED
    assert run.rows_affected == 0


async def test_mysql_refuses_an_update_through_a_join_it_was_not_given(mysql: object) -> None:
    """Either table in the join can be assigned to, so both are write targets."""
    db = gantry.sql.connect("mysql", url=MYSQL)
    write = db.write(destinations=["analytics.write_notes"])

    run = await write(
        "UPDATE analytics.write_notes n JOIN analytics.write_other o ON n.id = o.id "
        "SET o.body = 'changed' WHERE n.id = 1"
    )

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None
    assert "analytics.write_other" in run.failure.message
    _never_ran(run)


# -------------------------------------------------------- counting, for real


async def test_postgres_counts_a_dry_run_and_leaves_the_rows(raw: object) -> None:
    write = _db().write(destinations=["write_scratch.*"], max_rows_affected=10)

    plain = await write.dry_run("DELETE FROM write_scratch.notes WHERE id <= 40")
    returning = await write.dry_run(
        "UPDATE write_scratch.notes SET body = 'x' WHERE id <= 6 RETURNING id"
    )

    assert (plain["would_change"], plain["over_limit"]) == (40, True)
    assert (returning["would_change"], returning["over_limit"]) == (6, False)
    assert await _count(raw, "write_scratch.notes") == 50
    assert await _count(raw, "write_scratch.notes", "body = 'x'") == 0


async def test_postgres_shows_the_rows_a_dry_run_would_change(raw: object) -> None:
    write = _db().write(destinations=["write_scratch.*"], max_rows_affected=10)

    answer = await write.dry_run(
        "UPDATE write_scratch.notes SET body = 'x' WHERE id <= 6", preview=4
    )

    assert answer["would_change"] == 6 and answer["previewed"] is True
    assert answer["columns"] == ["id", "body"]
    assert [row[1] for row in answer["rows"]] == ["x"] * 4  # type: ignore[attr-defined]
    assert answer["preview_truncated"] is True
    assert await _count(raw, "write_scratch.notes", "body = 'x'") == 0


async def test_mysql_counts_a_dry_run_and_leaves_the_rows(mysql: object) -> None:
    db = gantry.sql.connect("mysql", url=MYSQL)
    write = db.write(destinations=["analytics.write_*"], max_rows_affected=10)

    answer = await write.dry_run("DELETE FROM analytics.write_notes WHERE id <= 40")

    assert (answer["would_change"], answer["over_limit"]) == (40, True)
    assert await _mysql_count(mysql) == 50
    previewed = await write.dry_run("DELETE FROM analytics.write_notes WHERE id <= 4", preview=5)
    assert previewed["would_change"] == 4 and previewed["previewed"] is False, (
        "MySQL has no RETURNING: the count, and an honest no"
    )


async def test_postgres_refuses_a_write_it_cannot_plan_and_records_nothing_ran(
    raw: object,
) -> None:
    """Refused by the engine at validation: not run, so no execution is kept."""
    write = _db().write(destinations=["write_scratch.*"])

    run = await write("UPDATE write_scratch.notes SET no_such_column = 1 WHERE id = 1")

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None
    _never_ran(run)


# ------------------------------------------------------- rows as parameters

HOSTILE = "x'); DROP TABLE write_scratch.loaded; --"


async def test_postgres_inserts_rows_as_parameters_with_their_types(raw: object) -> None:
    from datetime import UTC, date, datetime
    from decimal import Decimal

    for statement in (
        "DROP TABLE IF EXISTS write_scratch.loaded",
        'CREATE TABLE write_scratch.loaded (id int PRIMARY KEY, "order" int, body text, '
        "amount numeric(10, 2), seen_at timestamptz, born date, ok boolean, note text)",
    ):
        await raw.execute(statement)  # type: ignore[attr-defined]
    write = _db().write(destinations=["write_scratch.*"], max_rows_affected=10)
    rows = [
        (
            1,
            7,
            HOSTILE,
            Decimal("12.50"),
            datetime(2026, 1, 2, tzinfo=UTC),
            date(1990, 1, 1),
            True,
            None,
        ),
        (
            2,
            8,
            "plain",
            Decimal("0.10"),
            datetime(2026, 1, 3, tzinfo=UTC),
            date(1991, 2, 2),
            False,
            "n",
        ),
    ]

    run = await write.insert_rows(
        "write_scratch.loaded",
        ("id", "order", "body", "amount", "seen_at", "born", "ok", "note"),
        rows,
    )

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert run.rows_affected == 2
    stored = await raw.fetch("SELECT * FROM write_scratch.loaded ORDER BY id")  # type: ignore[attr-defined]
    assert [tuple(row) for row in stored] == rows


async def test_postgres_inserts_nothing_when_a_load_fails_part_way(raw: object) -> None:
    write = _db().write(destinations=["write_scratch.*"], max_rows_affected=10)

    run = await write.insert_rows(
        "write_scratch.archive", ("id", "body"), [(1, "a"), (2, "b"), (1, "again")]
    )

    assert run.status is not RunStatus.ACCEPTED
    assert await _count(raw, "write_scratch.archive") == 0


async def test_postgres_refuses_a_load_into_a_column_that_does_not_exist(raw: object) -> None:
    """Prepared before anything runs, so the engine's answer comes back as a refusal."""
    write = _db().write(destinations=["write_scratch.*"])

    run = await write.insert_rows("write_scratch.archive", ("id", "nope"), [(1, "a")])

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None and "nope" in run.failure.message
    assert await _count(raw, "write_scratch.archive") == 0


async def test_mysql_inserts_rows_as_parameters(mysql: object) -> None:
    async with mysql.cursor() as cursor:  # type: ignore[attr-defined]
        await cursor.execute("DROP TABLE IF EXISTS analytics.write_loaded")
        await cursor.execute(
            "CREATE TABLE analytics.write_loaded (id INT PRIMARY KEY, `order` INT, body TEXT, "
            "amount DECIMAL(10, 2)) ENGINE=InnoDB"
        )
    from decimal import Decimal

    db = gantry.sql.connect("mysql", url=MYSQL)
    write = db.write(destinations=["analytics.write_*"], max_rows_affected=10)
    rows = [(1, 7, HOSTILE, Decimal("12.50")), (2, 8, "plain", Decimal("0.10"))]

    run = await write.insert_rows("analytics.write_loaded", ("id", "order", "body", "amount"), rows)

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert run.rows_affected == 2
    async with mysql.cursor() as cursor:  # type: ignore[attr-defined]
        await cursor.execute("SELECT * FROM analytics.write_loaded ORDER BY id")
        assert [tuple(row) for row in await cursor.fetchall()] == rows

    failed = await write.insert_rows(
        "analytics.write_loaded", ("id", "body"), [(3, "a"), (1, "duplicate")]
    )
    assert failed.status is not RunStatus.ACCEPTED
    async with mysql.cursor() as cursor:  # type: ignore[attr-defined]
        await cursor.execute("SELECT COUNT(*) FROM analytics.write_loaded")
        assert (await cursor.fetchone())[0] == 2, "the failed load left nothing behind"


async def test_postgres_takes_another_engine_s_booleans_and_zoneless_times(raw: object) -> None:
    """MySQL has no boolean and keeps no zone: 1 is true, and a bare time is UTC."""
    from datetime import UTC, datetime

    for statement in (
        "DROP TABLE IF EXISTS write_scratch.converted",
        "CREATE TABLE write_scratch.converted (id int PRIMARY KEY, ok boolean, at timestamptz)",
    ):
        await raw.execute(statement)  # type: ignore[attr-defined]
    write = _db().write(destinations=["write_scratch.*"])

    run = await write.insert_rows(
        "write_scratch.converted",
        ("id", "ok", "at"),
        [(1, 1, datetime(2026, 1, 2, 3, 4)), (2, 0, datetime(2026, 1, 2, 3, 4, tzinfo=UTC))],
    )

    assert run.status is RunStatus.ACCEPTED, run.failure
    stored = await raw.fetch("SELECT ok, at FROM write_scratch.converted ORDER BY id")  # type: ignore[attr-defined]
    assert [row["ok"] for row in stored] == [True, False]
    assert stored[0]["at"] == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
