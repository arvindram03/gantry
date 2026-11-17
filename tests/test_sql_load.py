# SPDX-License-Identifier: Apache-2.0
"""Rows passed in as values, inserted as parameters, against DuckDB.

The claims worth testing are about what lands in the table and what does not:
a value that looks like SQL is stored as text, a load over its limit inserts
nothing, a load that fails part way inserts nothing, and every refusal leaves
the table as it was.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import gantry
import pytest
from gantry import Policy, allow
from gantry.failure import FailureKind
from gantry.runs.model import OperationKind
from gantry.runs.status import RunStatus
from gantry.sql.load import digest, problem

COLUMNS = ("id", "name", "amount", "seen_at", "born", "ok", "note")


@pytest.fixture
def path(tmp_path: Path) -> Path:
    database = tmp_path / "loads.duckdb"
    native = duckdb.connect(str(database))
    native.execute("CREATE SCHEMA scratch")
    native.execute("CREATE SCHEMA private")
    native.execute(
        "CREATE TABLE scratch.people (id INTEGER PRIMARY KEY, name VARCHAR, "
        "amount DECIMAL(10, 2), seen_at TIMESTAMP, born DATE, ok BOOLEAN, note VARCHAR)"
    )
    native.execute("CREATE TABLE private.keys (id INTEGER, secret VARCHAR)")
    native.execute('CREATE TABLE scratch.lines (id INTEGER, "order" INTEGER, "select" VARCHAR)')
    native.close()
    return database


def _rows(path: Path, table: str = "scratch.people") -> list[tuple[object, ...]]:
    native = duckdb.connect(str(path))
    try:
        return native.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    finally:
        native.close()


def _people(count: int, start: int = 1) -> list[tuple[object, ...]]:
    return [
        (
            number,
            f"person {number}",
            Decimal("12.50"),
            datetime(2026, 1, 2, 3, 4, 5),
            date(1990, 1, number % 28 + 1),
            number % 2 == 0,
            None,
        )
        for number in range(start, start + count)
    ]


async def test_rows_go_in_as_given_and_the_run_says_so(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], max_rows_affected=10)
    rows = _people(3)

    run = await write.insert_rows("scratch.people", COLUMNS, rows)

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert run.operation.kind is OperationKind.WRITE
    assert run.rows_affected == 3
    assert run.proposal is not None
    assert run.proposal.body == (
        "INSERT INTO scratch.people (id, name, amount, seen_at, born, ok, note) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    observed = {item.name: item.value for item in run.evidence.observations}  # type: ignore[union-attr]
    assert observed["values_sha256"] == digest(rows)
    stored = _rows(path)
    assert [row[0] for row in stored] == [1, 2, 3]
    assert stored[0][1] == "person 1" and stored[0][2] == Decimal("12.50")


async def test_a_value_that_looks_like_sql_is_only_a_value(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"])
    hostile = "x'); DROP TABLE scratch.people; --"

    run = await write.insert_rows("scratch.people", ("id", "name"), [(1, hostile)])

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert _rows(path)[0][1] == hostile


async def test_more_rows_than_the_limit_insert_nothing(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], max_rows_affected=5)

    run = await write.insert_rows("scratch.people", COLUMNS, _people(6))

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None and run.failure.kind is FailureKind.ROW_LIMIT_EXCEEDED
    assert _rows(path) == []


async def test_a_load_that_fails_part_way_inserts_nothing(path: Path) -> None:
    """One transaction: the duplicate key in the third row undoes the first two."""
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"])

    run = await write.insert_rows("scratch.people", ("id", "name"), [(1, "a"), (2, "b"), (1, "c")])

    assert run.status is not RunStatus.ACCEPTED
    assert _rows(path) == []


@pytest.mark.parametrize(
    ("table", "columns", "rows", "message"),
    [
        ("scratch.people; DROP TABLE x", ("id",), [(1,)], "cannot name a table"),
        ('scratch."people"', ("id",), [(1,)], "cannot name a table"),
        ("a.b.c", ("id",), [(1,)], "cannot name a table"),
        ("scratch.people", ("id", "name) VALUES (1"), [(1, "a")], "cannot name a column"),
        ("scratch.people", ("id", "ID"), [(1, 2)], "named twice"),
        ("scratch.people", ("id", "name"), [(1,)], "1 values for 2 columns"),
        ("scratch.people", ("id",), [], "no rows"),
        ("scratch.people", ("id",), [(object(),)], "cannot be inserted as a value"),
    ],
)
async def test_what_cannot_be_inserted_safely_is_refused(
    path: Path, table: str, columns: tuple[str, ...], rows: list[tuple[object, ...]], message: str
) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"])

    run = await write.insert_rows(table, columns, rows)

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None and message in run.failure.message
    assert _rows(path) == []


async def test_only_named_destinations_are_written(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"])

    run = await write.insert_rows("private.keys", ("id", "secret"), [(1, "x")])

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None and run.failure.kind is FailureKind.DESTINATION_NOT_ALLOWED
    assert _rows(path, "private.keys") == []


async def test_inserts_must_be_among_the_statements_allowed(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], statements=["update", "delete"])

    run = await write.insert_rows("scratch.people", ("id",), [(1,)])

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None and run.failure.kind is FailureKind.OPERATION_NOT_ALLOWED


async def test_a_rule_that_asks_first_refuses_rows_that_cannot_wait(path: Path) -> None:
    policy = Policy(
        "asks",
        [allow.write(destinations=["scratch.*"], require_confirmation=True)],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    write = db.write(destinations=["scratch.*"])

    run = await write.insert_rows("scratch.people", ("id",), [(1,)])

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None and "cannot wait" in run.failure.message
    assert _rows(path) == []


async def test_the_policy_decides_as_it_does_for_a_statement(path: Path) -> None:
    allowed = Policy("writes", [allow.write(destinations=["scratch.*"])])
    db = gantry.sql.connect("duckdb", path=str(path), policy=allowed)
    write = db.write(destinations=["scratch.*"])

    run = await write.insert_rows("scratch.people", ("id",), [(1,)])

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert [row[0] for row in _rows(path)] == [1]


def test_the_digest_is_of_the_values_in_order() -> None:
    assert digest([(1, "a")]) == digest([(1, "a")])
    assert digest([(1, "a")]) != digest([(1, "b")])
    assert digest([(1,), (2,)]) != digest([(2,), (1,)])
    assert digest([(1,)]) != digest([("1",)]), "a number and its text are different values"


def test_a_name_with_a_digit_first_is_not_an_identifier() -> None:
    assert problem("scratch.1people", ("id",), [(1,)]) is not None
    assert problem("people", ("id",), [(1,)]) is None


async def test_a_column_named_with_a_reserved_word_is_quoted(path: Path) -> None:
    """A table copied from elsewhere keeps its names, reserved words included."""
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"])

    run = await write.insert_rows("scratch.lines", ("id", "order", "select"), [(1, 2, "x")])

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert _rows(path, "scratch.lines") == [(1, 2, "x")]
