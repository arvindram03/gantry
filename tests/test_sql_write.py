# SPDX-License-Identifier: Apache-2.0
"""Governed writes, and the table scanner they rest on.

Two halves. The first is the classifier: a write is admitted on the strength
of which table it changes and which it reads, so what the scanner reports has
to be what the statement does. The second runs the operation end to end
against DuckDB, where a real transaction can be rolled back without any
infrastructure — the property worth testing in a row limit is that the rows
are still there afterwards.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import gantry
import pytest
from gantry import Policy, allow, deny
from gantry.failure import FailureKind
from gantry.policy.errors import PolicyConfigurationError
from gantry.runs.model import OperationKind, Run
from gantry.runs.status import RunStatus
from gantry.sql import ConservativeDialect, MySQLDialect, SQLOperation, WritePolicy

S = SQLOperation


def _names(references: object) -> list[str]:
    return [reference.qualified_name for reference in references]  # type: ignore[attr-defined]


def _shape(sql: str, dialect: ConservativeDialect | None = None) -> tuple[object, ...]:
    """Operation, what is written, what else is read, filtered, returning."""
    found = (dialect or ConservativeDialect()).classify(sql)
    written = _names(found.write_targets)
    read = [name for name in _names(found.tables) if name not in written]
    return found.operation, written, read, found.filtered, found.returning


# ------------------------------------------------------ what a write changes


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "INSERT INTO app.notes (body) VALUES ('x')",
            (S.INSERT, ["app.notes"], [], None, False),
        ),
        (
            "UPDATE app.users SET name = 'x' WHERE id = 1",
            (S.UPDATE, ["app.users"], [], True, False),
        ),
        ("UPDATE app.users SET name = 'x'", (S.UPDATE, ["app.users"], [], False, False)),
        ("DELETE FROM app.users", (S.DELETE, ["app.users"], [], False, False)),
        (
            "DELETE FROM ONLY app.users USING app.banned b, app.audit a "
            "WHERE users.id = b.id RETURNING users.id",
            (S.DELETE, ["app.users"], ["app.banned", "app.audit"], True, True),
        ),
        # The first table a statement names is not always the one it changes.
        # This used to report `a` — the table being read — as the write target.
        (
            "WITH src AS (SELECT * FROM a) UPDATE b SET x = src.x FROM src WHERE b.id = src.id",
            (S.UPDATE, ["b"], ["a"], True, False),
        ),
        # A data-modifying CTE writes too; PostgreSQL runs both.
        (
            "WITH gone AS (DELETE FROM a WHERE id < 5 RETURNING *) "
            "INSERT INTO b SELECT * FROM gone",
            (S.DELETE, ["a", "b"], [], True, True),
        ),
        # `DO UPDATE` is a clause of the insert, not a second statement, and the
        # commas in its SET list are not a FROM list.
        (
            "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET a = 1, b = 3",
            (S.INSERT, ["t"], [], None, False),
        ),
        # The WHERE belongs to the subquery, so the update is not filtered.
        (
            "UPDATE t SET a = (SELECT max(x) FROM s, s2 WHERE s.k = 1), b = 2",
            (S.UPDATE, ["t"], ["s", "s2"], False, False),
        ),
        (
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = s.a, b = 1 "
            "WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
            (S.MERGE, ["t"], ["s"], None, False),
        ),
        ("INSERT INTO t SELECT * FROM s FOR UPDATE", (S.INSERT, ["t"], ["s"], None, False)),
        (
            "INSERT INTO t SELECT * FROM a JOIN b USING (id, k)",
            (S.INSERT, ["t"], ["a", "b"], None, False),
        ),
        (
            "UPDATE t SET d = EXTRACT(YEAR FROM created_at), s = TRIM(BOTH ' ' FROM s) "
            "WHERE id = 1",
            (S.UPDATE, ["t"], [], True, False),
        ),
        ('UPDATE "Order" SET x = 1 WHERE id = 2', (S.UPDATE, ["Order"], [], True, False)),
    ],
)
def test_a_write_reports_what_it_changes_and_what_it_reads(
    sql: str, expected: tuple[object, ...]
) -> None:
    assert _shape(sql) == expected


def test_text_being_written_is_not_read_as_sql() -> None:
    """The commonest write there is, and the one a word scanner refuses.

    An agent writes notes, summaries and messages, and they are full of
    `update`, `from` and `into`. None of that names a table.
    """
    sql = "INSERT INTO notes (body) VALUES ('update from Bob: moved into review; delete from x')"

    assert _shape(sql) == (S.INSERT, ["notes"], [], None, False)


def test_a_word_that_is_also_a_verb_is_not_one_as_a_column() -> None:
    """`insert`, `update` and `delete` are not reserved in PostgreSQL."""
    assert _shape("INSERT INTO t SELECT delete, insert FROM s") == (
        S.INSERT,
        ["t"],
        ["s"],
        None,
        False,
    )


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        # Any table in the join can be the one a SET assigns to.
        (
            "UPDATE t1 JOIN t2 ON t1.id = t2.id SET t2.x = 1 WHERE t1.y = 2",
            (S.UPDATE, ["t1", "t2"], [], True, False),
        ),
        (
            "UPDATE t1, t2 SET t1.a = t2.a WHERE t1.id = t2.id",
            (S.UPDATE, ["t1", "t2"], [], True, False),
        ),
        # A double-quoted span is a string in MySQL's default sql_mode.
        ('INSERT INTO t VALUES ("update from x")', (S.INSERT, ["t"], [], None, False)),
        (
            "INSERT IGNORE INTO t VALUES (1) ON DUPLICATE KEY UPDATE a = 2, b = 3",
            (S.INSERT, ["t"], [], None, False),
        ),
    ],
)
def test_mysql_writes_are_read_by_mysql_s_rules(sql: str, expected: tuple[object, ...]) -> None:
    assert _shape(sql, MySQLDialect()) == expected


def test_a_delete_with_no_table_to_attach_it_to_is_not_classified() -> None:
    """MySQL's multi-table form. Naming no target would authorize a write to nowhere."""
    found = MySQLDialect().classify("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.id")

    assert found.operation is S.UNKNOWN
    assert found.write_targets == ()


# ------------------------------------------------------- what a read touches


@pytest.mark.parametrize(
    ("sql", "tables"),
    [
        # Each of these was admitted under a policy allowing only `analytics.*`
        # and returned the other table's rows. See test_sql_write_live.py.
        (
            "SELECT c.* FROM analytics.orders o, secret.contacts c",
            ["analytics.orders", "secret.contacts"],
        ),
        (
            "SELECT * FROM (secret.contacts CROSS JOIN analytics.orders)",
            ["analytics.orders", "secret.contacts"],
        ),
        ("SELECT * FROM ((a JOIN b ON true) JOIN c ON true), d", ["b", "c", "a", "d"]),
        # A parenthesis inside a string is text. Counted, it would move the
        # depth of everything after it and hide the table past the comma.
        ("SELECT * FROM a JOIN b ON b.x = '(' , secret", ["a", "b", "secret"]),
        ("SELECT * FROM a STRAIGHT_JOIN secret", ["a", "secret"]),
        (
            "SELECT x FROM a WHERE EXISTS (SELECT 1 FROM b, c WHERE b.k IN (1, 2)) ORDER BY x, y",
            ["a", "b", "c"],
        ),
        ("SELECT * FROM sécurité, données", ["sécurité", "données"]),
        ("SELECT * FROM a, LATERAL (SELECT 1) b", ["a"]),
    ],
)
def test_every_table_in_a_from_list_is_named(sql: str, tables: list[str]) -> None:
    assert _names(ConservativeDialect().classify(sql).tables) == tables


def test_the_scanner_only_ever_names_more_tables_than_before() -> None:
    """The structural scan is joined to the old pattern, never substituted for it.

    A statement that names a table only inside a string literal still reports
    it, which is over-refusal and is what this module trades for.
    """
    found = ConservativeDialect().classify("SELECT * FROM t WHERE note = 'copied from secret'")

    assert "secret" in _names(found.tables)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT table_to_xml('secret.contacts', true, false, '')",
        "SELECT query_to_xml('sel' || 'ect * from secret.contacts', true, false, '')",
        "SELECT pg_catalog.query_to_xml('select 1', true, false, '')",
        "SELECT \"query_to_xml\"('select 1', true, false, '')",
        "SELECT * FROM dblink('dbname=x', 'select 1') AS t(a int)",
        "INSERT INTO t SELECT query_to_xml('select * from secret', true, false, '')",
        "SELECT U&\"t\\0061ble_to_xml\"('x', true, false, '')",
    ],
)
def test_a_function_that_reaches_unnamed_tables_is_not_classified(sql: str) -> None:
    """These return a table's rows with no `FROM` for a scanner to find."""
    found = ConservativeDialect().classify(sql)

    assert found.operation is S.UNKNOWN
    assert found.read_only is False
    assert found.read_only_reason, "the refusal has to say which call it was"


def test_a_string_that_mentions_such_a_function_is_only_a_string() -> None:
    found = ConservativeDialect().classify(
        "SELECT * FROM t WHERE note = 'remember to call table_to_xml(x)'"
    )

    assert found.operation is S.SELECT


# ------------------------------------------------------------- configuration


def test_a_write_has_to_say_where() -> None:
    with pytest.raises(ValueError, match="at least one write destination"):
        WritePolicy(destinations=[])
    with pytest.raises(TypeError, match="not a string"):
        WritePolicy(destinations="scratch.*")
    with pytest.raises(ValueError, match="unknown statement kind: truncate"):
        WritePolicy(destinations=["scratch.*"], statements=["insert", "truncate"])
    with pytest.raises(ValueError, match="max_rows_affected must be positive"):
        WritePolicy(destinations=["scratch.*"], max_rows_affected=0)


def test_a_row_limit_belongs_to_a_write() -> None:
    from gantry.sql import SQLPolicy

    with pytest.raises(ValueError, match="cannot be set on a read"):
        SQLPolicy(max_rows_affected=10)
    assert SQLPolicy(read_only=False, max_rows_affected=10).max_rows_affected == 10


def test_an_allow_rule_for_writes_has_to_say_where() -> None:
    with pytest.raises(PolicyConfigurationError, match="must name destinations"):
        allow.write(destinations=[])


def test_write_is_an_operation_a_rule_can_name() -> None:
    rule = allow.write(destinations=["scratch.*"], constraints={"max_rows_affected": 50})

    assert rule.operations == (OperationKind.WRITE,)
    assert rule.constraints == {"max_rows_affected": 50}
    assert gantry.PolicyRule.from_dict(rule.as_dict()) == rule


# ----------------------------------------------------------------- end to end


@pytest.fixture
def path(tmp_path: Path) -> Path:
    database = tmp_path / "writes.duckdb"
    native = duckdb.connect(str(database))
    native.execute("CREATE SCHEMA scratch")
    native.execute("CREATE SCHEMA prod")
    native.execute("CREATE TABLE scratch.notes AS SELECT range AS id, 'n' AS body FROM range(50)")
    native.execute("CREATE TABLE prod.orders AS SELECT range AS id, 10 AS total FROM range(20)")
    native.close()
    return database


def _count(path: Path, table: str, where: str = "true") -> int:
    native = duckdb.connect(str(path))
    try:
        return int(native.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0])  # type: ignore[index]
    finally:
        native.close()


async def test_a_write_changes_rows_and_says_how_many(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"])

    run = await write("UPDATE scratch.notes SET body = 'seen' WHERE id < 3")

    assert run.status is RunStatus.ACCEPTED
    assert run.operation.kind is OperationKind.WRITE
    assert run.rows_affected == 3
    assert [ref.resource for ref in run.outputs] == ["scratch.notes"]
    assert _count(path, "scratch.notes", "body = 'seen'") == 3


async def test_a_write_over_its_limit_changes_nothing(path: Path) -> None:
    """The bound is a ceiling, not a report: the rows are still there."""
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], max_rows_affected=10)

    run = await write("DELETE FROM scratch.notes WHERE id < 40")

    assert run.status is RunStatus.EXECUTION_FAILED
    assert run.failure is not None
    assert run.failure.kind is FailureKind.ROW_LIMIT_EXCEEDED
    assert "40 rows" in run.failure.message
    assert "nothing was changed" in run.failure.message
    assert run.rows_affected is None
    assert run.safe_to_retry is False, "the same statement changes the same rows"
    assert _count(path, "scratch.notes") == 50


async def test_the_limit_holds_for_a_statement_that_returns_its_rows(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], max_rows_affected=10)

    over = await write("DELETE FROM scratch.notes WHERE id < 40 RETURNING id")
    under = await write("DELETE FROM scratch.notes WHERE id < 4 RETURNING id")

    assert over.status is RunStatus.EXECUTION_FAILED
    assert under.status is RunStatus.ACCEPTED
    assert under.rows_affected == 4
    assert {row[0] for row in under.rows} == {0, 1, 2, 3}
    assert _count(path, "scratch.notes") == 46


async def test_what_comes_back_is_bounded_separately_from_what_changed(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], max_rows_affected=10, max_rows=2)

    run = await write("UPDATE scratch.notes SET body = 'x' WHERE id < 5 RETURNING id")

    assert run.rows_affected == 5
    assert len(run.rows) == 2
    assert run.truncated is True


@pytest.mark.parametrize(
    ("sql", "kind", "said"),
    [
        (
            "DELETE FROM scratch.notes",
            FailureKind.OPERATION_NOT_ALLOWED,
            "without a WHERE clause would change every row",
        ),
        (
            "UPDATE prod.orders SET total = 0 WHERE id = 1",
            FailureKind.DESTINATION_NOT_ALLOWED,
            "writing to prod.orders is not allowed",
        ),
        (
            "INSERT INTO scratch.notes SELECT id, 'x' FROM prod.orders",
            FailureKind.SOURCE_NOT_ALLOWED,
            "reading prod.orders is not allowed in a write",
        ),
        (
            "SELECT * FROM scratch.notes",
            FailureKind.OPERATION_NOT_ALLOWED,
            "SELECT is not a row write",
        ),
        ("DROP TABLE scratch.notes", FailureKind.OPERATION_NOT_ALLOWED, "DDL is not a row write"),
        (
            "UPDATE scratch.notes SET body = 'a' WHERE id = 1; "
            "DELETE FROM scratch.notes WHERE id = 2",
            FailureKind.OPERATION_NOT_ALLOWED,
            "a write is one statement",
        ),
        (
            "MERGE INTO scratch.notes USING prod.orders ON false WHEN NOT MATCHED THEN DO NOTHING",
            FailureKind.OPERATION_NOT_ALLOWED,
            "MERGE is not allowed here",
        ),
    ],
)
async def test_what_a_write_will_not_run_is_refused_before_it_runs(
    path: Path, sql: str, kind: FailureKind, said: str
) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"])

    run = await write(sql)

    assert run.status is RunStatus.POLICY_REJECTED, run.failure
    assert run.failure is not None
    assert run.failure.kind is kind
    assert said in run.failure.message
    assert run.execution is None, "a refusal must not have reached the engine"
    assert (_count(path, "scratch.notes"), _count(path, "prod.orders")) == (50, 20)


async def test_an_unfiltered_write_can_be_allowed_on_purpose(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], require_where=False, max_rows_affected=100)

    run = await write("UPDATE scratch.notes SET body = 'all'")

    assert run.status is RunStatus.ACCEPTED
    assert run.rows_affected == 50


async def test_a_source_has_to_be_named_to_be_read(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(destinations=["scratch.*"], sources=["prod.orders"], max_rows_affected=20)

    run = await write("INSERT INTO scratch.notes SELECT id + 100, 'order' FROM prod.orders")

    assert run.status is RunStatus.ACCEPTED, run.failure
    assert run.rows_affected == 20
    assert [ref.resource for ref in run.inputs] == ["prod.orders"]


async def test_a_rule_that_allows_queries_does_not_allow_writes(path: Path) -> None:
    """Write is its own operation, so reading authority is not writing authority."""
    policy = Policy("reads-only", [allow.query(destinations=["scratch.*"])])
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)

    run = await db.write(destinations=["scratch.*"])("DELETE FROM scratch.notes WHERE id = 1")

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.admission is not None
    assert run.admission.codes == ("OPERATION_DENIED",)
    assert run.admission.reasons == ("no rule allows write",)
    assert _count(path, "scratch.notes") == 50


async def test_a_deny_wins_over_what_the_operation_was_configured_to_allow(path: Path) -> None:
    """Both layers have to agree, and neither can widen the other."""
    policy = Policy(
        "no-prod",
        [allow.write(destinations=["*"]), deny.write(destinations=["prod.*"], name="never-prod")],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    write = db.write(destinations=["scratch.*", "prod.*"])

    refused = await write("UPDATE prod.orders SET total = 0 WHERE id = 1")
    allowed = await write("UPDATE scratch.notes SET body = 'x' WHERE id = 1")

    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.admission is not None
    assert refused.admission.matched_rules == ("never-prod",)
    assert allowed.status is RunStatus.ACCEPTED
    assert _count(path, "prod.orders", "total = 0") == 0


async def test_a_rule_can_require_a_tighter_limit_than_the_operation_set(path: Path) -> None:
    """A constraint is a ceiling on what was configured, never a grant."""
    policy = Policy(
        "small-writes",
        [allow.write(destinations=["scratch.*"], constraints={"max_rows_affected": 5})],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)

    loose = await db.write(destinations=["scratch.*"], max_rows_affected=50)(
        "DELETE FROM scratch.notes WHERE id = 1"
    )
    tight = await db.write(destinations=["scratch.*"], max_rows_affected=5)(
        "DELETE FROM scratch.notes WHERE id = 1"
    )

    assert loose.status is RunStatus.POLICY_REJECTED
    assert tight.status is RunStatus.ACCEPTED


async def test_a_write_held_for_confirmation_changes_nothing_until_it_is_confirmed(
    path: Path,
) -> None:
    policy = Policy(
        "ask-first",
        [
            allow.write(
                destinations=["scratch.*"],
                require_confirmation=True,
                confirmation_message="This changes scratch data.",
            )
        ],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)

    parked = await db.write(destinations=["scratch.*"])("DELETE FROM scratch.notes WHERE id < 3")

    assert parked.status is RunStatus.AWAITING_CONFIRMATION
    assert _count(path, "scratch.notes") == 50, "nothing may change while confirmation is pending"

    done = await gantry.runs.confirm(parked.id)

    assert done.id == parked.id
    assert done.status is RunStatus.ACCEPTED
    assert done.rows_affected == 3
    assert _count(path, "scratch.notes") == 47


async def test_a_connection_that_cannot_roll_back_refuses_rather_than_running_unbounded(
    path: Path,
) -> None:
    """A bound is admitted only when the adapter can apply it."""
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True)

    run = await db.write(destinations=["scratch.*"])("DELETE FROM scratch.notes WHERE id = 1")

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.failure is not None
    assert "adapter cannot enforce the affected-row limit" in run.failure.message
    # Counted through the same connection: DuckDB will not open the file again
    # read-write while a read-only handle on it is alive.
    assert (await db.query()("SELECT count(*) FROM scratch.notes")).rows == ((50,),)


async def test_the_tool_hands_a_model_the_count_and_the_limit(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    tool = db.write(destinations=["scratch.*"], max_rows_affected=10).tool()

    assert "At most 10 rows" in tool.description
    assert "WHERE" in tool.description

    done = (
        await tool.invoke(sql="UPDATE scratch.notes SET body = 'x' WHERE id = 7")
    ).as_tool_result()
    over = (await tool.invoke(sql="DELETE FROM scratch.notes WHERE id < 40")).as_tool_result()

    assert (done["status"], done["rows_affected"]) == ("ACCEPTED", 1)
    assert over["status"] == "EXECUTION_FAILED"
    assert "rolled back" in str(over["refused_because"])
    assert over["retry"] == "running this again will not help"
    with pytest.raises(ValueError, match="unexpected write tool arguments"):
        await tool.invoke(sql="DELETE FROM scratch.notes WHERE id = 1", force=True)


async def test_a_stored_write_still_says_how_many_rows_it_changed(path: Path) -> None:
    db = gantry.sql.connect("duckdb", path=str(path))
    run = await db.write(destinations=["scratch.*"])("DELETE FROM scratch.notes WHERE id < 2")

    stored = Run.from_json(run.to_json())

    assert stored.rows_affected == 2
    assert stored.operation.kind is OperationKind.WRITE
    assert "2 rows affected" in stored.render()


async def test_a_read_stays_silent_about_rows_affected(path: Path) -> None:
    """`None`, not zero: a read changed nothing because it is not a write."""
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True)
    run = await db.query()("SELECT id FROM scratch.notes LIMIT 1")

    assert run.rows_affected is None
    assert "rows_affected" not in run.as_tool_result()
    assert "affected" not in (run.as_dict()["result_ref"] or {})  # type: ignore[operator]


# ------------------------------------------------ denied tables, and asking first


async def test_a_denied_table_is_neither_written_nor_read(path: Path) -> None:
    """Denied wins over the patterns that would have allowed it, in both directions."""
    db = gantry.sql.connect("duckdb", path=str(path))
    write = db.write(
        destinations=["scratch.*", "prod.*"],
        sources=["prod.*"],
        denied_tables=["prod.orders"],
    )

    as_target = await write("UPDATE prod.orders SET total = 0 WHERE id = 1")
    as_source = await write("INSERT INTO scratch.notes SELECT id + 500, 'x' FROM prod.orders")

    assert as_target.status is RunStatus.POLICY_REJECTED
    assert as_target.failure is not None
    assert as_target.failure.kind is FailureKind.DESTINATION_NOT_ALLOWED
    assert as_source.status is RunStatus.POLICY_REJECTED
    assert as_source.failure is not None
    assert as_source.failure.kind is FailureKind.SOURCE_NOT_ALLOWED
    assert "prod.orders is denied" in as_source.failure.message
    assert (_count(path, "scratch.notes"), _count(path, "prod.orders", "total = 0")) == (50, 0)


async def test_a_write_can_be_checked_without_being_run(path: Path) -> None:
    """Asked first, nothing changes and nothing is recorded."""
    policy = Policy(
        "scratch-only",
        [allow.write(destinations=["scratch.*"]), deny.write(destinations=["prod.*"], name="np")],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    write = db.write(destinations=["scratch.*", "prod.*"], max_rows_affected=7)

    fine = write.check("DELETE FROM scratch.notes WHERE id < 40")
    by_policy = write.check("UPDATE prod.orders SET total = 0 WHERE id = 1")
    by_bounds = write.check("DELETE FROM scratch.notes")

    assert fine["allowed"] is True
    assert fine["writes"] == ["scratch.notes"]
    assert fine["max_rows_affected"] == 7, "the limit it will be held to when it does run"
    assert by_policy["allowed"] is False
    assert by_policy["rules"] == ["np"]
    assert by_bounds["allowed"] is False
    assert "WHERE" in str(by_bounds["because"])
    assert _count(path, "scratch.notes") == 50


async def test_the_check_tool_answers_a_row_write_as_the_write_it_would_be(path: Path) -> None:
    """A write checked as a query is a different operation and a different answer."""
    from gantry.sql.introspection import check_tool

    policy = Policy("p", [allow.query(), allow.write(destinations=["scratch.*"])])
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    write = db.write(destinations=["scratch.*"])
    sql = "UPDATE scratch.notes SET body = 'x' WHERE id = 1"

    offered = await check_tool(db, write=write).invoke(sql=sql)
    not_offered = await check_tool(db).invoke(sql=sql)
    a_read = await check_tool(db, write=write).invoke(sql="SELECT id FROM scratch.notes")

    assert (offered["operation"], offered["allowed"]) == ("write", True)
    assert not_offered["allowed"] is False
    assert "does not offer a tool that changes rows" in str(not_offered["because"])
    assert a_read["allowed"] is True
    assert "operation" not in a_read, "a read is answered as it always was"
