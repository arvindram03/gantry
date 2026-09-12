# SPDX-License-Identifier: Apache-2.0
"""An adversarial corpus for `ConservativeDialect.classify`.

`classify` decides `read_only`, and `policy_errors` gates the read-only policy
on that answer. It is a scanner by design — the module is explicit that it is
"conservative SQL structure used for policy checks, never transpilation" — so
the question is not whether it can be fooled in principle, but whether the ways
it can be fooled all fail closed.

The ground rule, from #9: every case asserts an outcome, and anything that is
not a correct classification must land on `UNKNOWN`, `MULTI_STATEMENT`, or a
non-read-only operation. **A write classified `read_only=True` is a security
bug**, because it is the one direction the rest of the stack cannot recover
from.

`writes` in the table below is ground truth about the engine, not about Gantry.
Every entry marked `PG16` was executed against PostgreSQL 16 to confirm it:
`SELECT ... INTO` really does create a table, `SELECT ... FOR UPDATE` and
`nextval()` really are refused by a read-only transaction, and
`SELECT 1 IN/**/TO x` really is a syntax error rather than a disguised write.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import ConservativeDialect


@dataclass(frozen=True, slots=True)
class Case:
    label: str
    sql: str
    writes: bool
    """Whether the engine would modify data, schema, or locks. Ground truth."""
    operation: SQLOperation
    statements: int


S = SQLOperation

CORPUS: tuple[Case, ...] = (
    # --- comment obfuscation ------------------------------------------------
    Case("leading block comment", "/*x*/SELECT 1", False, S.SELECT, 1),
    Case("leading line comment", "--x\nSELECT 1", False, S.SELECT, 1),
    Case("comment stack before keyword", "-- a\n /* b */ -- c\n SELECT 1", False, S.SELECT, 1),
    Case("comment hides a DELETE", "--x\nDELETE FROM t", True, S.DELETE, 1),
    Case("comment between keywords", "SELECT/**/1", False, S.SELECT, 1),
    # A comment is whitespace, so this splits the keyword into two tokens and is
    # not a DELETE at all. PG16: syntax error.
    Case("comment splits a keyword", "DEL/**/ETE FROM t", False, S.UNKNOWN, 1),
    # Ours ends the comment at the first `*/`; PostgreSQL nests and swallows the
    # lot. Seeing more code than the engine does is the safe direction.
    Case(
        "nested block comment",
        "SELECT 1 /* /* */ ; DROP TABLE victim */",
        False,
        S.MULTI_STATEMENT,
        2,
    ),
    Case("unterminated block comment", "SELECT 1 /* ; DROP TABLE t", False, S.SELECT, 1),
    # --- CTEs that write ----------------------------------------------------
    Case("CTE deletes", "WITH t AS (DELETE FROM x RETURNING *) SELECT * FROM t", True, S.DELETE, 1),
    Case(
        "CTE inserts",
        "WITH t AS (INSERT INTO x VALUES (1) RETURNING *) SELECT * FROM t",
        True,
        S.INSERT,
        1,
    ),
    Case(
        "CTE updates", "WITH t AS (UPDATE x SET a=1 RETURNING *) SELECT * FROM t", True, S.UPDATE, 1
    ),
    Case(
        "recursive CTE reads", "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t", False, S.SELECT, 1
    ),
    # --- SELECT that writes -------------------------------------------------
    # PG16: creates a table, and is refused by a read-only transaction.
    Case("SELECT INTO", "SELECT * INTO new_table FROM orders", True, S.DDL, 1),
    Case("SELECT INTO mixed case", "SeLeCt 1 InTo x FROM t", True, S.DDL, 1),
    Case("SELECT INTO across newlines", "SELECT 1\n INTO\n x FROM t", True, S.DDL, 1),
    Case("CTE then SELECT INTO", "WITH t AS (SELECT 1) SELECT 1 INTO x FROM t", True, S.DDL, 1),
    Case("MySQL SELECT INTO OUTFILE", "SELECT * FROM t INTO OUTFILE '/tmp/x'", True, S.DDL, 1),
    # PG16: "cannot execute SELECT FOR UPDATE in a read-only transaction".
    Case("FOR UPDATE", "SELECT * FROM t FOR UPDATE", True, S.SELECT, 1),
    Case("FOR NO KEY UPDATE", "SELECT * FROM t FOR NO KEY UPDATE", True, S.SELECT, 1),
    Case("FOR SHARE", "SELECT * FROM t FOR SHARE", True, S.SELECT, 1),
    Case("FOR KEY SHARE", "SELECT * FROM t FOR KEY SHARE", True, S.SELECT, 1),
    Case("for update lower case", "select * from t for update", True, S.SELECT, 1),
    # A comment is whitespace here, so this remains a locking clause. PG16 agrees.
    Case("FOR UPDATE split by comment", "SELECT * FROM t FOR/**/UPDATE", True, S.SELECT, 1),
    Case("FOR UPDATE SKIP LOCKED", "SELECT * FROM t FOR UPDATE SKIP LOCKED", True, S.SELECT, 1),
    # PG16: "cannot execute nextval() in a read-only transaction".
    Case("nextval", "SELECT nextval('s')", True, S.SELECT, 1),
    Case("setval", "SELECT setval('s', 1)", True, S.SELECT, 1),
    # PG16 permits this in a read-only transaction, so it is not a write —
    # an earlier draft of the writing-function list had it wrong.
    Case("advisory lock", "SELECT pg_advisory_lock(1)", False, S.SELECT, 1),
    Case("schema-qualified nextval", "SELECT pg_catalog.nextval('s')", True, S.SELECT, 1),
    # The one write a read-only transaction does NOT stop: the effect lands on
    # another server. Measured — the inserted row survived the ROLLBACK.
    Case("dblink_exec", "SELECT dblink_exec('dbname=x', 'UPDATE t SET a = 1')", True, S.SELECT, 1),
    # A read-only session cannot supply the function it would need.
    Case(
        "CREATE FUNCTION",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        True,
        S.DDL,
        1,
    ),
    Case("CREATE TABLE AS SELECT", "CREATE TABLE x AS SELECT * FROM orders", True, S.DDL, 1),
    Case("INSERT RETURNING", "INSERT INTO x VALUES (1) RETURNING *", True, S.INSERT, 1),
    # --- quoting ------------------------------------------------------------
    Case("dollar-quoted body", "SELECT $$a; b$$", False, S.SELECT, 1),
    Case("tagged dollar quote", "SELECT $tag$a; b$tag$", False, S.SELECT, 1),
    Case(
        "dollar inside an identifier",
        "SELECT * FROM my$tab$le; DROP TABLE victim",
        True,
        S.MULTI_STATEMENT,
        2,
    ),
    Case("E-string escape", r"SELECT E'O\'Brien; DROP TABLE t'", False, S.SELECT, 1),
    Case("ordinary string keeps backslash", r"SELECT 'a\'; SELECT 2", False, S.MULTI_STATEMENT, 2),
    Case("unicode string literal", r"SELECT U&'\0044; DROP TABLE victim'", False, S.SELECT, 1),
    Case("doubled quotes in identifier", 'SELECT * FROM "quo""ted"', False, S.SELECT, 1),
    Case("semicolon in quoted identifier", 'SELECT * FROM "a;b"', False, S.SELECT, 1),
    Case("semicolon in backtick identifier", "SELECT * FROM `a;b`", False, S.SELECT, 1),
    # Brackets are not string delimiters here, so this over-splits. Safe direction.
    Case("semicolon in bracket identifier", "SELECT * FROM [a;b]", False, S.MULTI_STATEMENT, 2),
    # --- unicode and whitespace ---------------------------------------------
    Case("non-breaking space", "SELECT * FROM t", False, S.SELECT, 1),
    Case("vertical tab", "SELECT\x0b*\x0bFROM\x0bt", False, S.SELECT, 1),
    Case("zero-width space in keyword", "SEL​ECT 1", False, S.UNKNOWN, 1),
    Case("zero-width space in DELETE", "DEL​ETE FROM t", False, S.UNKNOWN, 1),
    Case("fullwidth keyword", "ＳELECT 1", False, S.UNKNOWN, 1),
    Case("null byte before a semicolon", "SELECT 1\x00; DROP TABLE t", True, S.MULTI_STATEMENT, 2),
    # --- procedural and executing forms -------------------------------------
    Case("DO block", "DO $$ BEGIN DELETE FROM t; END $$", True, S.UNKNOWN, 1),
    Case("CALL", "CALL do_something()", True, S.UNKNOWN, 1),
    # EXPLAIN ANALYZE executes the statement it explains.
    Case("EXPLAIN ANALYZE DELETE", "EXPLAIN ANALYZE DELETE FROM t", True, S.UNKNOWN, 1),
    Case("EXPLAIN SELECT", "EXPLAIN SELECT 1", False, S.UNKNOWN, 1),
    Case("COPY FROM", "COPY t FROM '/tmp/x'", True, S.UNKNOWN, 1),
    Case("TRUNCATE", "TRUNCATE t", True, S.DDL, 1),
    Case("REFRESH MATERIALIZED VIEW", "REFRESH MATERIALIZED VIEW mv", True, S.UNKNOWN, 1),
    # Session configuration, not a data write: PG16 allows it read-only.
    Case("SET", "SET work_mem = '1GB'", False, S.UNKNOWN, 1),
    # --- genuinely unknown, and must stay unknown ---------------------------
    Case("TABLE shorthand", "TABLE orders", False, S.UNKNOWN, 1),
    Case("VALUES", "VALUES (1)", False, S.UNKNOWN, 1),
    Case("BEGIN", "BEGIN", False, S.UNKNOWN, 1),
    Case("empty string", "", False, S.UNKNOWN, 0),
    Case("comment only", "-- nothing", False, S.UNKNOWN, 1),
    Case("gibberish", "%%%", False, S.UNKNOWN, 1),
    # --- reads that must stay usable ----------------------------------------
    Case("plain select", "SELECT * FROM orders", False, S.SELECT, 1),
    Case("join", "SELECT * FROM a JOIN b ON a.id = b.id", False, S.SELECT, 1),
    Case("aggregate", "SELECT count(*) FROM orders", False, S.SELECT, 1),
    Case("window function", "SELECT row_number() OVER () FROM t", False, S.SELECT, 1),
    Case("scalar subquery", "SELECT (SELECT max(id) FROM t) AS m", False, S.SELECT, 1),
    Case("union", "SELECT 1 UNION SELECT 2", False, S.SELECT, 1),
    Case("lateral join", "SELECT * FROM a, LATERAL (SELECT 1) b", False, S.SELECT, 1),
    Case("column named into_total", "SELECT into_total FROM t", False, S.SELECT, 1),
    Case("trailing semicolon", "SELECT 1;", False, S.SELECT, 1),
)

IDS = [case.label for case in CORPUS]


@pytest.mark.parametrize("case", CORPUS, ids=IDS)
def test_no_write_is_ever_classified_read_only(case: Case) -> None:
    """The security property. Everything else in this file is a regression test.

    If this fails for a new case, the classifier is telling `policy_errors` that
    a write is a read, and a read-only policy will admit it. That is the one
    failure this component exists to prevent.
    """
    classification = ConservativeDialect().classify(case.sql)

    if case.writes:
        assert not classification.read_only, (
            f"{case.label!r} modifies the database but classified read-only as "
            f"{classification.operation.value}"
        )


@pytest.mark.parametrize("case", CORPUS, ids=IDS)
def test_the_corpus_classifies_as_recorded(case: Case) -> None:
    """Pins operation and statement count so a change here has to be deliberate.

    Several entries record deliberate over-refusal — a bracket-quoted semicolon
    splits, a nested comment splits — because moving those to a more permissive
    answer is a decision worth making on purpose, not by accident.
    """
    classification = ConservativeDialect().classify(case.sql)

    assert classification.operation is case.operation, case.label
    assert classification.statement_count == case.statements, case.label


READS = tuple(case for case in CORPUS if not case.writes and case.operation is S.SELECT)


@pytest.mark.parametrize("case", READS, ids=[case.label for case in READS])
def test_ordinary_reads_stay_read_only(case: Case) -> None:
    """Failing closed everywhere is easy and useless. Reads still have to pass."""
    assert ConservativeDialect().classify(case.sql).read_only, case.label


PATHOLOGICAL: tuple[tuple[str, str], ...] = (
    ("leading comments", "/*x*/" * 20_000 + "SELECT 1"),
    ("line comments", "--x\n" * 50_000 + "SELECT 1"),
    ("whitespace", " " * 500_000 + "SELECT 1"),
    ("unterminated comment", "/*" + "a" * 500_000),
    ("unterminated quote", "'" + "a" * 500_000),
    ("unterminated dollar quote", "$$" + "a" * 500_000),
    ("many dollar quotes", "SELECT " + "$$a$$," * 20_000 + "1"),
    ("many semicolons", ";" * 200_000),
    ("deep parentheses", "SELECT " + "(" * 20_000 + "1" + ")" * 20_000),
    ("nested function calls", "SELECT " + "f(" * 10_000 + "1" + ")" * 10_000),
    ("very long identifier", "SELECT * FROM " + "a" * 500_000),
    ("alternating comment and quote", "/*'*/" * 20_000 + "SELECT 1"),
)


@pytest.mark.parametrize("label,sql", PATHOLOGICAL, ids=[label for label, _ in PATHOLOGICAL])
def test_pathological_input_neither_hangs_nor_raises(label: str, sql: str) -> None:
    """The classifier runs on caller-supplied text, so it is an attack surface.

    Several of these regexes nest quantifiers, which is where catastrophic
    backtracking lives. Measured at half a megabyte of input, the slowest of
    these takes tens of milliseconds; a second is three orders of magnitude of
    headroom and still fails the test long before a request times out.
    """
    import time

    started = time.perf_counter()
    classification = ConservativeDialect().classify(sql)
    elapsed = time.perf_counter() - started

    assert elapsed < 1.0, f"{label} took {elapsed:.2f}s"
    # Whatever it decides, an unterminated construct must not become a read.
    if "unterminated" in label:
        assert not classification.read_only


def test_a_writing_function_is_the_boundary_and_the_session_is_the_control() -> None:
    """The known limit of a scanner, and the things that narrow it.

    `SELECT some_function()` can write, and no amount of scanning reveals that —
    the body lives in the database, not in the text. But three things have to
    line up before that is exploitable, and measuring them against PostgreSQL 16
    made the gap much smaller than it first looks.

    The function has to exist already. A read-only session cannot create one:
    `CREATE FUNCTION` is DDL to the classifier, and the read-only transaction
    refuses it too, so an agent cannot supply its own.

    Its writes have to escape the transaction, and ordinary ones do not. A
    function that inserts is refused inside a read-only transaction — including
    a `SECURITY DEFINER` one, which was the case worth checking, since privilege
    buys no way past it.

    What escapes is a function whose effect lands outside PostgreSQL's
    transaction. `dblink_exec` writing to another server is the demonstrated
    case: the read-only transaction permits the call and the inserted row
    survives the rollback. That is why it is in `_WRITING_FUNCTIONS` — the one
    member the session cannot also catch — and why that list is a floor rather
    than a boundary.
    """
    from gantry.sql.capabilities import SQLCapabilities
    from gantry.sql.enforcement import policy_errors
    from gantry.sql.policy import SQLPolicy

    classification = ConservativeDialect().classify("SELECT writes_a_row()")
    assert classification.read_only, "the scanner cannot see into a function body"

    # An adapter that cannot hold a read-only session may not take the policy at
    # all, so the classifier's answer is never the only thing standing there.
    unable = SQLCapabilities(read_only_session=False, row_limit=True, statement_timeout=True)
    errors = policy_errors(classification, SQLPolicy(read_only=True), unable)
    assert "adapter cannot enforce a read-only session" in errors

    able = SQLCapabilities(read_only_session=True, row_limit=True, statement_timeout=True)
    assert policy_errors(classification, SQLPolicy(read_only=True), able) == ()


def test_a_select_that_is_not_a_read_says_why() -> None:
    """A refusal has to be readable by whoever has to act on it.

    "SELECT is not allowed by read-only policy" looks like a bug in Gantry. The
    reason turns it into something an operator can fix: the clause or the
    function that made a SELECT into a write.
    """
    from gantry.sql.capabilities import SQLCapabilities
    from gantry.sql.enforcement import policy_errors
    from gantry.sql.policy import SQLPolicy

    dialect = ConservativeDialect()
    capable = SQLCapabilities(read_only_session=True, row_limit=True, statement_timeout=True)
    read_only = SQLPolicy(read_only=True)

    locking = dialect.classify("SELECT * FROM orders FOR UPDATE")
    assert locking.read_only_reason == (
        "the statement takes row locks with a FOR UPDATE or FOR SHARE clause"
    )
    assert "FOR UPDATE" in " ".join(policy_errors(locking, read_only, capable))

    writing = dialect.classify("SELECT nextval('s')")
    assert writing.read_only_reason is not None
    assert "nextval" in writing.read_only_reason
    assert "nextval" in " ".join(policy_errors(writing, read_only, capable))

    # An ordinary read carries no reason, and a plain write does not need one:
    # "DELETE is not allowed by read-only policy" already says everything.
    assert dialect.classify("SELECT 1").read_only_reason is None
    assert dialect.classify("DELETE FROM t").read_only_reason is None
