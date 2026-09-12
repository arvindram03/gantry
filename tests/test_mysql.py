# SPDX-License-Identifier: Apache-2.0
"""MySQL adapter pieces that do not need a server.

The parts that do — read-only transactions, the timeout, materialization — are
in `test_mysql_live.py`, because they are the parts a stub would get wrong. The
row-count metadata key in this adapter was `row_count` until a real MySQL
reported "destination row count is unavailable"; `gantry.verify.RowCount` reads
`rows`. No unit test would have noticed.
"""

from __future__ import annotations

import pytest
from gantry.execution import ExecutionState
from gantry.failure import FailureKind
from gantry.handle import ExecutionHandle
from gantry.output import OutputKind
from gantry.sql.adapters.mysql import (
    _connect_kwargs,
    _destination_output,
    _failure,
    _quoted,
)
from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import MySQLDialect
from gantry.sql.registry import resolve_dialect, resolve_provider


def test_a_url_becomes_driver_keywords() -> None:
    """aiomysql takes host/user/db, not a URL, so the provider splits one."""
    config = _connect_kwargs({"url": "mysql://user:secret@db.example:3307/analytics"})

    assert config["host"] == "db.example"
    assert config["port"] == 3307
    assert config["user"] == "user"
    assert config["password"] == "secret"
    assert config["db"] == "analytics"
    assert config["autocommit"] is True


def test_url_credentials_are_percent_decoded() -> None:
    """A password with a `@` or `/` in it has to survive the round trip."""
    config = _connect_kwargs({"url": "mysql://user:p%40ss%2Fword@localhost/db"})

    assert config["password"] == "p@ss/word"
    assert config["port"] == 3306


def test_explicit_fields_win_over_the_url_and_extras_pass_through() -> None:
    config = _connect_kwargs(
        {"url": "mysql://user:secret@db.example/analytics", "db": "other", "charset": "utf8mb4"}
    )

    assert config["db"] == "other"
    assert config["charset"] == "utf8mb4"
    assert config["host"] == "db.example"


def test_connecting_without_a_url_keeps_explicit_fields() -> None:
    config = _connect_kwargs({"host": "h", "user": "u", "password": "p", "db": "d"})

    assert config == {
        "host": "h",
        "user": "u",
        "password": "p",
        "db": "d",
        "port": 3306,
        "autocommit": True,
    }


def test_read_only_is_not_passed_to_the_driver() -> None:
    """It is a policy concept; aiomysql would reject the keyword."""
    assert "read_only" not in _connect_kwargs({"host": "h", "read_only": True})


@pytest.mark.parametrize(
    "code,expected",
    [
        (1045, FailureKind.AUTH_ERROR),
        (1146, FailureKind.OBJECT_NOT_FOUND),
        (1064, FailureKind.SYNTAX_ERROR),
        (3024, FailureKind.TIMEOUT),
        (1317, FailureKind.CANCELLED),
        (1050, FailureKind.DESTINATION_EXISTS),
        (1792, FailureKind.POLICY_REJECTED),
        (9999, FailureKind.ENGINE_ERROR),
    ],
)
def test_mysql_error_numbers_map_to_the_portable_taxonomy(code: int, expected: FailureKind) -> None:
    """1792 is "cannot execute statement in a READ ONLY transaction".

    It arrives when something got past the classifier and the read-only
    transaction caught it, so it is a policy refusal rather than an engine
    fault, and a caller branching on the kind should see it that way.
    """
    failure = _failure(Exception(code, "boom"))

    assert failure.kind is expected
    assert failure.native_code == str(code)
    assert failure.retryable is (expected is FailureKind.TIMEOUT)


def test_a_materialization_is_reported_as_a_reference_to_its_destination() -> None:
    """Slashes, not dots: `_materialized_output` matches on the slash form."""
    handle = ExecutionHandle(
        "run_1",
        "sql",
        "mysql",
        "mysql_1",
        metadata={
            "gantry.sql.materialization.destination": "reporting.rollup",
            "gantry.sql.materialization.operation": "CREATE_TABLE_AS",
        },
    )

    outputs = _destination_output(handle)

    assert len(outputs) == 1
    assert outputs[0].kind is OutputKind.TABLE
    assert outputs[0].uri == "mysql://reporting/rollup"
    assert outputs[0].metadata["object_kind"] == "table"


def test_an_ordinary_statement_reports_no_destination() -> None:
    handle = ExecutionHandle("run_1", "sql", "mysql", "mysql_1")

    assert _destination_output(handle) == ()


def test_identifiers_are_backtick_quoted_and_escaped() -> None:
    assert _quoted("rollup") == "`rollup`"
    assert _quoted("we`ird") == "`we``ird`"


def test_the_mysql_provider_is_registered_with_its_own_dialect() -> None:
    provider = resolve_provider("mysql")

    assert provider.dialect == "mysql"
    assert provider.driver == "aiomysql"
    assert isinstance(resolve_dialect("mysql"), MySQLDialect)


def test_the_mysql_provider_validates_config_before_loading_a_driver() -> None:
    provider = resolve_provider("mysql")

    with pytest.raises(ValueError, match="url or host"):
        provider.validate_config({})
    provider.validate_config({"url": "mysql://u:p@h/d"})
    provider.validate_config({"host": "h", "database": "d", "user": "u"})


def test_mysql_lexing_differs_from_postgres_where_mysql_differs() -> None:
    """Both checked against MySQL 8.4 with the default `sql_mode`.

    MySQL escapes backslashes in ordinary strings, so the first case is one
    statement there and two under PostgreSQL's rules. And MySQL has no
    dollar-quoting — `$$` is a syntax error, `$` is an identifier character.
    """
    mysql = MySQLDialect()

    escaped = mysql.classify(r"SELECT 'a\'; SELECT 2'")
    assert escaped.operation is SQLOperation.SELECT
    assert escaped.statement_count == 1

    # Not a quoting construct in MySQL, so the `;` really does separate.
    assert mysql.classify("SELECT $$a; b$$").statement_count == 2
    # `$` inside an identifier still does not open anything.
    assert mysql.classify("SELECT * FROM my$tab$le; DROP TABLE victim").statement_count == 2

    # The guards that are not dialect-specific still apply.
    assert not mysql.classify("SELECT * FROM t FOR UPDATE").read_only
    assert not mysql.classify("SELECT * INTO OUTFILE '/tmp/x' FROM t").read_only


def test_a_missing_driver_names_the_extra_to_install(monkeypatch: pytest.MonkeyPatch) -> None:
    """The import failure a caller sees first should say how to fix it.

    The adapter calls `importlib.import_module`, which answers from
    `sys.modules` when aiomysql is installed, so patching `__import__` proves
    nothing here. Patching the function the adapter actually calls does.
    """
    import importlib

    from gantry.sql.adapters import mysql as adapter_module
    from gantry.sql.target import SQLTarget

    def refuse(name: str, *args: object, **kwargs: object) -> object:
        if name == "aiomysql":
            raise ImportError("no module named aiomysql")
        return importlib.import_module(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(importlib, "import_module", refuse)

    with pytest.raises(ImportError, match=r"data-gantry\[mysql\]"):
        adapter_module.MySQLAdapter(SQLTarget("mysql", "mysql", "aiomysql", {"host": "h"}))


def test_unknown_execution_state_is_reported_rather_than_guessed() -> None:
    from gantry.sql.adapters.mysql import _unknown

    execution = _unknown(ExecutionHandle("run_1", "sql", "mysql", "mysql_1"), "gone")

    assert execution.state is ExecutionState.UNKNOWN
    assert execution.failure is not None
    assert execution.failure.message == "gone"
