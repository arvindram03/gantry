# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path

import duckdb
import gantry
import pytest
from gantry import (
    Context,
    Execution,
    ExecutionHandle,
    ExecutionResult,
    ExecutionState,
    Failure,
    FailureKind,
    OutputKind,
    OutputRef,
    ResultStatus,
    ValidationResult,
)
from gantry.sql import (
    Column,
    ConservativeDialect,
    DatabaseSchema,
    ExplainResult,
    InlineRows,
    SQLCapabilities,
    SQLOperation,
    SQLPolicy,
    SQLTarget,
    Table,
)
from gantry.sql.enforcement import policy_errors


class StubSQLAdapter:
    def __init__(self, *, read_only: bool = True) -> None:
        self._capabilities = SQLCapabilities(
            describe_schema=True,
            explain=True,
            cancellation=True,
            read_only_session=read_only,
            statement_timeout=True,
            row_limit=True,
            query_metrics=True,
            result_reference=True,
        )
        self.handle: ExecutionHandle | None = None
        self.policy: SQLPolicy | None = None
        self.submissions = 0

    def capabilities(self) -> SQLCapabilities:
        return self._capabilities

    async def describe(self, target: SQLTarget) -> DatabaseSchema:
        return DatabaseSchema(
            catalogs=("catalog",),
            schemas=("public",),
            tables=(
                Table(
                    "events",
                    "public",
                    "catalog",
                    (Column("id", "integer", False),),
                    primary_key=("id",),
                ),
            ),
        )

    async def validate(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
        policy: SQLPolicy,
    ) -> ValidationResult:
        return ValidationResult.accepted(metadata={"provider": target.provider})

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult:
        return ExplainResult(supported=True, estimated_rows=3)

    async def submit(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
    ) -> ExecutionHandle:
        self.submissions += 1
        value = context.metadata.get("gantry.sql.policy")
        assert isinstance(value, SQLPolicy)
        self.policy = value
        self.handle = ExecutionHandle("sql-run", "sql", target.provider, "native-query")
        return self.handle

    async def status(self, handle: ExecutionHandle) -> Execution:
        return Execution(handle, ExecutionState.SUCCEEDED)

    async def result(self, handle: ExecutionHandle) -> ExecutionResult:
        rows = InlineRows(("id",), ((1,), (2,), (3,)))
        return ExecutionResult.succeeded(
            handle,
            outputs=(
                OutputRef(OutputKind.INLINE, "inline://sql-run", {"inline": rows}),
                OutputRef(OutputKind.TABLE, "warehouse://temporary/sql-run"),
            ),
        )

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, f"cancelled ({mode})"),
        )


def test_builtin_provider_registry_separates_provider_and_dialect() -> None:
    assert {"postgres", "neon", "supabase", "bigquery", "snowflake", "duckdb"} <= set(
        gantry.sql.providers()
    )


def test_custom_provider_uses_public_connection_without_exposing_config() -> None:
    adapter = StubSQLAdapter()
    gantry.sql.register(
        "test-internal",
        adapter=adapter,
        dialect="postgres",
        replace=True,
    )

    db = gantry.sql.connect("test-internal", password="do-not-expose")
    query = db.query(max_rows=2)
    tool = query.tool()

    assert db.provider == "test-internal"
    assert db.dialect == "postgres"
    assert "password" not in tool.input_schema
    assert not hasattr(tool, "config")
    assert tool.input_schema["required"] == ["sql"]


async def test_tool_runs_lifecycle_and_defensively_bounds_inline_rows() -> None:
    adapter = StubSQLAdapter()
    gantry.sql.register("test-tool", adapter=adapter, dialect="postgres", replace=True)
    query = gantry.sql.connect("test-tool", secret="hidden").query(max_rows=2, timeout=4)
    tool = query.tool()

    result = await tool.invoke(sql="SELECT id FROM public.events")

    assert result.status is ResultStatus.ACCEPTED
    assert result.handle is adapter.handle
    assert result.inline == InlineRows(("id",), ((1,), (2,)), truncated=True)
    assert result.outputs[1].uri == "warehouse://temporary/sql-run"
    assert result.uri == "warehouse://temporary/sql-run"
    assert adapter.policy is not None
    assert adapter.policy.max_rows == 2
    assert adapter.policy.timeout_seconds == 4


async def test_submitted_handle_can_be_observed_and_result_recovered() -> None:
    adapter = StubSQLAdapter()
    gantry.sql.register("test-recovery", adapter=adapter, dialect="postgres", replace=True)
    db = gantry.sql.connect("test-recovery")

    handle = await db.submit("SELECT id FROM events")
    execution = await db.status(handle)
    result = await db.wait(handle, poll_interval_seconds=0)

    assert execution.state is ExecutionState.SUCCEEDED
    assert result.status is ResultStatus.ACCEPTED
    assert result.handle == handle
    assert result.outputs[1].kind is OutputKind.TABLE


async def test_read_only_policy_rejects_write_before_submission() -> None:
    adapter = StubSQLAdapter()
    gantry.sql.register("test-policy", adapter=adapter, dialect="postgres", replace=True)
    db = gantry.sql.connect("test-policy")

    result = await db.query()("DELETE FROM public.events")

    assert result.status is ResultStatus.REJECTED
    assert result.failure is not None
    assert "read-only" in result.failure.message
    assert adapter.submissions == 0


async def test_missing_native_read_only_boundary_fails_closed() -> None:
    adapter = StubSQLAdapter(read_only=False)
    gantry.sql.register("test-unscoped", adapter=adapter, dialect="postgres", replace=True)

    result = await gantry.sql.connect("test-unscoped").query()("SELECT 1")

    assert result.status is ResultStatus.REJECTED
    assert result.failure is not None
    assert "read-only session" in result.failure.message
    assert adapter.submissions == 0


async def test_query_tool_is_narrow_and_framework_neutral() -> None:
    adapter = StubSQLAdapter()
    gantry.sql.register("test-operations", adapter=adapter, dialect="postgres", replace=True)
    query = gantry.sql.connect("test-operations").query()
    tool = query.tool(name="query_analytics")

    result = await tool.invoke({"sql": "SELECT 1"})

    assert tool.name == "query_analytics"
    assert tool.input_schema == {
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"],
        "additionalProperties": False,
    }
    assert result.ok
    with pytest.raises(TypeError, match="sql must be a string"):
        await tool.invoke()
    with pytest.raises(ValueError, match="unexpected query tool arguments"):
        await tool.invoke(sql="SELECT 1", policy="untrusted")
    with pytest.raises(TypeError, match="mapping or keywords"):
        await tool.invoke({"sql": "SELECT 1"}, sql="SELECT 2")
    with pytest.raises(ValueError, match="requires read_only=True"):
        gantry.sql.connect("test-operations").query(read_only=False).tool()


def test_conservative_dialect_handles_comments_quotes_ctes_and_multiple_statements() -> None:
    dialect = ConservativeDialect()

    selected = dialect.classify('-- agent query\nSELECT * FROM "Analytics".`Events`')
    writable_cte = dialect.classify(
        "WITH source AS (SELECT id FROM events) "
        "DELETE FROM archive WHERE id IN (SELECT id FROM source)"
    )
    multiple = dialect.classify("SELECT 1; SELECT 2")

    assert selected.operation is SQLOperation.SELECT
    assert selected.read_only
    assert selected.tables[0].qualified_name == "Analytics.Events"
    assert writable_cte.operation is SQLOperation.DELETE
    assert not writable_cte.read_only
    assert multiple.operation is SQLOperation.MULTI_STATEMENT
    assert multiple.statement_count == 2


def test_conservative_dialect_honours_backslash_escapes_only_in_e_strings() -> None:
    """Backslashes are special in `E''` and nowhere else.

    Every expectation here was checked against PostgreSQL 16 with
    `standard_conforming_strings` on, which is the default: a backslash escapes
    the next character inside `E''`, is a literal backslash in an ordinary
    string, and an `E` glued to the end of an identifier is part of that
    identifier rather than a string prefix.
    """
    dialect = ConservativeDialect()

    # The escaped quote does not end the string, so the `;` inside it is not a
    # separator.
    escaped = dialect.classify(
        r"SELECT * FROM analytics.customers WHERE name = E'O\'Brien; DROP TABLE t'"
    )
    assert escaped.operation is SQLOperation.SELECT
    assert escaped.statement_count == 1
    assert escaped.read_only

    # Lower case is the same prefix.
    assert dialect.classify(r"SELECT e'O\'Brien; x' AS c").statement_count == 1

    # Doubling still works inside an E string.
    assert dialect.classify("SELECT E'O''Brien; x' AS c").statement_count == 1

    # In an ordinary string the backslash is literal, so the string ends at the
    # next quote and the `;` after it really does separate two statements.
    plain = dialect.classify(r"SELECT 'a\'; SELECT 2")
    assert plain.operation is SQLOperation.MULTI_STATEMENT
    assert plain.statement_count == 2

    # An escaped backslash ends the E string, so this `;` is a separator too.
    assert dialect.classify(r"SELECT E'a\\'; SELECT 2").statement_count == 2

    # `tableE` is an identifier; the string that follows is an ordinary one.
    assert dialect.classify(r"SELECT * FROM tableE'x\'; DROP TABLE t'").statement_count == 2


def test_policy_normalizes_allow_lists_and_checks_explain_limits() -> None:
    policy = SQLPolicy(
        allowed_schemas=["Analytics"],
        allowed_tables=["Analytics.Events"],
        denied_tables=["Secrets"],
        max_bytes_scanned=100,
        max_cost_usd=0.25,
    )
    capabilities = SQLCapabilities(
        read_only_session=True,
        statement_timeout=True,
        row_limit=True,
        bytes_scanned=True,
        cost_limit=True,
    )
    classification = ConservativeDialect().classify(
        'SELECT * FROM "Analytics"."Events" JOIN Analytics.Secrets USING (id)'
    )
    errors = policy_errors(
        classification,
        policy,
        capabilities,
        ExplainResult(True, estimated_bytes=101, estimated_cost=0.30),
    )

    assert policy.allowed_schemas == frozenset({"analytics"})
    assert "table is denied: analytics.secrets" in errors
    assert any("estimated bytes" in error for error in errors)
    assert any("estimated cost" in error for error in errors)
    with pytest.raises(TypeError, match="collection"):
        SQLPolicy(allowed_tables="events")


async def test_duckdb_provider_discovers_schema_bounds_rows_and_rejects_writes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "agent.duckdb"
    native = duckdb.connect(str(path))
    native.execute("CREATE TABLE events AS SELECT range AS id FROM range(5)")
    native.close()
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True)

    schema = await db.describe()
    query = db.query(max_rows=2)
    result = await query("SELECT id FROM events ORDER BY id")
    rejected = await query("DROP TABLE events")

    assert any(table.name == "events" for table in schema.tables)
    assert result.status is ResultStatus.ACCEPTED
    assert result.inline == InlineRows(("id",), ((0,), (1,)), truncated=True)
    assert result.uri is None
    assert rejected.status is ResultStatus.REJECTED


def test_provider_configuration_is_validated_before_driver_creation() -> None:
    with pytest.raises(ValueError, match="unknown DuckDB"):
        gantry.sql.connect("duckdb", typo=True)
    with pytest.raises(ValueError, match="requires url"):
        gantry.sql.connect("supabase")
    with pytest.raises(ValueError, match="unknown BigQuery"):
        gantry.sql.connect("bigquery", project="acme", typo=True)
    with pytest.raises(ValueError, match="Snowflake provider requires"):
        gantry.sql.connect("snowflake", account="acme")
    with pytest.raises(ValueError, match="unknown SQL provider"):
        gantry.sql.connect("missing")
