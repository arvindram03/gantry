# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import duckdb
import gantry
import pytest
from gantry import Context, FailureKind, OutputKind, ResultStatus
from gantry.sql import (
    MaterializationError,
    MaterializationOperation,
    MaterializationPlan,
    MaterializationProposal,
    SQLPolicy,
    SQLTarget,
    TableRef,
    parse_materialization,
)
from gantry.sql.adapters.bigquery import BigQueryAdapter


def _database(tmp_path: Path) -> gantry.sql.SQLConnection:
    path = tmp_path / "materialization.duckdb"
    native = duckdb.connect(str(path))
    native.execute("CREATE SCHEMA raw")
    native.execute("CREATE SCHEMA agent_scratch")
    native.execute(
        "CREATE TABLE raw.invoices(customer_id INTEGER, balance INTEGER, status VARCHAR)"
    )
    native.execute(
        "INSERT INTO raw.invoices VALUES (1, 10, 'unpaid'), (1, 20, 'unpaid'), (2, 5, 'paid')"
    )
    native.close()
    return gantry.sql.connect("duckdb", path=str(path))


def _sql(destination: str = "agent_scratch.high_risk_customers") -> str:
    return f"""
        CREATE TABLE {destination} AS
        SELECT customer_id, SUM(balance) AS outstanding_balance
        FROM raw.invoices
        WHERE status = 'unpaid'
        GROUP BY customer_id
    """


def test_materialization_parser_extracts_native_plan_and_cte_sources() -> None:
    plan = parse_materialization(
        """
        CREATE TABLE `acme.agent_scratch.high_risk` AS
        WITH unpaid AS (
            SELECT customer_id, balance FROM `acme.raw.invoices`
        )
        SELECT customer_id, SUM(balance) AS balance
        FROM unpaid
        GROUP BY customer_id
        """
    )

    assert plan.operation is MaterializationOperation.CREATE_TABLE_AS
    assert plan.destination == TableRef("high_risk", "agent_scratch", "acme")
    assert plan.sources == (TableRef("invoices", "raw", "acme"),)
    assert not plan.replace


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("CREATE OR REPLACE TABLE scratch.out AS SELECT 1", "replacement"),
        ("CREATE TABLE IF NOT EXISTS scratch.out AS SELECT 1", "IF NOT EXISTS"),
        ("CREATE TABLE scratch.out (id INTEGER)", "CREATE TABLE AS"),
        ("INSERT INTO scratch.out SELECT 1", "only CREATE"),
        ("CREATE TABLE scratch.out AS SELECT * FROM raw.a, raw.b", "explicit JOIN"),
        ("CREATE TABLE scratch.out AS SELECT * FROM read_csv('data.csv')", "table-valued"),
        ("CREATE TABLE out AS SELECT 1", "schema or dataset"),
    ],
)
def test_materialization_parser_fails_closed(sql: str, message: str) -> None:
    with pytest.raises(MaterializationError, match=message):
        parse_materialization(sql)


async def test_duckdb_materialization_returns_reference_and_verifies_destination(
    tmp_path: Path,
) -> None:
    db = _database(tmp_path)
    materialize = db.materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
        verify=[
            gantry.verify.destination_exists(),
            gantry.verify.row_count(min=1),
            gantry.verify.required_columns(["customer_id", "outstanding_balance"]),
        ],
    )

    result = await materialize(_sql())

    assert result.status is ResultStatus.ACCEPTED
    assert result.output is not None
    assert result.output.kind is OutputKind.TABLE
    assert result.output.uri == "duckdb://agent_scratch/high_risk_customers"
    assert result.uri == "duckdb://agent_scratch/high_risk_customers"
    assert result.execution is not None
    assert result.execution.state.value == "SUCCEEDED"
    assert result.verification is not None
    assert result.verification.ok
    assert all(output.kind is not OutputKind.INLINE for output in (result.output,))


async def test_materialization_enforces_source_destination_and_create_only_policy(
    tmp_path: Path,
) -> None:
    db = _database(tmp_path)
    materialize = db.materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
    )

    source_rejected = await materialize(
        "CREATE TABLE agent_scratch.denied AS SELECT * FROM main.secret"
    )
    destination_rejected = await materialize(
        "CREATE TABLE main.denied AS SELECT * FROM raw.invoices"
    )
    accepted = await materialize(_sql())
    existing = await materialize(_sql())

    assert source_rejected.status is ResultStatus.REJECTED
    assert source_rejected.failure is not None
    assert source_rejected.failure.kind is FailureKind.SOURCE_NOT_ALLOWED
    assert source_rejected.uri is None
    assert destination_rejected.status is ResultStatus.REJECTED
    assert destination_rejected.failure is not None
    assert destination_rejected.failure.kind is FailureKind.DESTINATION_NOT_ALLOWED
    assert accepted.ok
    assert existing.status is ResultStatus.REJECTED
    assert existing.failure is not None
    assert existing.failure.kind is FailureKind.DESTINATION_EXISTS


async def test_verification_failure_reports_existing_output_without_cleanup(tmp_path: Path) -> None:
    db = _database(tmp_path)
    materialize = db.materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
        verify=[gantry.verify.row_count(min=2)],
    )

    result = await materialize(_sql("agent_scratch.too_small"))

    assert result.status is ResultStatus.VERIFICATION_FAILED
    assert result.output is not None
    assert result.failure is not None
    assert result.failure.kind is FailureKind.VERIFICATION_FAILED
    assert result.verification is not None
    assert result.verification.checks[0].actual == 1


async def test_duckdb_adapter_optionally_materializes_a_view(tmp_path: Path) -> None:
    db = _database(tmp_path)
    materialize = db.materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
        verify=[gantry.verify.row_count(min=3)],
    )

    result = await materialize(
        "CREATE VIEW agent_scratch.invoice_view AS SELECT * FROM raw.invoices"
    )

    assert result.ok
    assert result.output is not None
    assert result.output.metadata["object_kind"] == "view"
    assert result.verification is not None
    assert result.verification.checks[0].actual == 3


async def test_submitted_materialization_can_be_observed_by_a_recreated_materializer(
    tmp_path: Path,
) -> None:
    db = _database(tmp_path)
    first = db.materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
    )
    handle = await first.submit(_sql("agent_scratch.recovered"))
    recreated = db.materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
        verify=[gantry.verify.destination_exists()],
    )

    execution = await recreated.status(handle)
    result = await recreated.wait(handle, poll_interval_seconds=0)

    assert execution.handle == handle
    assert result.ok
    assert result.handle == handle
    assert result.output is not None


def test_materializer_configuration_is_trusted_and_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "read-only.duckdb"
    duckdb.connect(str(path)).close()
    read_only = gantry.sql.connect(
        "duckdb",
        path=str(path),
        read_only=True,
    )
    materialize = read_only.materialize(destinations=["scratch.*"])

    assert not materialize.capabilities.create_table_as
    with pytest.raises(ValueError, match="create_only=True"):
        read_only.materialize(destinations=["scratch.*"], create_only=False)
    with pytest.raises(ValueError, match="destination"):
        read_only.materialize(destinations=[])

    writable = _database(tmp_path)
    assert not writable.capabilities().result_reference
    assert writable.materialize(destinations=["agent_scratch.*"]).capabilities.result_reference


async def test_read_only_adapter_rejects_materialization_before_submission(tmp_path: Path) -> None:
    path = tmp_path / "read-only-rejection.duckdb"
    native = duckdb.connect(str(path))
    native.execute("CREATE SCHEMA scratch")
    native.close()
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True)

    result = await db.materialize(destinations=["scratch.*"])(
        "CREATE TABLE scratch.result AS SELECT 1 AS id"
    )

    assert result.status is ResultStatus.REJECTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.OPERATION_NOT_ALLOWED


async def test_materializer_normalizes_invalid_sql_and_unsupported_limits(tmp_path: Path) -> None:
    db = _database(tmp_path)
    materialize = db.materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
        max_bytes_scanned=1,
    )

    invalid = await materialize("DELETE FROM raw.invoices")
    unsupported = await materialize(_sql("agent_scratch.expensive"))

    assert invalid.status is ResultStatus.REJECTED
    assert invalid.failure is not None
    assert invalid.failure.kind is FailureKind.OPERATION_NOT_ALLOWED
    assert unsupported.status is ResultStatus.REJECTED
    assert unsupported.failure is not None
    assert unsupported.failure.kind is FailureKind.UNSUPPORTED_POLICY_REQUIREMENT
    assert materialize.input_schema["required"] == ["sql"]


async def test_materialization_tool_exposes_only_native_sql(tmp_path: Path) -> None:
    materialize = _database(tmp_path).materialize(
        sources=["raw.*"],
        destinations=["agent_scratch.*"],
    )
    tool = materialize.tool()

    result = await tool.invoke(sql=_sql("agent_scratch.from_tool"))

    assert tool.name == "materialize_sql"
    schema = cast(dict[str, Any], tool.input_schema)
    assert schema["required"] == ["sql"]
    assert set(schema["properties"]) == {"sql", "verify"}
    variants = schema["properties"]["verify"]["items"]["oneOf"]
    assert {item["properties"]["type"]["const"] for item in variants} == {
        "destination_exists",
        "not_empty",
        "required_columns",
        "row_count",
    }
    assert result.ok
    with pytest.raises(ValueError, match="unexpected materialization tool arguments"):
        await tool.invoke(sql=_sql("agent_scratch.denied"), destinations=["main.*"])


class _RecoveringBigQueryClient:
    def __init__(self) -> None:
        self.job_id: str | None = None
        self.job = SimpleNamespace(job_id="", state="RUNNING")

    def query(self, sql: str, **kwargs: object) -> object:
        self.job_id = cast(str, kwargs["job_id"])
        self.job.job_id = self.job_id
        raise TimeoutError("submission response was lost")

    def get_job(self, job_id: str, **kwargs: object) -> object:
        assert job_id == self.job_id
        return self.job


async def test_bigquery_submission_recovers_the_preallocated_native_job_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _RecoveringBigQueryClient()
    module = ModuleType("google.cloud.bigquery")
    module.__dict__["QueryJobConfig"] = lambda **kwargs: kwargs
    module.__dict__["Client"] = lambda **kwargs: client
    monkeypatch.setattr(
        "gantry.sql.adapters.bigquery.importlib.import_module",
        lambda name: module,
    )
    target = SQLTarget("bigquery", "bigquery", "bigquery", {"project": "acme"})
    adapter = BigQueryAdapter(target)
    plan = MaterializationPlan(
        MaterializationOperation.CREATE_TABLE_AS,
        (),
        TableRef("result", "scratch", "acme"),
    )

    handle = await adapter.submit(
        MaterializationProposal("CREATE TABLE scratch.result AS SELECT 1").sql,
        target,
        Context(
            metadata={
                "gantry.sql.policy": SQLPolicy(read_only=False),
                "gantry.sql.materialization.plan": plan,
            }
        ),
    )

    assert handle.native_id == client.job_id
    assert handle.native_id.startswith("gantry_")
    assert handle.metadata["gantry.sql.materialization.destination"] == "acme.scratch.result"
