# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import gantry
import pytest
from gantry import ExecutionState, FailureKind
from gantry.batch import BatchCapabilities, BatchConnection
from gantry.flink.artifact import FlinkMode, FlinkSQLArtifact
from gantry.flink.client import HTTPResponse
from gantry.flink.operation import FlinkJobError, FlinkJobStatement
from gantry.flink.target import FlinkTarget
from gantry.runs.status import RunStatus
from gantry.stream import StreamCapabilities, StreamConnection


@dataclass(frozen=True, slots=True)
class RecordedRequest:
    method: str
    url: str
    headers: Mapping[str, str]
    body: Mapping[str, object] | None
    timeout_seconds: float


class FakeFlinkTransport:
    def __init__(self) -> None:
        self.calls: list[RecordedRequest] = []
        self.statements: dict[str, str] = {}
        self.states: list[str] = ["RUNNING"]
        self.failure = "Kafka connector could not connect to broker"
        self.fail_explain = False
        self.cancelled = False
        self.row_count = 3
        self.restart_count = 1
        self._operation = 0
        self._watermark = datetime.now(UTC).timestamp() * 1000 - 5_000

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Mapping[str, object] | None,
        timeout_seconds: float,
    ) -> HTTPResponse:
        self.calls.append(RecordedRequest(method, url, dict(headers), body, timeout_seconds))
        path = url.split("?", 1)[0]
        if method == "POST" and path.endswith("/v2/sessions"):
            return HTTPResponse(200, {"sessionHandle": "session-1"})
        if method == "POST" and path.endswith("/statements"):
            self._operation += 1
            operation = f"operation-{self._operation}"
            assert body is not None and isinstance(body.get("statement"), str)
            self.statements[operation] = str(body["statement"])
            return HTTPResponse(200, {"operationHandle": operation})
        if method == "GET" and path.endswith("/status"):
            return HTTPResponse(200, {"status": "FINISHED"})
        if method == "GET" and "/result/" in path:
            operation = path.split("/operations/", 1)[1].split("/", 1)[0]
            statement = self.statements[operation]
            if self.fail_explain and statement.startswith("EXPLAIN"):
                return HTTPResponse(400, {"errors": ["Unknown table raw_events"]})
            if statement.startswith(("EXPLAIN", "USE")):
                return HTTPResponse(200, {"resultType": "EOS", "resultKind": "SUCCESS"})
            if statement.startswith("DESCRIBE"):
                return HTTPResponse(
                    200,
                    {
                        "resultType": "EOS",
                        "results": {
                            "data": [
                                {"fields": ["id", "BIGINT", "FALSE"]},
                                {"fields": ["label", "STRING", "TRUE"]},
                            ]
                        },
                    },
                )
            if statement.startswith("SELECT COUNT"):
                return HTTPResponse(
                    200,
                    {
                        "resultType": "EOS",
                        "results": {"data": [{"fields": [self.row_count]}]},
                    },
                )
            return HTTPResponse(
                200,
                {
                    "jobID": "0123456789abcdef0123456789abcdef",
                    "resultType": "EOS",
                    "resultKind": "SUCCESS_WITH_CONTENT",
                },
            )
        if method == "GET" and path.endswith("/exceptions"):
            return HTTPResponse(200, {"root-exception": self.failure})
        if method == "GET" and "/subtasks/metrics" in path:
            vertex = path.split("/vertices/", 1)[1].split("/", 1)[0]
            if vertex == "source":
                return HTTPResponse(
                    200,
                    [
                        {"id": "numRecordsIn", "sum": "10"},
                        {"id": "numRecordsOut", "sum": "9"},
                        {"id": "numRecordsOutPerSecond", "sum": "3.5"},
                        {"id": "currentInputWatermark", "min": str(self._watermark)},
                    ],
                )
            return HTTPResponse(
                200,
                [
                    {"id": "numRecordsIn", "sum": "9"},
                    {"id": "numRecordsOut", "sum": "8"},
                    {"id": "numRecordsOutPerSecond", "sum": "2.5"},
                ],
            )
        if method == "GET" and path.endswith("/metrics"):
            return HTTPResponse(
                200,
                [
                    {"id": "numRestarts", "value": str(self.restart_count)},
                    {"id": "uptime", "value": "12000"},
                ],
            )
        if method == "GET" and "/jobs/" in path:
            state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
            if self.cancelled:
                state = "CANCELED"
            return HTTPResponse(
                200,
                {
                    "jid": "0123456789abcdef0123456789abcdef",
                    "state": state,
                    "start-time": int(datetime.now(UTC).timestamp() * 1000 - 12_000),
                    "duration": 12_000,
                    "vertices": [{"id": "source"}, {"id": "sink"}],
                },
            )
        if method == "PATCH" and "/jobs/" in path:
            self.cancelled = True
            return HTTPResponse(202)
        if method == "DELETE":
            return HTTPResponse(200)
        if method == "POST" and path.endswith(("/cancel", "/heartbeat")):
            return HTTPResponse(200)
        raise AssertionError(f"unexpected Flink request: {method} {url}")


def _batch(transport: FakeFlinkTransport, **config: object) -> BatchConnection:
    return gantry.batch.connect(
        "flink",
        endpoint="https://gateway.example/flink",
        config={
            "jobmanager_endpoint": "https://jobmanager.example",
            "transport": transport,
            **config,
        },
    )


def _stream(transport: FakeFlinkTransport, **config: object) -> StreamConnection:
    return gantry.stream.connect(
        "flink",
        endpoint="https://gateway.example/flink",
        config={
            "jobmanager_endpoint": "https://jobmanager.example",
            "transport": transport,
            **config,
        },
    )


def test_flink_internals_validate_the_engine_boundary() -> None:
    artifact = FlinkSQLArtifact(
        "INSERT INTO clean SELECT * FROM raw",
        declared_inputs=["raw"],
        declared_outputs=["clean"],
    )

    assert artifact.mode is FlinkMode.STREAMING
    assert artifact.to_artifact().kind == "flink_sql"
    with pytest.raises(ValueError, match="must not be empty"):
        FlinkSQLArtifact(" ")
    with pytest.raises(ValueError, match=r"streaming.*batch"):
        FlinkSQLArtifact("INSERT INTO x SELECT 1", mode="continuous")
    with pytest.raises(ValueError, match="http"):
        FlinkTarget("localhost:8083")
    with pytest.raises(ValueError, match="unknown Flink"):
        FlinkTarget("https://flink.example", {"typo": True})


def test_batch_and_stream_have_distinct_capabilities() -> None:
    batch = _batch(FakeFlinkTransport())
    stream = _stream(FakeFlinkTransport())

    assert batch.provider == stream.provider == "flink"
    assert batch.capabilities() == BatchCapabilities(True, True, True, True, True, True)
    assert stream.capabilities() == StreamCapabilities(True, True, True, True, True, True)
    with pytest.raises(ValueError, match="unsupported batch provider"):
        gantry.batch.connect("beam", endpoint="https://runner.example")


async def test_validation_uses_gateway_planner_and_configured_namespace() -> None:
    transport = FakeFlinkTransport()
    job = _stream(
        transport,
        default_catalog="prod-catalog",
        default_database="analytics",
        session_properties={"sql-gateway.session.idle-timeout": "10 min"},
    ).job(inputs=["raw"], outputs=["clean"])

    validation = await job.validate("INSERT INTO clean SELECT * FROM raw")

    assert validation.ok
    assert validation.metadata["mode"] == "stream"
    assert list(transport.statements.values()) == [
        "USE CATALOG `prod-catalog`",
        "USE `analytics`",
        "EXPLAIN PLAN FOR INSERT INTO clean SELECT * FROM raw",
    ]
    statement_calls = [call for call in transport.calls if call.url.endswith("/statements")]
    assert statement_calls[-1].body is not None
    assert statement_calls[-1].body["executionConfig"] == {
        "execution.runtime-mode": "streaming",
        "execution.attached": "false",
    }


async def test_admission_enforces_actual_inputs_outputs_and_operations_locally() -> None:
    transport = FakeFlinkTransport()
    job = _stream(transport).job(inputs=["raw.*"], outputs=["clean.*"])

    denied_input = await job("INSERT INTO clean.events SELECT * FROM finance.events")
    denied_output = await job("INSERT INTO finance.events SELECT * FROM raw.events")
    denied_operation = await job("DROP TABLE raw.events")
    overwrite = await job("INSERT OVERWRITE clean.events SELECT * FROM raw.events")

    assert denied_input.failure is not None
    assert denied_input.failure.kind is FailureKind.INPUT_NOT_ALLOWED
    assert denied_output.failure is not None
    assert denied_output.failure.kind is FailureKind.OUTPUT_NOT_ALLOWED
    assert denied_operation.failure is not None
    assert denied_operation.failure.kind is FailureKind.OPERATION_NOT_ALLOWED
    assert overwrite.failure is not None
    assert overwrite.failure.kind is FailureKind.OPERATION_NOT_ALLOWED
    assert not transport.calls


async def test_batch_allows_overwrite_and_derives_concrete_scope() -> None:
    transport = FakeFlinkTransport()
    transport.states = ["FINISHED"]
    job = _batch(transport).job(inputs=["raw.*"], outputs=["snapshots.*"], poll_interval=0)
    sql = "INSERT OVERWRITE snapshots.daily SELECT * FROM raw.orders"

    plan = job.inspect(sql)
    result = await job(sql)

    assert plan.statement is FlinkJobStatement.INSERT_OVERWRITE
    assert plan.inputs == ("raw.orders",)
    assert plan.output == "snapshots.daily"
    assert result.ok
    assert result.handle is not None
    assert result.handle.metadata["mode"] == "batch"
    assert result.handle.metadata["declared_inputs"] == ("raw.orders",)
    assert result.handle.metadata["declared_outputs"] == ("snapshots.daily",)


async def test_batch_waits_for_success_then_verifies_the_output() -> None:
    transport = FakeFlinkTransport()
    transport.states = ["RUNNING", "FINISHED"]
    job = _batch(transport).job(
        inputs=["raw"],
        outputs=["snapshot"],
        checks=[
            gantry.verify.job_succeeded(),
            gantry.verify.output_exists(),
            gantry.verify.row_count(min=1),
            gantry.verify.required_columns(["id", "label"]),
        ],
        poll_interval=0,
    )

    result = await job("INSERT INTO snapshot SELECT * FROM raw")

    assert result.status is RunStatus.ACCEPTED
    assert result.execution is not None
    assert result.execution.status == "SUCCEEDED"
    assert result.uri == "flink-table:///snapshot"
    assert result.verification is not None
    assert {check.name: check.ok for check in result.verification.checks} == {
        "job_succeeded": True,
        "output_exists": True,
        "row_count": True,
        "required_columns": True,
    }
    assert "DESCRIBE `snapshot`" in transport.statements.values()
    assert "SELECT COUNT(*) FROM `snapshot`" in transport.statements.values()


async def test_engine_success_is_not_accepted_when_batch_verification_fails() -> None:
    transport = FakeFlinkTransport()
    transport.states = ["FINISHED"]
    transport.row_count = 0
    job = _batch(transport).job(
        inputs=["raw"],
        outputs=["snapshot"],
        checks=[gantry.verify.row_count(min=1)],
        poll_interval=0,
    )

    result = await job("INSERT INTO snapshot SELECT * FROM raw")

    assert result.execution is not None
    assert result.execution.status == "SUCCEEDED"
    assert result.status is RunStatus.REJECTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.VERIFICATION_FAILED


async def test_stream_accepts_a_healthy_running_job_and_returns_stream_uri() -> None:
    transport = FakeFlinkTransport()
    job = _stream(transport, token="secret-token", output_refs={"clean": "kafka://clean"}).job(
        inputs=["raw"],
        outputs=["clean"],
        checks=[
            gantry.verify.running(),
            gantry.verify.restart_count(max=2),
            gantry.verify.watermark_lag(max_seconds="60s"),
        ],
        poll_interval=0,
    )

    result = await job("INSERT INTO clean SELECT * FROM raw")

    assert result.ok
    assert result.handle is not None
    assert result.handle.metadata["mode"] == "stream"
    assert result.execution is not None and result.execution.status == "RUNNING"
    assert result.status.accepted
    # The counters the health checks read must also reach the durable record,
    # or a run read back later cannot show what the decision rested on.
    assert result.execution.metrics["records_in"] == 19
    assert result.execution.metrics["records_out"] == 17
    assert result.execution.metrics["restart_count"] == 1
    assert "watermark_lag_seconds" in result.execution.metrics
    assert result.uri == "kafka://clean"
    assert "secret-token" not in repr(result.handle)
    assert transport.calls[0].headers["Authorization"] == "Bearer secret-token"


async def test_running_stream_is_not_accepted_when_health_check_fails() -> None:
    transport = FakeFlinkTransport()
    transport.restart_count = 4
    job = _stream(transport).job(
        inputs=["raw"],
        outputs=["clean"],
        checks=[gantry.verify.restart_count(max=3)],
        poll_interval=0,
    )

    result = await job("INSERT INTO clean SELECT * FROM raw")

    assert result.execution is not None
    assert result.execution.status == "RUNNING"
    assert result.status is RunStatus.REJECTED


async def test_handle_reconnects_from_a_fresh_configured_job_and_can_be_cancelled() -> None:
    first = _stream(FakeFlinkTransport()).job(inputs=["raw"], outputs=["clean"])
    handle = await first.submit("INSERT INTO clean SELECT * FROM raw")
    second_transport = FakeFlinkTransport()
    reconnected = _stream(second_transport).job(inputs=["raw"], outputs=["clean"])

    execution = await reconnected.status(handle)
    cancelled = await reconnected.cancel(handle)

    assert execution.state is ExecutionState.RUNNING
    assert cancelled.state is ExecutionState.CANCELLED
    cancel_call = next(call for call in second_transport.calls if call.method == "PATCH")
    assert cancel_call.url.endswith(f"/jobs/{handle.native_id}?mode=cancel")


async def test_tool_exposes_only_sql_and_uses_the_same_governed_job() -> None:
    transport = FakeFlinkTransport()
    job = _stream(transport, basic_auth=("agent", "password")).job(
        inputs=["raw"], outputs=["clean"], poll_interval=0
    )
    tool = job.tool()

    result = await tool.invoke(sql="INSERT INTO clean SELECT * FROM raw")

    assert result.ok
    assert tool.name == "flink_stream_job"
    schema = cast(dict[str, Any], tool.input_schema)
    assert set(schema["properties"]) == {"sql", "verify"}
    variants = schema["properties"]["verify"]["items"]["oneOf"]
    assert {item["properties"]["type"]["const"] for item in variants} == {
        "restart_count",
        "running",
        "watermark_lag",
    }
    assert "password" not in repr(tool)
    assert "inputs" not in repr(tool.input_schema)
    with pytest.raises(ValueError, match="unexpected"):
        await tool.invoke(sql="INSERT INTO clean SELECT * FROM raw", outputs=["finance"])


async def test_failed_job_maps_connector_failure_and_preserves_native_state() -> None:
    transport = FakeFlinkTransport()
    transport.states = ["FAILED"]
    job = _stream(transport).job(inputs=["raw"], outputs=["clean"], poll_interval=0)

    result = await job("INSERT INTO clean SELECT * FROM raw")

    assert result.status is RunStatus.EXECUTION_FAILED
    assert result.execution is not None
    assert result.execution.metrics["state"] == "FAILED"
    assert result.failure is not None
    assert result.failure.kind is FailureKind.CONNECTOR_ERROR


async def test_planner_rejections_are_normalized() -> None:
    transport = FakeFlinkTransport()
    transport.fail_explain = True
    job = _stream(transport).job(inputs=["raw_events"], outputs=["clean"])

    result = await job("INSERT INTO clean SELECT * FROM raw_events")

    assert result.status is RunStatus.POLICY_REJECTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.VALIDATION_ERROR
    assert "Unknown table" in result.failure.message


async def test_submit_surfaces_structured_admission_failure() -> None:
    job = _batch(FakeFlinkTransport()).job(inputs=["raw"], outputs=["clean"])

    with pytest.raises(FlinkJobError) as captured:
        await job.submit("INSERT INTO finance SELECT * FROM raw")

    assert captured.value.failure.kind is FailureKind.OUTPUT_NOT_ALLOWED


async def test_statement_requests_carry_no_execution_timeout() -> None:
    transport = FakeFlinkTransport()
    transport.states = ["FINISHED"]
    job = _batch(transport).job(inputs=["src"], outputs=["sink"], poll_interval=0)

    await job("INSERT INTO sink SELECT * FROM src")

    statements = [call for call in transport.calls if call.url.endswith("/statements")]
    assert statements
    for call in statements:
        assert call.body is not None
        assert "executionTimeout" not in call.body
        assert call.timeout_seconds > 0


def test_old_public_flink_api_is_removed() -> None:
    import gantry.flink as flink

    assert not hasattr(flink, "connect")
    assert not hasattr(flink, "FlinkConnection")
    assert not hasattr(flink, "FlinkSQLTool")
