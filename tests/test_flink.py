# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

import gantry
import pytest
from gantry import ExecutionHandle, ExecutionState, FailureKind, OutputKind, ResultStatus
from gantry.flink import (
    FlinkConnection,
    FlinkMode,
    FlinkSQLArtifact,
    FlinkTarget,
    HTTPResponse,
    MaxRestartCount,
    MaxWatermarkLag,
    MinOutputRate,
)


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
                    {"id": "numRestarts", "value": "1"},
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


def _connection(
    transport: FakeFlinkTransport,
    **config: object,
) -> FlinkConnection:
    values = {
        "jobmanager_endpoint": "https://jobmanager.example",
        "transport": transport,
        **config,
    }
    return gantry.flink.connect(
        "https://gateway.example/flink",
        config=values,
    )


def test_flink_artifact_and_target_validate_the_v0_boundary() -> None:
    artifact = FlinkSQLArtifact(
        "INSERT INTO clean SELECT * FROM raw",
        declared_inputs=["raw"],
        declared_outputs=["clean"],
    )

    assert artifact.mode is FlinkMode.STREAMING
    assert artifact.to_artifact().kind == "flink_sql"
    assert artifact.declared_outputs == ("clean",)
    with pytest.raises(ValueError, match="must not be empty"):
        FlinkSQLArtifact(" ")
    with pytest.raises(ValueError, match=r"streaming.*batch"):
        FlinkSQLArtifact("INSERT INTO x SELECT 1", mode="continuous")
    with pytest.raises(TypeError, match="collection"):
        FlinkSQLArtifact("INSERT INTO x SELECT 1", declared_inputs="raw")
    with pytest.raises(ValueError, match="http"):
        FlinkTarget("localhost:8083")
    with pytest.raises(ValueError, match="unknown Flink"):
        FlinkTarget("https://flink.example", {"typo": True})


async def test_validation_uses_gateway_planner_and_configured_namespace() -> None:
    transport = FakeFlinkTransport()
    flink = _connection(
        transport,
        default_catalog="prod-catalog",
        default_database="analytics",
        session_properties={"sql-gateway.session.idle-timeout": "10 min"},
    )

    validation = await flink.validate("INSERT INTO clean SELECT * FROM raw")

    assert validation.ok
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


async def test_validation_rejects_unsupported_sql_and_planner_errors() -> None:
    transport = FakeFlinkTransport()
    flink = _connection(transport)

    unsupported = await flink.validate("CREATE TABLE events (id INT)")
    multiple = await flink.validate("INSERT INTO a SELECT 1; INSERT INTO b SELECT 2")
    transport.fail_explain = True
    unresolved = await flink.validate("INSERT INTO clean SELECT * FROM raw_events")

    assert not unsupported.ok
    assert "only INSERT" in unsupported.errors[0]
    assert not multiple.ok
    assert "exactly one" in multiple.errors[0]
    assert not unresolved.ok
    assert "Unknown table" in unresolved.errors[0]


async def test_streaming_run_returns_durable_job_handle_health_and_output_refs() -> None:
    transport = FakeFlinkTransport()
    flink = _connection(transport, token="secret-token", output_refs={"clean": "kafka://clean"})

    result = await flink.run(
        sql="INSERT INTO clean SELECT * FROM raw",
        declared_inputs=("raw",),
        declared_outputs=("clean",),
        checks=(MaxRestartCount(2), MaxWatermarkLag("60s"), MinOutputRate(1)),
        poll_interval_seconds=0,
    )

    assert result.status is ResultStatus.ACCEPTED
    assert result.handle is not None
    assert result.handle.native_id == "0123456789abcdef0123456789abcdef"
    assert result.handle.metadata["mode"] == "streaming"
    assert "secret-token" not in repr(result.handle)
    assert result.execution is not None
    assert result.execution.state is ExecutionState.RUNNING
    assert result.health is not None and result.health.healthy
    assert result.health.checks == {
        "job_running": True,
        "max_restarts": True,
        "max_watermark_lag": True,
        "min_output_rate": True,
    }
    assert result.metrics.records_in == 19
    assert result.metrics.records_out == 17
    assert result.metrics.restart_count == 1
    assert result.outputs[0].kind is OutputKind.STREAM
    assert result.outputs[0].uri == "kafka://clean"
    assert transport.calls[0].headers["Authorization"] == "Bearer secret-token"


async def test_handle_reconnects_from_a_fresh_connection_and_cancel_uses_jobmanager() -> None:
    first_transport = FakeFlinkTransport()
    first = _connection(first_transport)
    handle = await first.submit(
        FlinkSQLArtifact(
            "INSERT INTO clean SELECT * FROM raw",
            declared_outputs=("clean",),
        )
    )
    second_transport = FakeFlinkTransport()
    reconnected = _connection(second_transport)

    execution = await reconnected.status(handle)
    cancelled = await reconnected.cancel(handle)

    assert execution.state is ExecutionState.RUNNING
    assert cancelled.state is ExecutionState.CANCELLED
    cancel_call = next(call for call in second_transport.calls if call.method == "PATCH")
    assert cancel_call.url.endswith(f"/jobs/{handle.native_id}?mode=cancel")


async def test_failed_job_maps_connector_failure_and_preserves_native_state() -> None:
    transport = FakeFlinkTransport()
    transport.states = ["FAILED"]
    flink = _connection(transport)
    handle = ExecutionHandle(
        "flink_existing",
        "flink",
        "flink",
        "0123456789abcdef0123456789abcdef",
        metadata={"mode": "streaming"},
    )

    execution = await flink.status(handle)
    result = await flink.wait(handle, poll_interval_seconds=0)

    assert execution.state is ExecutionState.FAILED
    assert execution.native["state"] == "FAILED"
    assert execution.failure is not None
    assert execution.failure.kind is FailureKind.CONNECTOR_ERROR
    assert result.status is ResultStatus.FAILED


async def test_batch_waits_for_finished_and_returns_table_reference() -> None:
    transport = FakeFlinkTransport()
    transport.states = ["RUNNING", "FINISHED"]
    flink = _connection(transport)
    handle = await flink.submit(
        FlinkSQLArtifact(
            "INSERT INTO snapshot SELECT * FROM raw",
            mode="batch",
            declared_outputs=("snapshot",),
        )
    )

    result = await flink.wait(handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.ACCEPTED
    assert result.execution is not None
    assert result.execution.state is ExecutionState.SUCCEEDED
    assert result.outputs[0].kind is OutputKind.TABLE
    assert result.outputs[0].uri == "flink-table:///snapshot"


async def test_tool_is_small_credential_free_and_runs_the_same_lifecycle() -> None:
    transport = FakeFlinkTransport()
    tool = _connection(transport, basic_auth=("agent", "password")).as_tool(timeout=5)

    result = await tool("INSERT INTO clean SELECT * FROM raw", declared_outputs=("clean",))

    assert result.status is ResultStatus.ACCEPTED
    assert "password" not in repr(tool.input_schema)
    assert "endpoint" not in repr(tool.input_schema["properties"])
    assert not hasattr(tool, "config")
    assert not hasattr(tool, "client")


async def test_statement_requests_carry_no_execution_timeout() -> None:
    """Flink's SQL Gateway refuses any positive `executionTimeout`.

    `SqlGatewayService doesn't support timeout mechanism now` — it throws
    before planning the statement, so a request carrying one fails outright and
    every operation with it. Gantry sent one derived from its own configured
    timeouts, which made the adapter unusable against a real gateway while
    every test here passed.

    The timeout that matters is on the HTTP call, which is what actually bounds
    how long a caller waits, and it is asserted here too so removing the field
    cannot quietly remove the bound as well.
    """
    transport = FakeFlinkTransport()
    transport.states = ["RUNNING", "FINISHED"]
    connection = _connection(transport)
    handle = await connection.submit(
        FlinkSQLArtifact(
            "INSERT INTO sink SELECT * FROM src",
            mode="batch",
            declared_inputs=("src",),
            declared_outputs=("sink",),
        )
    )
    await connection.wait(handle, poll_interval_seconds=0)

    statements = [call for call in transport.calls if call.url.endswith("/statements")]
    assert statements, "the run should have submitted at least one statement"
    for call in statements:
        assert call.body is not None
        assert "executionTimeout" not in call.body, (
            "Flink rejects a positive executionTimeout; it must not be sent at all"
        )
        assert call.timeout_seconds > 0, "the HTTP call must still be bounded"
