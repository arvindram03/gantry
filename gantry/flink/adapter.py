# SPDX-License-Identifier: Apache-2.0
"""Flink SQL execution adapter."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from contextlib import suppress
from datetime import UTC, datetime
from typing import cast
from urllib.parse import quote
from uuid import uuid4

from gantry.artifact import Artifact
from gantry.capabilities import AdapterCapabilities
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.flink.artifact import FlinkMode
from gantry.flink.client import FlinkHTTPError, FlinkRESTClient
from gantry.flink.metrics import FlinkMetrics
from gantry.flink.target import FlinkTarget
from gantry.handle import ExecutionHandle
from gantry.output import OutputKind, OutputRef
from gantry.policy import PolicyRequirements
from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import ConservativeDialect
from gantry.target import ExecutionTarget

_OPERATION_FAILURES = frozenset({"CANCELED", "CANCELLED", "ERROR", "CLOSED"})
_PENDING_STATES = frozenset({"CREATED", "SCHEDULED", "DEPLOYING", "INITIALIZING", "RECONCILING"})
_RUNNING_STATES = frozenset({"RUNNING", "RESTARTING", "CANCELLING", "CANCELING"})
_FAILED_STATES = frozenset({"FAILED", "FAILING"})
_CANCELLED_STATES = frozenset({"CANCELED", "CANCELLED"})
_JOB_METRICS = ("numRestarts", "fullRestarts", "uptime")
_VERTEX_METRICS = (
    "numRecordsIn",
    "numRecordsOut",
    "numRecordsOutPerSecond",
    "currentInputWatermark",
)


class FlinkAdapter:
    """Map Flink's native SQL and monitoring APIs onto Gantry's lifecycle."""

    def __init__(self, target: FlinkTarget, client: FlinkRESTClient | None = None) -> None:
        self._target = target
        self._client = client or FlinkRESTClient(target)
        self._dialect = ConservativeDialect()

    def capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            reconnect=True,
            cancellation=True,
            write_execution=True,
            scoped_credentials=True,
            remote_status=True,
            metrics=True,
            result_reference=True,
        )

    async def validate(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
        policy: PolicyRequirements,
    ) -> ValidationResult:
        del target, context, policy
        errors = self._local_errors(artifact)
        if errors:
            return ValidationResult.rejected(*errors)
        sql, mode = _artifact_values(artifact)
        session: str | None = None
        operation: str | None = None
        try:
            session = await self._open_configured_session(mode)
            operation = await self._client.execute_statement(
                session,
                f"EXPLAIN PLAN FOR {sql}",
                execution_config=self._execution_config(mode),
            )
            response = await self._wait_for_operation(
                session,
                operation,
                timeout_seconds=self._timeout("validation_timeout", 30.0),
            )
            return ValidationResult.accepted(
                metadata={
                    "engine": "flink",
                    "mode": mode.value,
                    "planner_result": response.get("resultKind", "SUCCESS"),
                }
            )
        except (FlinkHTTPError, TimeoutError) as error:
            return ValidationResult.rejected(str(error))
        finally:
            await self._close_gateway_resources(session, operation)

    async def submit(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
    ) -> ExecutionHandle:
        del target, context
        errors = self._local_errors(artifact)
        if errors:
            raise ValueError("; ".join(errors))
        sql, mode = _artifact_values(artifact)
        session: str | None = None
        operation: str | None = None
        try:
            session = await self._open_configured_session(mode)
            operation = await self._client.execute_statement(
                session,
                sql,
                execution_config=self._execution_config(mode),
            )
            response = await self._wait_for_operation(
                session,
                operation,
                timeout_seconds=self._timeout("submission_timeout", 30.0),
            )
            job_id = _job_id(response)
            if job_id is None:
                raise FlinkHTTPError(
                    None,
                    "Flink SQL submission completed without returning a JobID",
                    response,
                )
            outputs = self._output_metadata(artifact, mode)
            return ExecutionHandle(
                gantry_id=f"flink_{uuid4().hex}",
                engine="flink",
                target=self._target.name,
                native_id=job_id,
                metadata={
                    "mode": mode.value,
                    "declared_outputs": tuple(artifact.declared_outputs),
                    "outputs": outputs,
                    "gateway_session": session,
                    "gateway_operation": operation,
                },
            )
        finally:
            await self._close_gateway_resources(session, operation)

    async def status(self, *, handle: ExecutionHandle) -> Execution:
        invalid = self._handle_error(handle)
        if invalid is not None:
            return _unknown(handle, invalid)
        try:
            details = await self._client.job_details(handle.native_id)
            native_state = str(details.get("state", "UNKNOWN")).upper()
            state = _normalize_state(native_state)
            flink_metrics = await self._collect_metrics(handle.native_id, details)
            failure = None
            if state is ExecutionState.FAILED:
                failure = await self._job_failure(handle.native_id, details)
            elif state is ExecutionState.CANCELLED:
                failure = Failure(FailureKind.CANCELLED, False, "Flink job was cancelled")
            elif state is ExecutionState.UNKNOWN:
                failure = Failure(
                    FailureKind.UNKNOWN,
                    False,
                    f"unknown Flink job state: {native_state}",
                    native={"state": native_state},
                )
            return Execution(
                handle=handle,
                state=state,
                started_at=_timestamp(details.get("start-time")),
                updated_at=datetime.now(UTC),
                metrics=flink_metrics.to_execution_metrics(),
                failure=failure,
                native={"state": native_state, "job": details},
            )
        except FlinkHTTPError as error:
            return _unknown(handle, str(error), failure=_failure_from_error(error))

    async def result(self, *, handle: ExecutionHandle) -> ExecutionResult:
        execution = await self.status(handle=handle)
        mode = _handle_mode(handle)
        ready = execution.state is ExecutionState.SUCCEEDED or (
            mode is FlinkMode.STREAMING and execution.state is ExecutionState.RUNNING
        )
        if ready:
            return ExecutionResult.succeeded(
                handle,
                outputs=_outputs(handle),
                metrics=execution.metrics,
                native=execution.native,
            )
        failure = execution.failure or Failure(
            FailureKind.ENGINE_ERROR,
            False,
            f"Flink job result is not available in state {execution.state.value}",
        )
        return ExecutionResult.failed(handle, failure)

    async def cancel(self, *, handle: ExecutionHandle, mode: str = "default") -> Execution:
        if mode not in {"default", "cancel"}:
            return _unknown(handle, f"unsupported Flink cancellation mode: {mode}")
        invalid = self._handle_error(handle)
        if invalid is not None:
            return _unknown(handle, invalid)
        try:
            await self._client.cancel_job(handle.native_id)
            return await self.status(handle=handle)
        except FlinkHTTPError as error:
            return _unknown(handle, str(error), failure=_failure_from_error(error))

    async def metrics(self, handle: ExecutionHandle) -> FlinkMetrics:
        invalid = self._handle_error(handle)
        if invalid is not None:
            raise ValueError(invalid)
        details = await self._client.job_details(handle.native_id)
        return await self._collect_metrics(handle.native_id, details)

    def outputs(self, handle: ExecutionHandle) -> tuple[OutputRef, ...]:
        invalid = self._handle_error(handle)
        if invalid is not None:
            raise ValueError(invalid)
        return _outputs(handle)

    def _local_errors(self, artifact: Artifact) -> tuple[str, ...]:
        if artifact.kind != "flink_sql":
            return ("Flink adapter requires a flink_sql artifact",)
        if not isinstance(artifact.payload, str) or not artifact.payload.strip():
            return ("Flink SQL must not be empty",)
        classification = self._dialect.classify(artifact.payload)
        errors: list[str] = []
        if classification.statement_count != 1:
            errors.append("Flink v0 requires exactly one SQL statement")
        if classification.operation is not SQLOperation.INSERT:
            errors.append("Flink v0 supports only INSERT INTO or INSERT OVERWRITE statements")
        mode = artifact.metadata.get("flink_mode", FlinkMode.STREAMING.value)
        try:
            if not isinstance(mode, str):
                raise TypeError
            FlinkMode(mode)
        except (TypeError, ValueError):
            errors.append("Flink mode must be 'streaming' or 'batch'")
        return tuple(errors)

    async def _open_configured_session(self, mode: FlinkMode) -> str:
        session = await self._client.open_session(self._string_mapping("session_properties"))
        try:
            catalog = self._target.config.get("default_catalog")
            database = self._target.config.get("default_database")
            if isinstance(catalog, str):
                await self._execute_control(session, f"USE CATALOG {_identifier(catalog)}", mode)
            if isinstance(database, str):
                await self._execute_control(session, f"USE {_identifier(database)}", mode)
        except Exception:
            with suppress(Exception):
                await self._client.close_session(session)
            raise
        return session

    async def _execute_control(self, session: str, statement: str, mode: FlinkMode) -> None:
        operation = await self._client.execute_statement(
            session,
            statement,
            execution_config=self._execution_config(mode),
        )
        try:
            await self._wait_for_operation(
                session,
                operation,
                timeout_seconds=self._timeout("request_timeout", 30.0),
            )
        finally:
            with suppress(Exception):
                await self._client.close_operation(session, operation)

    async def _wait_for_operation(
        self,
        session: str,
        operation: str,
        *,
        timeout_seconds: float,
    ) -> Mapping[str, object]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            status = await self._client.operation_status(session, operation)
            if status == "FINISHED":
                response = await self._client.fetch_result(session, operation)
                result_type = str(response.get("resultType", "PAYLOAD")).upper()
                if result_type == "NOT_READY":
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Flink SQL operation timed out")
                    await asyncio.sleep(0.05)
                    continue
                return response
            if status in _OPERATION_FAILURES:
                # Fetching surfaces the planner or submission exception when available.
                try:
                    await self._client.fetch_result(session, operation)
                except FlinkHTTPError as error:
                    raise error
                raise FlinkHTTPError(None, f"Flink SQL operation ended in {status}")
            if time.monotonic() >= deadline:
                with suppress(Exception):
                    await self._client.cancel_operation(session, operation)
                raise TimeoutError("Flink SQL operation timed out")
            await asyncio.sleep(0.05)

    async def _close_gateway_resources(self, session: str | None, operation: str | None) -> None:
        if session is not None and operation is not None:
            with suppress(Exception):
                await self._client.close_operation(session, operation)
        if session is not None:
            with suppress(Exception):
                await self._client.close_session(session)

    async def _collect_metrics(self, job_id: str, details: Mapping[str, object]) -> FlinkMetrics:
        native: dict[str, object] = {}
        job_metrics: tuple[Mapping[str, object], ...] = ()
        with suppress(FlinkHTTPError):
            job_metrics = await self._client.job_metrics(job_id, _JOB_METRICS)
            native["job"] = job_metrics
        values = _metric_values(job_metrics)
        restarts = _integer(values.get("numRestarts"))
        if restarts is None:
            restarts = _integer(values.get("fullRestarts"))
        duration = _number(details.get("duration"))
        runtime = duration / 1000 if duration is not None and duration >= 0 else None
        if runtime is None:
            uptime = _number(values.get("uptime"))
            runtime = uptime / 1000 if uptime is not None else None

        records_in = 0
        records_out = 0
        input_seen = False
        output_seen = False
        output_rate = 0.0
        output_rate_seen = False
        watermarks: list[float] = []
        vertices_native: dict[str, object] = {}
        vertices = details.get("vertices", ())
        if isinstance(vertices, list):
            for vertex in vertices:
                if not isinstance(vertex, Mapping) or not isinstance(vertex.get("id"), str):
                    continue
                vertex_id = cast(str, vertex["id"])
                rows: tuple[Mapping[str, object], ...] = ()
                with suppress(FlinkHTTPError):
                    rows = await self._client.vertex_metrics(job_id, vertex_id, _VERTEX_METRICS)
                if rows:
                    vertices_native[vertex_id] = rows
                aggregated = _metric_aggregates(rows)
                incoming = _number(aggregated.get(("numRecordsIn", "sum")))
                outgoing = _number(aggregated.get(("numRecordsOut", "sum")))
                rate = _number(aggregated.get(("numRecordsOutPerSecond", "sum")))
                watermark = _number(aggregated.get(("currentInputWatermark", "min")))
                if incoming is not None:
                    records_in += int(incoming)
                    input_seen = True
                if outgoing is not None:
                    records_out += int(outgoing)
                    output_seen = True
                if rate is not None:
                    output_rate += rate
                    output_rate_seen = True
                if watermark is not None and watermark > 0:
                    watermarks.append(watermark)
        if vertices_native:
            native["vertices"] = vertices_native
        if output_rate_seen:
            native["output_rate"] = output_rate
        lag = None
        if watermarks:
            lag = max(0.0, datetime.now(UTC).timestamp() - min(watermarks) / 1000)
        return FlinkMetrics(
            records_in=records_in if input_seen else None,
            records_out=records_out if output_seen else None,
            runtime_seconds=runtime,
            restart_count=restarts,
            watermark_lag_seconds=lag,
            native=native,
        )

    async def _job_failure(self, job_id: str, details: Mapping[str, object]) -> Failure:
        native: Mapping[str, object] = details
        message = _failure_message(details)
        with suppress(FlinkHTTPError):
            exceptions = await self._client.job_exceptions(job_id)
            native = exceptions
            message = _failure_message(exceptions) or message
        return _classify_failure(message or "Flink job failed", native=native)

    def _execution_config(self, mode: FlinkMode) -> Mapping[str, str]:
        values = dict(self._string_mapping("execution_config"))
        values.setdefault("execution.runtime-mode", mode.value)
        # Detached submission makes the JobID, rather than the Gateway session,
        # the durable lifecycle boundary.
        values.setdefault("execution.attached", "false")
        return values

    def _string_mapping(self, name: str) -> Mapping[str, str]:
        value = self._target.config.get(name, {})
        assert isinstance(value, Mapping)
        result: dict[str, str] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not isinstance(item, str):
                raise TypeError(f"Flink {name} must map strings to strings")
            result[key] = item
        return result

    def _timeout(self, name: str, default: float) -> float:
        fallback = self._target.request_timeout if name != "request_timeout" else default
        value = self._target.config.get(name, fallback)
        assert isinstance(value, (int, float)) and not isinstance(value, bool)
        return float(value)

    def _output_metadata(
        self, artifact: Artifact, mode: FlinkMode
    ) -> tuple[Mapping[str, str], ...]:
        configured = self._target.config.get("output_refs", {})
        assert isinstance(configured, Mapping)
        default_kind = OutputKind.STREAM if mode is FlinkMode.STREAMING else OutputKind.TABLE
        outputs: list[Mapping[str, str]] = []
        names = artifact.declared_outputs
        if not names and isinstance(artifact.payload, str):
            classification = self._dialect.classify(artifact.payload)
            names = tuple(reference.qualified_name for reference in classification.write_targets)
        for name in names:
            value = configured.get(name)
            uri = value if isinstance(value, str) and value.strip() else _output_uri(name)
            outputs.append(
                {"kind": _output_kind(uri, default_kind).value, "uri": uri, "name": name}
            )
        return tuple(outputs)

    def _handle_error(self, handle: ExecutionHandle) -> str | None:
        if handle.engine != "flink":
            return "execution handle is not a Flink handle"
        if handle.target != self._target.name:
            return "execution handle belongs to a different Flink target"
        return None


def _artifact_values(artifact: Artifact) -> tuple[str, FlinkMode]:
    assert isinstance(artifact.payload, str)
    mode = artifact.metadata.get("flink_mode", "streaming")
    assert isinstance(mode, str)
    return artifact.payload, FlinkMode(mode)


def _job_id(response: Mapping[str, object]) -> str | None:
    for name in ("jobID", "jobId", "job_id"):
        value = response.get(name)
        if isinstance(value, str) and value:
            return value
    return None


def _normalize_state(state: str) -> ExecutionState:
    if state in _PENDING_STATES:
        return ExecutionState.PENDING
    if state in _RUNNING_STATES:
        return ExecutionState.RUNNING
    if state == "FINISHED":
        return ExecutionState.SUCCEEDED
    if state in _FAILED_STATES:
        return ExecutionState.FAILED
    if state in _CANCELLED_STATES:
        return ExecutionState.CANCELLED
    return ExecutionState.UNKNOWN


def _timestamp(value: object) -> datetime | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    return datetime.fromtimestamp(number / 1000, tz=UTC)


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _integer(value: object) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _metric_values(rows: tuple[Mapping[str, object], ...]) -> Mapping[str, object]:
    return {
        str(row["id"]): row.get("value")
        for row in rows
        if isinstance(row.get("id"), str) and "value" in row
    }


def _metric_aggregates(
    rows: tuple[Mapping[str, object], ...],
) -> Mapping[tuple[str, str], object]:
    values: dict[tuple[str, str], object] = {}
    for row in rows:
        name = row.get("id")
        if not isinstance(name, str):
            continue
        for aggregation in ("min", "max", "sum", "avg"):
            if aggregation in row:
                values[(name, aggregation)] = row[aggregation]
    return values


def _failure_message(payload: Mapping[str, object]) -> str:
    root = payload.get("root-exception")
    if isinstance(root, str) and root:
        return root
    history = payload.get("exceptionHistory")
    if isinstance(history, Mapping):
        entries = history.get("entries")
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, Mapping):
                    for key in ("stacktrace", "exceptionName"):
                        value = entry.get(key)
                        if isinstance(value, str) and value:
                            return value
    for key in ("exception", "failure-cause", "message"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _failure_from_error(error: FlinkHTTPError) -> Failure:
    if error.status in {401, 403}:
        return Failure(FailureKind.AUTH_ERROR, False, str(error), native_code=str(error.status))
    if error.status == 404:
        return Failure(FailureKind.OBJECT_NOT_FOUND, False, str(error), native_code="404")
    return _classify_failure(str(error), native=error.payload)


def _classify_failure(message: str, *, native: object = None) -> Failure:
    lowered = message.lower()
    if any(word in lowered for word in ("connector", "kafka", "jdbc", "filesystem")):
        kind = FailureKind.CONNECTOR_ERROR
        retryable = False
    elif any(
        word in lowered
        for word in ("outofmemory", "out of memory", "insufficient resource", "no resource")
    ):
        kind = FailureKind.RESOURCE_ERROR
        retryable = True
    elif any(word in lowered for word in ("parseexception", "sql parse", "syntax error")):
        kind = FailureKind.SYNTAX_ERROR
        retryable = False
    elif any(word in lowered for word in ("table not found", "object not found", "unknown table")):
        kind = FailureKind.OBJECT_NOT_FOUND
        retryable = False
    elif any(
        word in lowered
        for word in ("user code", "usercode", "scalarfunction", "tablefunction", "udf")
    ):
        kind = FailureKind.USER_CODE_ERROR
        retryable = False
    else:
        kind = FailureKind.ENGINE_ERROR
        retryable = False
    native_mapping = native if isinstance(native, Mapping) else {}
    return Failure(kind, retryable, message, native_message=message, native=native_mapping)


def _unknown(handle: ExecutionHandle, message: str, *, failure: Failure | None = None) -> Execution:
    return Execution(
        handle,
        ExecutionState.UNKNOWN,
        updated_at=datetime.now(UTC),
        failure=failure or Failure(FailureKind.UNKNOWN, False, message),
    )


def _handle_mode(handle: ExecutionHandle) -> FlinkMode:
    value = handle.metadata.get("mode", "streaming")
    try:
        if not isinstance(value, str):
            raise TypeError
        return FlinkMode(value)
    except (TypeError, ValueError):
        return FlinkMode.STREAMING


def _outputs(handle: ExecutionHandle) -> tuple[OutputRef, ...]:
    value = handle.metadata.get("outputs", ())
    if not isinstance(value, (tuple, list)):
        return ()
    outputs: list[OutputRef] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        kind = item.get("kind")
        uri = item.get("uri")
        name = item.get("name")
        if not isinstance(kind, str) or not isinstance(uri, str):
            continue
        try:
            output_kind = OutputKind(kind)
        except ValueError:
            output_kind = OutputKind.CUSTOM
        metadata = {"name": name} if isinstance(name, str) else {}
        outputs.append(OutputRef(output_kind, uri, metadata))
    return tuple(outputs)


def _identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def _output_uri(name: str) -> str:
    return f"flink-table:///{quote(name, safe='._-')}"


def _output_kind(uri: str, default: OutputKind) -> OutputKind:
    scheme = uri.partition(":")[0].lower()
    if scheme in {"kafka", "kinesis", "pulsar"}:
        return OutputKind.STREAM
    if scheme in {
        "bigquery",
        "flink-table",
        "jdbc",
        "mysql",
        "postgres",
        "postgresql",
        "snowflake",
        "table",
    }:
        return OutputKind.TABLE
    if scheme == "file":
        return OutputKind.FILE
    if scheme in {"abfs", "gs", "s3"}:
        return OutputKind.OBJECT
    return default
