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
from gantry.sql.schema import Column, Table
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
            # The root cause, not the stack that wraps it. A rejected
            # validation is the message an agent gets back to correct its own
            # SQL with, and "Internal server error" followed by two hundred
            # Java frames is not something anything can act on.
            return ValidationResult.rejected(_root_cause(str(error)))
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
                    "mode": "stream" if mode is FlinkMode.STREAMING else "batch",
                    "flink_mode": mode.value,
                    "declared_inputs": tuple(artifact.declared_inputs),
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

    async def inspect_table(self, name: str, *, include_row_count: bool = False) -> Table | None:
        """Inspect one catalog table through the existing SQL Gateway connection."""

        # Two spellings, because "a.b" is genuinely ambiguous. A JDBC catalog
        # exposes a PostgreSQL table as *one* identifier containing a dot
        # (`analytics.orders`); other catalogs mean schema-then-table
        # (`analytics`.`orders`). Guessing one way makes every table in a
        # non-public schema uninspectable, so both are tried and whichever the
        # engine recognises wins.
        candidates = [_qualified_identifier(name)]
        if "." in name:
            single = _identifier(_unquote_identifier(name))
            if single not in candidates:
                candidates.insert(0, single)

        describe = None
        last: Exception | None = None
        for candidate in candidates:
            try:
                describe = await self._query(f"DESCRIBE {candidate}")
                break
            except FlinkHTTPError as error:
                last = error
                continue
        if describe is None:
            if last is None or _is_missing_object(last):
                return None
            raise last
        columns = _describe_columns(describe)
        rows: int | None = None
        if include_row_count:
            count = await self._query(f"SELECT COUNT(*) FROM {candidate}")
            rows = _row_count(count)
        catalog, schema, table_name = _name_parts(name)
        metadata: dict[str, object] = {}
        if rows is not None:
            metadata["rows"] = rows
        return Table(
            name=table_name,
            schema=schema,
            catalog=catalog,
            columns=columns,
            metadata=metadata,
        )

    async def _query(self, statement: str) -> Mapping[str, object]:
        session: str | None = None
        operation: str | None = None
        try:
            session = await self._open_configured_session(FlinkMode.BATCH)
            operation = await self._client.execute_statement(
                session,
                statement,
                execution_config=self._execution_config(FlinkMode.BATCH),
            )
            return await self._wait_for_operation(
                session,
                operation,
                timeout_seconds=self._timeout("request_timeout", 30.0),
            )
        finally:
            await self._close_gateway_resources(session, operation)

    async def _collect_result(
        self,
        session: str,
        operation: str,
        *,
        deadline: float,
    ) -> Mapping[str, object]:
        """Read every page of a finished operation's result.

        Two things make a single fetch wrong, and both are silent.

        The first page is often `NOT_READY`, and the first `PAYLOAD` page is
        frequently empty with the rows on the page after it — so stopping at
        the first payload returns "no rows" for a query that has plenty.

        The pages are also a *changelog*, not a result set. A `SELECT COUNT(*)`
        arrives as INSERT 1, UPDATE_BEFORE 1, UPDATE_AFTER 2, … up to the real
        answer, so the rows have to be accumulated in order and interpreted by
        their `kind` rather than read positionally.
        """
        token = 0
        merged: list[object] = []
        head: Mapping[str, object] | None = None
        columns: object = None
        while True:
            response = await self._client.fetch_result(session, operation, token)
            result_type = str(response.get("resultType", "PAYLOAD")).upper()
            if head is None and result_type != "NOT_READY":
                head = response
            results = response.get("results")
            if isinstance(results, Mapping):
                if columns is None:
                    columns = results.get("columns")
                data = results.get("data")
                if isinstance(data, list):
                    merged.extend(data)
            if result_type == "EOS":
                break
            if result_type == "NOT_READY" and time.monotonic() >= deadline:
                raise TimeoutError("Flink SQL operation timed out")
            if result_type != "NOT_READY":
                token += 1
            else:
                await asyncio.sleep(0.05)
            if time.monotonic() >= deadline:
                raise TimeoutError("Flink SQL operation timed out")

        payload = dict(head or response)
        payload["results"] = {"columns": columns or [], "data": merged}
        return payload

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
                return await self._collect_result(session, operation, deadline=deadline)
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
    # `message` is the whole server-side stack, thousands of characters of Java
    # frames wrapping one sentence that says what is actually wrong. The full
    # text is kept as `native_message` for a human reading a log; the summary
    # is the root cause, because the thing most likely to read this is a model
    # deciding how to correct its own SQL, and it cannot do that from a stack
    # trace.
    return Failure(
        kind, retryable, _root_cause(message), native_message=message, native=native_mapping
    )


def _is_missing_object(error: Exception) -> bool:
    """Whether an error means "no such table" rather than "something broke".

    A verification check has to tell those apart: a missing destination is a
    result to report, and a broken connection is not. Flink wraps both in an
    HTTP 500 with the same shape, so the distinction lives in the root cause —
    "Tables or views with the identifier ... doesn't exist."
    """
    if getattr(error, "status", None) == 404:
        return True
    cause = _root_cause(str(error)).lower()
    return any(
        phrase in cause
        for phrase in (
            "doesn't exist",
            "does not exist",
            "not found",
            "unknown table",
        )
    )


def _root_cause(message: str) -> str:
    """The innermost `Caused by:` sentence, or the message unchanged.

    Java nests its causes, so the last one is the specific complaint —
    "Column 'no_such_column' not found in any table" rather than "Internal
    server error". Frames are skipped; only the exception line is wanted.
    """
    causes = [
        line.strip()[len("Caused by:") :].strip()
        for line in message.splitlines()
        if line.strip().startswith("Caused by:")
    ]
    for cause in reversed(causes):
        _, _, detail = cause.partition(": ")
        text = (detail or cause).strip()
        if text:
            return text
    first = message.strip().splitlines()
    return first[0].strip() if first else message


def _unknown(handle: ExecutionHandle, message: str, *, failure: Failure | None = None) -> Execution:
    return Execution(
        handle,
        ExecutionState.UNKNOWN,
        updated_at=datetime.now(UTC),
        failure=failure or Failure(FailureKind.UNKNOWN, False, message),
    )


def _handle_mode(handle: ExecutionHandle) -> FlinkMode:
    value = handle.metadata.get("flink_mode", handle.metadata.get("mode", "streaming"))
    try:
        if not isinstance(value, str):
            raise TypeError
        if value == "stream":
            return FlinkMode.STREAMING
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


def _qualified_identifier(value: str) -> str:
    return ".".join(_identifier(_unquote_identifier(part.strip())) for part in value.split("."))


def _unquote_identifier(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "`"}:
        return value[1:-1].replace(value[0] * 2, value[0])
    return value


def _name_parts(value: str) -> tuple[str | None, str | None, str]:
    parts = tuple(_unquote_identifier(part.strip()) for part in value.split("."))
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0]


def _result_changelog(
    payload: Mapping[str, object],
) -> tuple[tuple[tuple[object, ...], str], ...]:
    """Result rows paired with their changelog kind.

    `_result_rows` drops the kind, which is right for a plain SELECT and wrong
    for anything Flink computes incrementally.
    """
    results = payload.get("results")
    if not isinstance(results, Mapping):
        return ()
    data = results.get("data")
    if not isinstance(data, list):
        return ()
    rows: list[tuple[tuple[object, ...], str]] = []
    for item in data:
        if not isinstance(item, Mapping):
            continue
        fields = item.get("fields")
        if isinstance(fields, list):
            rows.append((tuple(fields), str(item.get("kind", "INSERT")).upper()))
    return tuple(rows)


def _result_rows(payload: Mapping[str, object]) -> tuple[tuple[object, ...], ...]:
    results = payload.get("results")
    if not isinstance(results, Mapping):
        return ()
    data = results.get("data")
    if not isinstance(data, list):
        return ()
    rows: list[tuple[object, ...]] = []
    for item in data:
        if not isinstance(item, Mapping):
            continue
        fields = item.get("fields")
        if isinstance(fields, list):
            rows.append(tuple(fields))
    return tuple(rows)


def _describe_columns(payload: Mapping[str, object]) -> tuple[Column, ...]:
    columns: list[Column] = []
    for row in _result_rows(payload):
        if len(row) < 2 or not isinstance(row[0], str) or not isinstance(row[1], str):
            continue
        # DESCRIBE includes physical columns first and may append watermark/constraint rows.
        if row[0].startswith(("#", "WATERMARK", "CONSTRAINT")):
            continue
        nullable = True
        if len(row) > 2 and isinstance(row[2], str):
            nullable = row[2].upper() not in {"FALSE", "NO", "NOT NULL"}
        columns.append(Column(row[0], row[1], nullable))
    return tuple(columns)


def _row_count(payload: Mapping[str, object]) -> int | None:
    """The final value of an aggregate, read as a changelog.

    Flink returns `SELECT COUNT(*)` as a stream of retractions: INSERT 1,
    UPDATE_BEFORE 1, UPDATE_AFTER 2, and so on up to the answer. Taking the
    first row gives 1 for any non-empty table, which is a plausible-looking
    number and always wrong.

    So the last row that is not a retraction wins. An empty table does emit a
    single `0`, checked rather than assumed; None is reserved for a count that
    never arrived at all, which the caller reports differently from a count of
    zero.
    """
    for row, kind in reversed(_result_changelog(payload)):
        if kind in {"UPDATE_BEFORE", "DELETE"}:
            continue
        if not row:
            continue
        value = row[0]
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None
    return None


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
