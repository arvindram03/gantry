# SPDX-License-Identifier: Apache-2.0
"""Snowflake asynchronous-query adapter."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from types import ModuleType
from typing import Protocol, cast
from uuid import uuid4

from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.output import OutputKind, OutputRef
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.explain import ExplainResult
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import Column, DatabaseSchema, Table
from gantry.sql.target import SQLTarget


class _Description(Protocol):
    name: str


class _Cursor(Protocol):
    sfqid: str
    description: Sequence[_Description | Sequence[object]] | None

    def execute(self, sql: str, params: object = None) -> _Cursor: ...

    def execute_async(self, sql: str) -> _Cursor: ...

    def fetchall(self) -> list[Sequence[object]]: ...

    def fetchmany(self, count: int) -> list[Sequence[object]]: ...

    def get_results_from_sfqid(self, query_id: str) -> None: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...

    def get_query_status(self, query_id: str) -> object: ...

    def is_still_running(self, status: object) -> bool: ...

    def is_an_error(self, status: object) -> bool: ...

    def close(self) -> None: ...


class SnowflakeAdapter:
    """Reconnects by Snowflake query ID and bounds materialized agent output."""

    def __init__(self, target: SQLTarget) -> None:
        module: ModuleType
        try:
            module = importlib.import_module("snowflake.connector")
        except ImportError as error:
            raise ImportError(
                'Snowflake support requires `pip install "data-gantry[snowflake]"`'
            ) from error
        self._connect = cast(Callable[..., _Connection], module.connect)
        self._target = target
        read_only = target.config.get("read_only", False)
        if not isinstance(read_only, bool):
            raise ValueError("Snowflake read_only must be a boolean")
        self._read_only = read_only

    def capabilities(self) -> SQLCapabilities:
        return SQLCapabilities(
            describe_schema=True,
            explain=True,
            async_jobs=True,
            reconnect=True,
            cancellation=True,
            read_only_session=self._read_only,
            write_execution=not self._read_only,
            statement_timeout=True,
            row_limit=True,
            query_metrics=True,
            result_reference=True,
        )

    async def describe(self, target: SQLTarget) -> DatabaseSchema:
        return await asyncio.to_thread(self._describe_sync)

    async def validate(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
        policy: SQLPolicy,
    ) -> ValidationResult:
        try:
            await asyncio.to_thread(self._explain_sync, sql)
        except Exception as error:
            return ValidationResult.rejected(str(error))
        return ValidationResult.accepted()

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult:
        rows = await asyncio.to_thread(self._explain_sync, sql)
        return ExplainResult(supported=True, native={"rows": rows})

    async def submit(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
    ) -> ExecutionHandle:
        policy = context.metadata.get("gantry.sql.policy")
        if not isinstance(policy, SQLPolicy):
            raise ValueError("governed SQL policy is missing from execution context")
        query_id = await asyncio.to_thread(self._submit_sync, sql, policy)
        return ExecutionHandle(
            f"run_{uuid4().hex}",
            "sql",
            target.provider,
            query_id,
            metadata={"max_rows": policy.max_rows},
        )

    async def status(self, handle: ExecutionHandle) -> Execution:
        try:
            status, running, status_error = await asyncio.to_thread(
                self._status_sync, handle.native_id
            )
        except Exception as error:
            return _unknown(handle, str(error))
        normalized = status.upper()
        if "ABORT" in normalized or "CANCEL" in normalized:
            return Execution(
                handle,
                ExecutionState.CANCELLED,
                failure=Failure(FailureKind.CANCELLED, False, f"Snowflake status: {status}"),
                native={"status": status},
            )
        if status_error:
            return Execution(
                handle,
                ExecutionState.FAILED,
                failure=Failure(FailureKind.ENGINE_ERROR, False, f"Snowflake status: {status}"),
                native={"status": status},
            )
        if running:
            return Execution(
                handle,
                ExecutionState.RUNNING,
                started_at=handle.submitted_at,
                native={"status": status},
            )
        return Execution(
            handle,
            ExecutionState.SUCCEEDED,
            started_at=handle.submitted_at,
            updated_at=datetime.now(UTC),
            native={"status": status},
        )

    async def result(self, handle: ExecutionHandle) -> ExecutionResult:
        maximum = handle.metadata.get("max_rows", 1_000)
        max_rows = maximum if isinstance(maximum, int) else 1_000
        try:
            inline = await asyncio.to_thread(self._result_sync, handle.native_id, max_rows)
        except Exception as error:
            return ExecutionResult.failed(handle, _failure(error))
        outputs = (
            OutputRef(
                OutputKind.INLINE,
                f"inline://{handle.gantry_id}",
                metadata={"inline": inline},
            ),
            OutputRef(OutputKind.CUSTOM, f"snowflake-query://{handle.native_id}"),
        )
        return ExecutionResult.succeeded(
            handle,
            outputs=outputs,
            metrics=ExecutionMetrics(rows_read=len(inline.rows)),
        )

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        try:
            await asyncio.to_thread(self._cancel_sync, handle.native_id)
        except Exception as error:
            return _unknown(handle, str(error))
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, f"Snowflake query cancelled ({mode})"),
        )

    def _open(self) -> _Connection:
        config = dict(self._target.config)
        config.pop("read_only", None)
        return self._connect(**config)

    def _describe_sync(self) -> DatabaseSchema:
        connection = self._open()
        cursor = connection.cursor()
        try:
            schema_filter = self._target.config.get("schema")
            sql = """
                SELECT table_catalog, table_schema, table_name, data_type,
                       column_name, is_nullable
                FROM information_schema.columns
            """
            params: tuple[object, ...] = ()
            if isinstance(schema_filter, str):
                sql += " WHERE table_schema = %s"
                params = (schema_filter,)
            sql += " ORDER BY table_catalog, table_schema, table_name, ordinal_position"
            rows = cursor.execute(sql, params).fetchall()
        finally:
            cursor.close()
            connection.close()
        grouped: dict[tuple[str, str, str], list[Column]] = {}
        for catalog, schema, table, data_type, name, nullable in rows:
            key = (str(catalog), str(schema), str(table))
            grouped.setdefault(key, []).append(
                Column(str(name), str(data_type), str(nullable).upper() == "YES")
            )
        tables = tuple(
            Table(name, schema, catalog, tuple(columns))
            for (catalog, schema, name), columns in grouped.items()
        )
        return DatabaseSchema(
            catalogs=tuple(dict.fromkeys(table.catalog for table in tables if table.catalog)),
            schemas=tuple(dict.fromkeys(table.schema for table in tables if table.schema)),
            tables=tables,
        )

    def _explain_sync(self, sql: str) -> tuple[tuple[object, ...], ...]:
        connection = self._open()
        cursor = connection.cursor()
        try:
            rows = cursor.execute(f"EXPLAIN USING TEXT {sql}").fetchall()
            return tuple(tuple(value for value in row) for row in rows)
        finally:
            cursor.close()
            connection.close()

    def _submit_sync(self, sql: str, policy: SQLPolicy) -> str:
        connection = self._open()
        cursor = connection.cursor()
        try:
            seconds = max(1, int(policy.timeout_seconds))
            cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {seconds}")
            cursor.execute("ALTER SESSION SET ABORT_DETACHED_QUERY = FALSE")
            cursor.execute_async(sql)
            query_id = cursor.sfqid
            if not query_id:
                raise RuntimeError("Snowflake did not return a query ID")
            return query_id
        finally:
            cursor.close()
            connection.close()

    def _status_sync(self, query_id: str) -> tuple[str, bool, bool]:
        connection = self._open()
        try:
            status = connection.get_query_status(query_id)
            return (
                str(status),
                connection.is_still_running(status),
                connection.is_an_error(status),
            )
        finally:
            connection.close()

    def _result_sync(self, query_id: str, max_rows: int) -> InlineRows:
        connection = self._open()
        cursor = connection.cursor()
        try:
            cursor.get_results_from_sfqid(query_id)
            rows = cursor.fetchmany(max_rows + 1)
            columns = _columns(cursor.description or ())
            bounded = tuple(tuple(value for value in row) for row in rows[:max_rows])
            return InlineRows(columns, bounded, truncated=len(rows) > max_rows)
        finally:
            cursor.close()
            connection.close()

    def _cancel_sync(self, query_id: str) -> None:
        connection = self._open()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT SYSTEM$CANCEL_QUERY(%s)", (query_id,))
        finally:
            cursor.close()
            connection.close()


def _columns(description: Sequence[_Description | Sequence[object]]) -> tuple[str, ...]:
    columns: list[str] = []
    for value in description:
        name = getattr(value, "name", None)
        if isinstance(name, str):
            columns.append(name)
        else:
            sequence = cast(Sequence[object], value)
            columns.append(str(sequence[0]))
    return tuple(columns)


def _failure(error: Exception) -> Failure:
    message = str(error)
    lowered = message.lower()
    if "auth" in lowered or "privilege" in lowered:
        kind = FailureKind.AUTH_ERROR
    elif "does not exist" in lowered:
        kind = FailureKind.OBJECT_NOT_FOUND
    elif "syntax" in lowered:
        kind = FailureKind.SYNTAX_ERROR
    elif "timeout" in lowered:
        kind = FailureKind.TIMEOUT
    else:
        kind = FailureKind.ENGINE_ERROR
    return Failure(kind, kind is FailureKind.TIMEOUT, message, native_message=message)


def _unknown(handle: ExecutionHandle, message: str) -> Execution:
    return Execution(
        handle,
        ExecutionState.UNKNOWN,
        failure=Failure(FailureKind.UNKNOWN, False, message),
    )
