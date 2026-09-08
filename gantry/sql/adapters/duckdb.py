# SPDX-License-Identifier: Apache-2.0
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
from gantry.sql.adapter import SQLAdapter
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.explain import ExplainResult
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import Column, DatabaseSchema, Table
from gantry.sql.target import SQLTarget


class _Cursor(Protocol):
    @property
    def description(self) -> Sequence[Sequence[object]] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def fetchmany(self, size: int) -> list[tuple[object, ...]]: ...


class _Connection(Protocol):
    def execute(self, query: str) -> _Cursor: ...

    def interrupt(self) -> None: ...


class DuckDBAdapter(SQLAdapter):
    """Local DuckDB adapter with bounded rows and interruptible execution."""

    def __init__(self, target: SQLTarget) -> None:
        module: ModuleType
        try:
            module = importlib.import_module("duckdb")
        except ImportError as error:
            raise ImportError('DuckDB support requires `pip install "gantry[duckdb]"`') from error
        connect = cast(Callable[..., _Connection], module.connect)
        path = target.config.get("path", ":memory:")
        if not isinstance(path, str):
            raise ValueError("DuckDB path must be a string")
        read_only = target.config.get("read_only", False)
        if not isinstance(read_only, bool):
            raise ValueError("DuckDB read_only must be a boolean")
        self._connection = connect(path, read_only=read_only)
        self._read_only = read_only
        self._lock = asyncio.Lock()
        self._jobs: dict[str, asyncio.Task[ExecutionResult]] = {}

    def capabilities(self) -> SQLCapabilities:
        return SQLCapabilities(
            describe_schema=True,
            explain=True,
            async_jobs=False,
            reconnect=False,
            cancellation=True,
            read_only_session=self._read_only,
            write_execution=not self._read_only,
            statement_timeout=True,
            row_limit=True,
            query_metrics=True,
        )

    async def describe(self, target: SQLTarget) -> DatabaseSchema:
        rows = await self._query_all(
            """
            SELECT c.table_catalog, c.table_schema, c.table_name,
                   COALESCE(t.table_type, 'BASE TABLE'),
                   c.column_name, c.data_type, c.is_nullable
            FROM information_schema.columns AS c
            LEFT JOIN information_schema.tables AS t
              ON t.table_catalog = c.table_catalog
             AND t.table_schema = c.table_schema
             AND t.table_name = c.table_name
            ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position
            """
        )
        grouped: dict[tuple[str, str, str, str], list[Column]] = {}
        for catalog, schema, table, kind, name, data_type, nullable in rows:
            key = (str(catalog), str(schema), str(table), str(kind).lower())
            grouped.setdefault(key, []).append(
                Column(str(name), str(data_type), str(nullable).upper() == "YES")
            )
        tables = tuple(
            Table(name, schema, catalog, tuple(columns), kind=kind)
            for (catalog, schema, name, kind), columns in grouped.items()
        )
        return DatabaseSchema(
            catalogs=tuple(dict.fromkeys(table.catalog for table in tables if table.catalog)),
            schemas=tuple(dict.fromkeys(table.schema for table in tables if table.schema)),
            tables=tables,
        )

    async def validate(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
        policy: SQLPolicy,
    ) -> ValidationResult:
        try:
            await self._query_all(f"EXPLAIN {sql}")
        except Exception as error:
            return ValidationResult.rejected(str(error))
        return ValidationResult.accepted()

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult:
        rows = await self._query_all(f"EXPLAIN {sql}")
        return ExplainResult(supported=True, native={"rows": tuple(rows)})

    async def submit(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
    ) -> ExecutionHandle:
        policy = context.metadata.get("gantry.sql.policy")
        if not isinstance(policy, SQLPolicy):
            raise ValueError("governed SQL policy is missing from execution context")
        gantry_id = f"run_{uuid4().hex}"
        handle = ExecutionHandle(
            gantry_id=gantry_id,
            engine="sql",
            target=target.provider,
            native_id=f"duckdb_{uuid4().hex}",
            metadata={"database": target.config.get("path", ":memory:")},
        )
        self._jobs[gantry_id] = asyncio.create_task(self._execute(handle, sql, policy))
        return handle

    async def status(self, handle: ExecutionHandle) -> Execution:
        task = self._jobs.get(handle.gantry_id)
        if task is None:
            return Execution(
                handle,
                ExecutionState.UNKNOWN,
                failure=Failure(FailureKind.UNKNOWN, False, "DuckDB job is not available"),
            )
        if not task.done():
            return Execution(handle, ExecutionState.RUNNING, started_at=handle.submitted_at)
        result = await task
        if result.ok:
            return Execution(
                handle,
                ExecutionState.SUCCEEDED,
                started_at=handle.submitted_at,
                updated_at=datetime.now(UTC),
                metrics=result.metrics,
            )
        state = (
            ExecutionState.CANCELLED
            if result.failure is not None and result.failure.kind is FailureKind.CANCELLED
            else ExecutionState.FAILED
        )
        return Execution(
            handle,
            state,
            started_at=handle.submitted_at,
            updated_at=datetime.now(UTC),
            metrics=result.metrics,
            failure=result.failure,
        )

    async def result(self, handle: ExecutionHandle) -> ExecutionResult:
        task = self._jobs.get(handle.gantry_id)
        if task is None:
            return ExecutionResult.failed(
                handle,
                Failure(FailureKind.UNKNOWN, False, "DuckDB job is not available"),
            )
        return await task

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        task = self._jobs.get(handle.gantry_id)
        if task is not None and not task.done():
            self._connection.interrupt()
            task.cancel()
        failure = Failure(FailureKind.CANCELLED, False, f"DuckDB job cancelled ({mode})")
        return Execution(handle, ExecutionState.CANCELLED, failure=failure)

    async def _execute(
        self,
        handle: ExecutionHandle,
        sql: str,
        policy: SQLPolicy,
    ) -> ExecutionResult:
        started = asyncio.get_running_loop().time()
        try:
            async with self._lock:
                inline = await asyncio.wait_for(
                    asyncio.to_thread(self._execute_bounded, sql, policy.max_rows),
                    timeout=policy.timeout_seconds,
                )
        except TimeoutError:
            self._connection.interrupt()
            return ExecutionResult.failed(
                handle,
                Failure(FailureKind.TIMEOUT, True, "DuckDB statement timed out"),
            )
        except asyncio.CancelledError:
            return ExecutionResult.failed(
                handle,
                Failure(FailureKind.CANCELLED, False, "DuckDB statement was cancelled"),
            )
        except Exception as error:
            return ExecutionResult.failed(
                handle,
                Failure(
                    FailureKind.ENGINE_ERROR,
                    False,
                    str(error),
                    native_message=str(error),
                ),
            )
        runtime = asyncio.get_running_loop().time() - started
        output = OutputRef(
            OutputKind.INLINE,
            f"inline://{handle.gantry_id}",
            metadata={"inline": inline},
        )
        return ExecutionResult.succeeded(
            handle,
            outputs=(output,),
            metrics=ExecutionMetrics(rows_read=len(inline.rows), runtime_seconds=runtime),
        )

    def _execute_bounded(self, sql: str, max_rows: int) -> InlineRows:
        cursor = self._connection.execute(sql)
        description = cursor.description or ()
        columns = tuple(str(column[0]) for column in description)
        fetched = cursor.fetchmany(max_rows + 1) if description else []
        return InlineRows(columns, tuple(fetched[:max_rows]), truncated=len(fetched) > max_rows)

    async def _query_all(self, sql: str) -> list[tuple[object, ...]]:
        async with self._lock:
            return await asyncio.to_thread(lambda: self._connection.execute(sql).fetchall())
