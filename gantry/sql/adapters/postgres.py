# SPDX-License-Identifier: Apache-2.0
"""Shared PostgreSQL-family adapter used by Postgres, Neon, and Supabase."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from types import ModuleType
from typing import Protocol, cast
from urllib.parse import urlsplit
from uuid import uuid4

from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.output import OutputKind, OutputRef
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import ConservativeDialect
from gantry.sql.explain import ExplainResult
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import Column, DatabaseSchema, Table
from gantry.sql.target import SQLTarget


class _Cursor(Protocol):
    async def fetch(self, count: int) -> Sequence[Sequence[object]]: ...


class _Attribute(Protocol):
    name: str


class _PreparedStatement(Protocol):
    def get_attributes(self) -> Sequence[_Attribute]: ...

    def cursor(self) -> Awaitable[_Cursor]: ...


class _Transaction(Protocol):
    async def __aenter__(self) -> object: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> bool | None: ...


class _Connection(Protocol):
    def transaction(self, *, readonly: bool = False) -> _Transaction: ...

    async def execute(self, query: str) -> str: ...

    async def fetch(self, query: str) -> Sequence[Sequence[object]]: ...

    async def prepare(self, query: str) -> _PreparedStatement: ...

    async def close(self) -> None: ...


class PostgresAdapter:
    """PostgreSQL execution with per-query read-only transactions and bounds."""

    def __init__(self, target: SQLTarget) -> None:
        module: ModuleType
        try:
            module = importlib.import_module("asyncpg")
        except ImportError as error:
            raise ImportError(
                'PostgreSQL support requires `pip install "gantry[postgres]"`'
            ) from error
        self._connect = cast(Callable[..., Awaitable[_Connection]], module.connect)
        self._target = target
        self._jobs: dict[str, asyncio.Task[ExecutionResult]] = {}
        self._connections: dict[str, _Connection] = {}

    def capabilities(self) -> SQLCapabilities:
        return SQLCapabilities(
            describe_schema=True,
            explain=True,
            cancellation=True,
            read_only_session=True,
            statement_timeout=True,
            row_limit=True,
            query_metrics=True,
        )

    async def describe(self, target: SQLTarget) -> DatabaseSchema:
        connection = await self._open()
        try:
            rows = await connection.fetch(
                """
                SELECT c.table_catalog, c.table_schema, c.table_name, t.table_type,
                       c.column_name, c.data_type, c.is_nullable
                FROM information_schema.columns AS c
                JOIN information_schema.tables AS t
                  ON t.table_catalog = c.table_catalog
                 AND t.table_schema = c.table_schema
                 AND t.table_name = c.table_name
                WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position
                """
            )
        finally:
            await connection.close()
        grouped: dict[tuple[str, str, str, str], list[Column]] = {}
        for row in rows:
            catalog, schema, table, kind, name, data_type, nullable = row
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
        connection = await self._open()
        try:
            async with connection.transaction(readonly=policy.read_only):
                await self._set_timeout(connection, policy)
                await connection.fetch(f"EXPLAIN (FORMAT JSON) {sql}")
        except Exception as error:
            return ValidationResult.rejected(str(error))
        finally:
            await connection.close()
        return ValidationResult.accepted()

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult:
        connection = await self._open()
        try:
            async with connection.transaction(readonly=True):
                rows = await connection.fetch(f"EXPLAIN (FORMAT JSON) {sql}")
        finally:
            await connection.close()
        native = tuple(tuple(value for value in row) for row in rows)
        return ExplainResult(supported=True, native={"rows": native})

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
            gantry_id,
            "sql",
            target.provider,
            f"postgres_{uuid4().hex}",
            metadata={"provider": target.provider},
        )
        self._jobs[gantry_id] = asyncio.create_task(self._execute(handle, sql, policy))
        return handle

    async def status(self, handle: ExecutionHandle) -> Execution:
        task = self._jobs.get(handle.gantry_id)
        if task is None:
            return _unknown(handle, "PostgreSQL job is not available")
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
            failure=result.failure,
        )

    async def result(self, handle: ExecutionHandle) -> ExecutionResult:
        task = self._jobs.get(handle.gantry_id)
        if task is None:
            return ExecutionResult.failed(
                handle,
                Failure(FailureKind.UNKNOWN, False, "PostgreSQL job is not available"),
            )
        return await task

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        connection = self._connections.get(handle.gantry_id)
        task = self._jobs.get(handle.gantry_id)
        if connection is not None:
            await connection.close()
        if task is not None and not task.done():
            task.cancel()
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, f"PostgreSQL job cancelled ({mode})"),
        )

    async def _execute(
        self,
        handle: ExecutionHandle,
        sql: str,
        policy: SQLPolicy,
    ) -> ExecutionResult:
        started = asyncio.get_running_loop().time()
        outputs: tuple[OutputRef, ...]
        try:
            connection = await self._open()
            self._connections[handle.gantry_id] = connection
            async with connection.transaction(readonly=policy.read_only):
                await self._set_timeout(connection, policy)
                operation = ConservativeDialect().classify(sql).operation
                if operation is SQLOperation.SELECT:
                    statement = await connection.prepare(sql)
                    columns = tuple(attribute.name for attribute in statement.get_attributes())
                    cursor = await statement.cursor()
                    records = await cursor.fetch(policy.max_rows + 1)
                    rows = tuple(
                        tuple(value for value in row) for row in records[: policy.max_rows]
                    )
                    inline = InlineRows(columns, rows, truncated=len(records) > policy.max_rows)
                    output = OutputRef(
                        OutputKind.INLINE,
                        f"inline://{handle.gantry_id}",
                        metadata={"inline": inline},
                    )
                    outputs = (output,)
                    rows_read = len(rows)
                else:
                    await connection.execute(sql)
                    outputs = ()
                    rows_read = None
        except asyncio.CancelledError:
            return ExecutionResult.failed(
                handle, Failure(FailureKind.CANCELLED, False, "PostgreSQL statement was cancelled")
            )
        except Exception as error:
            return ExecutionResult.failed(handle, _failure(error))
        finally:
            final_connection = self._connections.get(handle.gantry_id)
            if final_connection is not None:
                del self._connections[handle.gantry_id]
                with suppress(Exception):
                    await final_connection.close()
        runtime = asyncio.get_running_loop().time() - started
        return ExecutionResult.succeeded(
            handle,
            outputs=outputs,
            metrics=ExecutionMetrics(rows_read=rows_read, runtime_seconds=runtime),
        )

    async def _open(self) -> _Connection:
        config = dict(self._target.config)
        url = config.pop("url", None)
        config.pop("read_only", None)
        if isinstance(url, str):
            _apply_transaction_pooling(self._target.provider, url, config)
            return await self._connect(url, **config)
        return await self._connect(**config)

    @staticmethod
    async def _set_timeout(connection: _Connection, policy: SQLPolicy) -> None:
        milliseconds = max(1, int(policy.timeout_seconds * 1_000))
        await connection.execute(f"SET LOCAL statement_timeout = {milliseconds}")


def _failure(error: Exception) -> Failure:
    name = type(error).__name__.lower()
    if "syntax" in name:
        kind = FailureKind.SYNTAX_ERROR
    elif "permission" in name or "authorization" in name or "authentication" in name:
        kind = FailureKind.AUTH_ERROR
    elif "undefined" in name:
        kind = FailureKind.OBJECT_NOT_FOUND
    elif "timeout" in name:
        kind = FailureKind.TIMEOUT
    else:
        kind = FailureKind.ENGINE_ERROR
    return Failure(kind, kind is FailureKind.TIMEOUT, str(error), native_message=str(error))


def _unknown(handle: ExecutionHandle, message: str) -> Execution:
    return Execution(
        handle,
        ExecutionState.UNKNOWN,
        failure=Failure(FailureKind.UNKNOWN, False, message),
    )


# Providers whose transaction-pooling endpoint cannot carry server-side prepared
# statements, and the port that endpoint listens on.
_TRANSACTION_POOLER_PORT = {"supabase": 6543}


def _apply_transaction_pooling(provider: str, url: str, config: dict[str, object]) -> None:
    """Disable the statement cache when the endpoint pools by transaction.

    asyncpg prepares every statement server-side. A transaction-pooled endpoint
    hands the next statement to whichever backend is free, which is often not
    the one that did the preparing, and asyncpg then fails with `prepared
    statement does not exist`.

    This is worth doing here rather than leaving to the caller because of how
    badly it hides. Forty concurrent queries against Supabase's `:6543` passed
    with the cache on, and so did one connection reused across twenty-five
    transactions — and then an ordinary `describe()` failed on the next run.
    Whether it bites depends on which backend the pooler happens to hand you,
    so a green test is not evidence and only the pooling mode is.

    An explicit `statement_cache_size` always wins: a caller who has measured
    their own deployment is better informed than this rule.
    """
    if "statement_cache_size" in config:
        return
    port = _TRANSACTION_POOLER_PORT.get(provider)
    if port is None:
        return
    try:
        if urlsplit(url).port == port:
            config["statement_cache_size"] = 0
    except ValueError:
        # An unparseable port is the connect call's problem to report, not this
        # helper's to guess about.
        return
