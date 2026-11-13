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
from gantry.sql.affected import (
    DRY_RUN,
    DRY_RUN_CEILING,
    DRY_RUN_PREVIEW,
    ROW_WRITES,
    AffectedRowLimitError,
    DryRunComplete,
    check,
    preview_rows,
)
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.dialect import ConservativeDialect
from gantry.sql.explain import ExplainResult
from gantry.sql.materialization import MaterializationPlan, TableRef
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import Column, DatabaseSchema, Table
from gantry.sql.target import SQLTarget


class _Cursor(Protocol):
    @property
    def description(self) -> Sequence[Sequence[object]] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

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
            raise ImportError(
                'DuckDB support requires `pip install "data-gantry[duckdb]"`'
            ) from error
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
            affected_row_limit=not self._read_only,
            returning=not self._read_only,
            query_metrics=True,
            create_table_as=not self._read_only,
            create_view_as=not self._read_only,
            destination_introspection=True,
            materialization_reference=not self._read_only,
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

    async def inspect_table(
        self,
        reference: TableRef,
        target: SQLTarget,
        *,
        include_row_count: bool = False,
    ) -> Table | None:
        schema = await self.describe(target)
        table = next(
            (
                candidate
                for candidate in schema.tables
                if candidate.name.lower() == reference.name.lower()
                and (
                    reference.schema is None
                    or (candidate.schema or "").lower() == reference.schema.lower()
                )
                and (
                    reference.catalog is None
                    or (candidate.catalog or "").lower() == reference.catalog.lower()
                )
            ),
            None,
        )
        if table is None:
            return None
        if not include_row_count:
            return table
        rows = await self._query_all(f"SELECT COUNT(*) FROM {_quoted_ref(reference)}")
        count_value = rows[0][0]
        if not isinstance(count_value, int) or isinstance(count_value, bool):
            raise TypeError("DuckDB returned a non-integer row count")
        count = count_value
        return Table(
            table.name,
            table.schema,
            table.catalog,
            table.columns,
            table.primary_key,
            table.kind,
            {**table.metadata, "rows": count},
        )

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
        metadata: dict[str, object] = {
            "database": target.config.get("path", ":memory:"),
            "max_rows": policy.max_rows,
            "timeout_seconds": policy.timeout_seconds,
        }
        plan = context.metadata.get("gantry.sql.materialization.plan")
        if isinstance(plan, MaterializationPlan):
            metadata.update(
                {
                    "gantry.sql.materialization.destination": plan.destination.qualified_name,
                    "gantry.sql.materialization.operation": plan.operation.value,
                }
            )
        if context.metadata.get(DRY_RUN) is True:
            metadata[DRY_RUN] = True
            preview = context.metadata.get(DRY_RUN_PREVIEW)
            if isinstance(preview, int) and not isinstance(preview, bool) and preview > 0:
                metadata[DRY_RUN_PREVIEW] = preview
        handle = ExecutionHandle(
            gantry_id=gantry_id,
            engine="sql",
            target=target.provider,
            native_id=f"duckdb_{uuid4().hex}",
            metadata=metadata,
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
        classification = ConservativeDialect().classify(sql)
        writes = classification.operation in ROW_WRITES
        rows_written: int | None = None
        try:
            async with self._lock:
                if writes:
                    rows_written, inline = await asyncio.wait_for(
                        asyncio.to_thread(
                            self._execute_write,
                            sql,
                            policy,
                            classification.returning,
                            handle.metadata.get(DRY_RUN) is True,
                            _preview_size(handle.metadata.get(DRY_RUN_PREVIEW)),
                        ),
                        timeout=policy.timeout_seconds,
                    )
                else:
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
        except AffectedRowLimitError as error:
            # `_execute_write` rolled the transaction back before raising.
            return ExecutionResult.failed(handle, error.failure())
        except DryRunComplete as counted:
            return ExecutionResult.succeeded(
                handle,
                outputs=(
                    ()
                    if counted.preview is None
                    else (
                        OutputRef(
                            OutputKind.INLINE,
                            f"inline://{handle.gantry_id}",
                            metadata={"inline": counted.preview},
                        ),
                    )
                ),
                metrics=ExecutionMetrics(
                    rows_written=counted.affected,
                    runtime_seconds=asyncio.get_running_loop().time() - started,
                    native={"dry_run": True, "at_least": counted.at_least},
                ),
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
        destination = handle.metadata.get("gantry.sql.materialization.destination")
        if isinstance(destination, str):
            output = OutputRef(
                OutputKind.TABLE,
                f"duckdb://{destination.replace('.', '/')}",
                metadata={"object_kind": _materialization_kind(handle)},
            )
        else:
            output = OutputRef(
                OutputKind.INLINE,
                f"inline://{handle.gantry_id}",
                metadata={"inline": inline},
            )
        return ExecutionResult.succeeded(
            handle,
            # A write that returns no rows has no inline result to hand back.
            outputs=() if writes and not classification.returning else (output,),
            metrics=ExecutionMetrics(
                rows_read=None if writes and not classification.returning else len(inline.rows),
                rows_written=rows_written,
                runtime_seconds=runtime,
            ),
        )

    def _execute_write(
        self,
        sql: str,
        policy: SQLPolicy,
        returning: bool,
        dry_run: bool = False,
        preview: int = 0,
    ) -> tuple[int, InlineRows]:
        """Run a row write in a transaction, and undo it if it changed too much.

        DuckDB answers a write with one row holding the count, unless the
        statement has `RETURNING`, in which case the changed rows come back and
        are the count. Reading one past the limit is enough to know it was
        exceeded.
        """
        limit = policy.max_rows_affected
        self._connection.execute("BEGIN TRANSACTION")
        try:
            cursor = self._connection.execute(sql)
            if dry_run:
                if returning:
                    seen = cursor.fetchmany(DRY_RUN_CEILING + 1)
                    raise DryRunComplete(
                        min(len(seen), DRY_RUN_CEILING),
                        at_least=len(seen) > DRY_RUN_CEILING,
                        preview=preview_rows(
                            tuple(str(column[0]) for column in (cursor.description or ())),
                            seen,
                            preview,
                        ),
                    )
                row = cursor.fetchone()
                raise DryRunComplete(int(row[0]) if row and isinstance(row[0], int) else 0)
            if returning:
                columns = tuple(str(column[0]) for column in (cursor.description or ()))
                fetched = cursor.fetchall() if limit is None else cursor.fetchmany(limit + 1)
                if limit is not None and len(fetched) > limit:
                    raise AffectedRowLimitError(limit)
                affected = len(fetched)
                inline = InlineRows(
                    columns,
                    tuple(fetched[: policy.max_rows]),
                    truncated=len(fetched) > policy.max_rows,
                )
            else:
                counted = cursor.fetchone()
                affected = int(counted[0]) if counted and isinstance(counted[0], int) else 0
                check(limit, affected)
                inline = InlineRows((), ())
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        self._connection.execute("COMMIT")
        return affected, inline

    def _execute_bounded(self, sql: str, max_rows: int) -> InlineRows:
        cursor = self._connection.execute(sql)
        description = cursor.description or ()
        columns = tuple(str(column[0]) for column in description)
        fetched = cursor.fetchmany(max_rows + 1) if description else []
        return InlineRows(columns, tuple(fetched[:max_rows]), truncated=len(fetched) > max_rows)

    async def _query_all(self, sql: str) -> list[tuple[object, ...]]:
        async with self._lock:
            return await asyncio.to_thread(lambda: self._connection.execute(sql).fetchall())


def _quoted_ref(reference: TableRef) -> str:
    return ".".join(
        f'"{part.replace(chr(34), chr(34) * 2)}"'
        for part in (reference.catalog, reference.schema, reference.name)
        if part is not None
    )


def _materialization_kind(handle: ExecutionHandle) -> str:
    operation = handle.metadata.get("gantry.sql.materialization.operation")
    return "view" if operation == "CREATE_VIEW_AS" else "table"


def _preview_size(value: object) -> int:
    """How many counted rows a dry run should keep: a small positive int, or none."""
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 0
