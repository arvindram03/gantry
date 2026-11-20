# SPDX-License-Identifier: Apache-2.0
"""Shared PostgreSQL-family adapter used by Postgres, Neon, and Supabase."""

from __future__ import annotations

import asyncio
import importlib
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
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
from gantry.sql.affected import (
    DRY_RUN,
    DRY_RUN_CEILING,
    DRY_RUN_PREVIEW,
    ROW_WRITES,
    AffectedRowLimitError,
    DryRunComplete,
    check,
    from_command_tag,
    preview_rows,
)
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import ConservativeDialect
from gantry.sql.explain import ExplainResult
from gantry.sql.load import RowLoad, load_of
from gantry.sql.materialization import (
    _DESTINATION_METADATA,
    _OPERATION_METADATA,
    _PLAN_METADATA,
    MaterializationPlan,
    TableRef,
)
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import Column, DatabaseSchema, Table
from gantry.sql.schema_change import SCHEMA_CHANGE
from gantry.sql.snapshot import Column as SnapshotColumn
from gantry.sql.snapshot import ObjectKind, SchemaObject, SchemaSnapshot
from gantry.sql.target import SQLTarget


class _Cursor(Protocol):
    async def fetch(self, count: int) -> Sequence[Sequence[object]]: ...


class _Attribute(Protocol):
    name: str


class _Type(Protocol):
    name: str


class _PreparedStatement(Protocol):
    def get_attributes(self) -> Sequence[_Attribute]: ...

    def get_parameters(self) -> Sequence[_Type]: ...

    async def executemany(self, args: Sequence[Sequence[object]]) -> None: ...

    def get_statusmsg(self) -> str: ...

    def cursor(self) -> Awaitable[_Cursor]: ...

    async def fetch(self) -> Sequence[Sequence[object]]: ...


class _Transaction(Protocol):
    async def start(self) -> None: ...

    async def rollback(self) -> None: ...

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

    async def fetch(self, query: str, *args: object) -> Sequence[Sequence[object]]: ...

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
                'PostgreSQL support requires `pip install "data-gantry[postgres]"`'
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
            affected_row_limit=True,
            returning=True,
            row_loads=True,
            schema_snapshot=True,
            schema_changes=True,
            query_metrics=True,
            create_table_as=True,
            create_view_as=True,
            destination_introspection=True,
            materialization_reference=True,
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
            # Each table's primary key, in key order. A view has none.
            keyed = await connection.fetch(
                """
                SELECT k.table_schema, k.table_name, k.column_name
                FROM information_schema.table_constraints AS c
                JOIN information_schema.key_column_usage AS k
                  ON k.constraint_schema = c.constraint_schema
                 AND k.constraint_name = c.constraint_name
                 AND k.table_schema = c.table_schema
                 AND k.table_name = c.table_name
                WHERE c.constraint_type = 'PRIMARY KEY'
                ORDER BY k.table_schema, k.table_name, k.ordinal_position
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
        keys: dict[tuple[str, str], list[str]] = {}
        for schema, table, column in keyed:
            keys.setdefault((str(schema), str(table)), []).append(str(column))
        tables = tuple(
            Table(
                name,
                schema,
                catalog,
                tuple(columns),
                primary_key=tuple(keys.get((schema, name), ())),
                kind=kind,
            )
            for (catalog, schema, name, kind), columns in grouped.items()
        )
        return DatabaseSchema(
            catalogs=tuple(dict.fromkeys(table.catalog for table in tables if table.catalog)),
            schemas=tuple(dict.fromkeys(table.schema for table in tables if table.schema)),
            tables=tables,
        )

    async def snapshot(self, schemas: Sequence[str]) -> SchemaSnapshot:
        """Every object in `schemas`, with PostgreSQL's own definition of each.

        Read in one read-only transaction, so the objects are of one moment.
        Objects an extension created are left to the extension: it makes them
        again when it is created.
        """
        wanted = list(schemas)
        connection = await self._open()
        try:
            async with connection.transaction(readonly=True):
                rows = {
                    name: await connection.fetch(query, *([wanted] if "$1" in query else []))
                    for name, query in _SNAPSHOT_QUERIES.items()
                }
        finally:
            await connection.close()
        return SchemaSnapshot("postgres", tuple(wanted), _snapshot_objects(rows, wanted))

    async def inspect_table(
        self,
        reference: TableRef,
        target: SQLTarget,
        *,
        include_row_count: bool = False,
    ) -> Table | None:
        """Look a destination up by name, for create-only and verification.

        A targeted query rather than filtering `describe()`, because this runs
        twice per materialization — once to refuse a destination that already
        exists, once to check what was built — and `describe()` reads every
        column of every table in the database.
        """
        connection = await self._open()
        try:
            rows = await connection.fetch(
                f"""
                SELECT c.table_catalog, c.table_schema, t.table_type,
                       c.column_name, c.data_type, c.is_nullable
                FROM information_schema.columns AS c
                JOIN information_schema.tables AS t
                  ON t.table_catalog = c.table_catalog
                 AND t.table_schema = c.table_schema
                 AND t.table_name = c.table_name
                WHERE c.table_name = '{_escaped(reference.name)}'
                  {_schema_filter(reference)}
                ORDER BY c.ordinal_position
                """
            )
            if not rows:
                return None
            catalog, schema, kind = str(rows[0][0]), str(rows[0][1]), str(rows[0][2]).lower()
            columns = tuple(
                Column(str(name), str(data_type), str(nullable).upper() == "YES")
                for _, _, _, name, data_type, nullable in rows
            )
            metadata: dict[str, object] = {}
            if include_row_count:
                counted = await connection.fetch(
                    f"SELECT COUNT(*) FROM {_quoted(schema)}.{_quoted(reference.name)}"
                )
                count = counted[0][0]
                if not isinstance(count, int) or isinstance(count, bool):
                    raise TypeError("PostgreSQL returned a non-integer row count")
                # The key is `rows`; `gantry.verify.RowCount` reads that and
                # nothing else.
                metadata["rows"] = count
        finally:
            await connection.close()
        return Table(reference.name, schema, catalog, columns, kind=kind, metadata=metadata)

    async def column_null_rates(
        self,
        reference: TableRef,
        target: SQLTarget,
        columns: Sequence[str],
    ) -> Mapping[str, float]:
        """Measure how often each column is null, in one pass over the table.

        One aggregate per column in a single statement rather than a query
        each: the scan is the expensive part, and doing it once bounds what
        verification costs on a large destination.
        """
        if not columns:
            return {}
        schema = reference.schema or "public"
        projections = ", ".join(
            f"AVG(CASE WHEN {_quoted(column)} IS NULL THEN 1.0 ELSE 0.0 END)" for column in columns
        )
        connection = await self._open()
        try:
            rows = await connection.fetch(
                f"SELECT {projections} FROM {_quoted(schema)}.{_quoted(reference.name)}"
            )
        finally:
            await connection.close()
        if not rows:
            return {}
        # An empty table has no rows to be null in, and AVG returns NULL. Zero
        # would claim a measurement that was not taken, so it is left out and
        # the check reports itself unsupported.
        rates: dict[str, float] = {}
        for column, value in zip(columns, rows[0], strict=False):
            # AVG over numeric comes back as Decimal from asyncpg, not float.
            # Excluding it produced an empty measurement, which read downstream
            # as "this provider cannot measure null rates" — a wrong answer that
            # still failed closed, and so was easy to miss.
            if isinstance(value, Decimal) or (
                isinstance(value, (int, float)) and not isinstance(value, bool)
            ):
                rates[column] = float(value)
        return rates

    async def validate(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
        policy: SQLPolicy,
    ) -> ValidationResult:
        load = load_of(context.metadata)
        connection = await self._open()
        try:
            if load is not None:
                # Parsed and resolved, types and all, without running it.
                await connection.prepare(_load_statement(load))
            elif context.metadata.get(SCHEMA_CHANGE) is True:
                # Schema changes are transactional here: run it, roll it back.
                await self._dry_run(connection, sql, policy)
            elif _explainable(sql):
                async with connection.transaction(readonly=policy.read_only):
                    await self._set_timeout(connection, policy)
                    await connection.fetch(f"EXPLAIN (FORMAT JSON) {sql}")
            else:
                await self._dry_run(connection, sql, policy)
        except Exception as error:
            return ValidationResult.rejected(str(error))
        finally:
            await connection.close()
        return ValidationResult.accepted()

    async def _dry_run(self, connection: _Connection, sql: str, policy: SQLPolicy) -> None:
        """Run the statement in a transaction and roll it back.

        `EXPLAIN` covers `CREATE TABLE AS` but not `CREATE VIEW`, which is a
        syntax error there. PostgreSQL's DDL is transactional, so running the
        view definition and rolling it back resolves every name in it — a
        missing source is caught here rather than at execution — and leaves
        nothing behind. Only for statements that define rather than move data:
        dry-running a `CREATE TABLE AS` would copy the rows twice.
        """
        transaction = connection.transaction(readonly=False)
        await transaction.start()
        try:
            await self._set_timeout(connection, policy)
            await connection.execute(sql)
        finally:
            await transaction.rollback()

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
        metadata: dict[str, object] = {"provider": target.provider}
        plan = context.metadata.get(_PLAN_METADATA)
        if isinstance(plan, MaterializationPlan):
            metadata[_DESTINATION_METADATA] = plan.destination.qualified_name
            metadata[_OPERATION_METADATA] = plan.operation.value
        handle = ExecutionHandle(
            gantry_id,
            "sql",
            target.provider,
            f"postgres_{uuid4().hex}",
            metadata=metadata,
        )
        dry_run = context.metadata.get(DRY_RUN) is True
        preview = _preview_size(context.metadata.get(DRY_RUN_PREVIEW))
        self._jobs[gantry_id] = asyncio.create_task(
            self._execute(
                handle,
                sql,
                policy,
                dry_run=dry_run,
                preview=preview,
                load=load_of(context.metadata),
            )
        )
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
        *,
        dry_run: bool = False,
        preview: int = 0,
        load: RowLoad | None = None,
    ) -> ExecutionResult:
        started = asyncio.get_running_loop().time()
        outputs: tuple[OutputRef, ...]
        rows_read: int | None
        rows_written: int | None = None
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
                elif load is not None:
                    rows_written = await _load(connection, load, policy)
                    outputs = ()
                    rows_read = None
                elif operation in ROW_WRITES:
                    rows_written, returned = await _write(
                        connection, sql, policy, dry_run, preview=preview
                    )
                    outputs = (
                        ()
                        if returned is None
                        else (
                            OutputRef(
                                OutputKind.INLINE,
                                f"inline://{handle.gantry_id}",
                                metadata={"inline": returned},
                            ),
                        )
                    )
                    rows_read = None if returned is None else len(returned.rows)
                else:
                    await connection.execute(sql)
                    # A materialization has to come back as a reference to what
                    # it built, or nothing downstream can verify the destination.
                    outputs = _destination_output(handle)
                    rows_read = None
        except asyncio.CancelledError:
            return ExecutionResult.failed(
                handle, Failure(FailureKind.CANCELLED, False, "PostgreSQL statement was cancelled")
            )
        except AffectedRowLimitError as error:
            # Raised inside the transaction block above, so by here the
            # statement has been rolled back.
            return ExecutionResult.failed(handle, error.failure())
        except DryRunComplete as counted:
            # Likewise rolled back: the statement ran, was counted, and is gone.
            return ExecutionResult.succeeded(
                handle,
                outputs=_preview_output(handle, counted),
                metrics=ExecutionMetrics(
                    rows_written=counted.affected,
                    runtime_seconds=asyncio.get_running_loop().time() - started,
                    native={"dry_run": True, "at_least": counted.at_least},
                ),
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
            metrics=ExecutionMetrics(
                rows_read=rows_read, rows_written=rows_written, runtime_seconds=runtime
            ),
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


# PostgreSQL can EXPLAIN a `CREATE TABLE AS`, but `EXPLAIN CREATE VIEW` is a
# syntax error. Anything not explainable is validated by dry run instead.
_UNEXPLAINABLE = re.compile(r"\A\s*CREATE\s+(OR\s+REPLACE\s+)?(RECURSIVE\s+)?VIEW\b", re.I)


def _explainable(sql: str) -> bool:
    return _UNEXPLAINABLE.search(sql) is None


def _escaped(value: str) -> str:
    """Single-quote escaping for a name interpolated into a metadata query.

    asyncpg takes `$1` parameters, not the `%s` this file's other queries use,
    and the surrounding query is a static string with one caller-influenced
    name in it. Doubling the quote is what PostgreSQL asks for, and the name
    has already been through `parse_materialization`.
    """
    return value.replace("'", "''")


def _schema_filter(reference: TableRef) -> str:
    """Constrain by schema only when the caller named one.

    A materialization destination is always schema-qualified — the parser
    refuses one that is not — so this is for sources and for direct callers.
    """
    if reference.schema is None:
        return ""
    return f"AND c.table_schema = '{_escaped(reference.schema)}'"


def _load_statement(load: RowLoad) -> str:
    return load.statement(_quoted, lambda index: f"${index}")


async def _load(connection: _Connection, load: RowLoad, policy: SQLPolicy) -> int:
    """Insert a load's rows as parameters, inside the caller's transaction.

    The bound is checked before anything is sent: one row per row, so the
    count is known in advance. It is checked again by `check` for the same
    reason every write is: the transaction is what makes it a ceiling.
    """
    affected = len(load.rows)
    check(policy.max_rows_affected, affected)
    statement = await connection.prepare(_load_statement(load))
    kinds = [parameter.name for parameter in statement.get_parameters()]
    await statement.executemany([_as_column(row, kinds) for row in load.rows])
    return affected


def _as_column(row: Sequence[object], kinds: Sequence[str]) -> tuple[object, ...]:
    """A row's values as PostgreSQL's columns take them, where that loses nothing.

    Rows often come from another engine. MySQL has no boolean, so a `0` or `1`
    is one; a timestamp with no zone, written to a `timestamptz`, is read as
    UTC. Anything else is passed as it is, and PostgreSQL says if it does not
    fit.
    """
    converted: list[object] = []
    for value, kind in zip(row, kinds, strict=True):
        if kind == "bool" and isinstance(value, int) and not isinstance(value, bool):
            if value in (0, 1):
                value = bool(value)
        elif kind == "timestamptz" and isinstance(value, datetime) and value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        converted.append(value)
    return tuple(converted)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def _write(
    connection: _Connection,
    sql: str,
    policy: SQLPolicy,
    dry_run: bool = False,
    *,
    preview: int = 0,
) -> tuple[int, InlineRows | None]:
    """Run an `INSERT`, `UPDATE`, `DELETE` or `MERGE`, counted and bounded.

    Called inside the caller's transaction, which is what makes the bound
    real: exceeding it raises, and leaving the transaction block on an
    exception rolls the statement back.

    Without `RETURNING` the count is the command tag's. With it, every changed
    row comes back, so the rows are the count — and they are read through a
    cursor one past the limit rather than fetched whole, because a statement
    that changed a million rows should be refused without first shipping a
    million rows here. PostgreSQL runs the whole statement on the first fetch
    either way; the cursor only bounds what crosses the wire.
    """
    statement = await connection.prepare(sql)
    attributes = statement.get_attributes()
    limit = policy.max_rows_affected
    if dry_run:
        # Counted and then undone whatever the count: the limit is not applied,
        # because the question being asked is how many, not whether.
        if not attributes:
            await statement.fetch()
            raise DryRunComplete(from_command_tag(statement.get_statusmsg()))
        counted = await (await statement.cursor()).fetch(DRY_RUN_CEILING + 1)
        raise DryRunComplete(
            min(len(counted), DRY_RUN_CEILING),
            at_least=len(counted) > DRY_RUN_CEILING,
            preview=preview_rows(
                tuple(attribute.name for attribute in attributes), counted, preview
            ),
        )
    if not attributes:
        await statement.fetch()
        affected = from_command_tag(statement.get_statusmsg())
        check(limit, affected)
        return affected, None

    columns = tuple(attribute.name for attribute in attributes)
    if limit is None:
        records = await statement.fetch()
    else:
        cursor = await statement.cursor()
        records = await cursor.fetch(limit + 1)
        if len(records) > limit:
            raise AffectedRowLimitError(limit)
    rows = tuple(tuple(value for value in row) for row in records[: policy.max_rows])
    return len(records), InlineRows(columns, rows, truncated=len(records) > policy.max_rows)


def _preview_size(value: object) -> int:
    """How many counted rows a dry run should keep: a small positive int, or none."""
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else 0


def _preview_output(handle: ExecutionHandle, counted: DryRunComplete) -> tuple[OutputRef, ...]:
    if counted.preview is None:
        return ()
    return (
        OutputRef(
            OutputKind.INLINE,
            f"inline://{handle.gantry_id}",
            metadata={"inline": counted.preview},
        ),
    )


def _destination_output(handle: ExecutionHandle) -> tuple[OutputRef, ...]:
    """A `postgres://schema/table` reference, when this run was a materialization.

    Slashes rather than dots: `_materialized_output` matches the destination
    against the URI with dots replaced by slashes, so a dotted name here would
    not be recognised as the table that was asked for.
    """
    destination = handle.metadata.get(_DESTINATION_METADATA)
    if not isinstance(destination, str):
        return ()
    operation = handle.metadata.get(_OPERATION_METADATA)
    object_kind = "view" if isinstance(operation, str) and "VIEW" in operation.upper() else "table"
    return (
        OutputRef(
            OutputKind.TABLE,
            f"postgres://{destination.replace('.', '/')}",
            metadata={"object_kind": object_kind},
        ),
    )


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


# ------------------------------------------------------------------ snapshot
#
# One query per kind of object, each taking the schemas as $1. Every
# definition comes from PostgreSQL's own pg_get_*def functions, so what is
# created elsewhere is what PostgreSQL itself would write.

#: Objects an extension owns: made again by `CREATE EXTENSION`, not by us.
_NOT_FROM_EXTENSION = (
    "NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = {oid} AND d.deptype = 'e')"
)

_SNAPSHOT_QUERIES: dict[str, str] = {
    "extensions": (
        "SELECT e.extname, n.nspname FROM pg_extension e "
        "JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname <> 'plpgsql' "
        "ORDER BY e.extname"
    ),
    "enums": (
        "SELECT n.nspname, t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder) "
        "FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid "
        "JOIN pg_namespace n ON n.oid = t.typnamespace "
        "WHERE n.nspname = ANY($1) AND "
        + _NOT_FROM_EXTENSION.format(oid="t.oid")
        + " GROUP BY n.nspname, t.typname ORDER BY 1, 2"
    ),
    "sequences": (
        "SELECT n.nspname, c.relname, s.seqtypid::regtype::text, s.seqstart, s.seqincrement, "
        "s.seqmin, s.seqmax, s.seqcycle, owner.relname, owner_col.attname, "
        "EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'i') "
        "FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN pg_depend dep ON dep.objid = c.oid AND dep.deptype IN ('a', 'i') "
        "AND dep.refobjsubid > 0 "
        "LEFT JOIN pg_class owner ON owner.oid = dep.refobjid "
        "LEFT JOIN pg_attribute owner_col ON owner_col.attrelid = dep.refobjid "
        "AND owner_col.attnum = dep.refobjsubid "
        "WHERE n.nspname = ANY($1) AND "
        + _NOT_FROM_EXTENSION.format(oid="c.oid")
        + " ORDER BY 1, 2"
    ),
    "tables": (
        "SELECT n.nspname, c.relname, c.relrowsecurity, c.relforcerowsecurity "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind IN ('r', 'p') AND n.nspname = ANY($1) AND "
        + _NOT_FROM_EXTENSION.format(oid="c.oid")
        + " ORDER BY 1, 2"
    ),
    "columns": (
        "SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), "
        "a.attnotnull, pg_get_expr(ad.adbin, ad.adrelid), a.attidentity, a.attgenerated "
        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum "
        "WHERE c.relkind IN ('r', 'p') AND n.nspname = ANY($1) AND a.attnum > 0 "
        "AND NOT a.attisdropped ORDER BY 1, 2, a.attnum"
    ),
    "constraints": (
        "SELECT n.nspname, c.relname, k.conname, k.contype, pg_get_constraintdef(k.oid, true), "
        "rn.nspname, rc.relname "
        "FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN pg_class rc ON rc.oid = k.confrelid "
        "LEFT JOIN pg_namespace rn ON rn.oid = rc.relnamespace "
        "WHERE n.nspname = ANY($1) AND k.contype IN ('p', 'u', 'c', 'f', 'x') "
        "ORDER BY 1, 2, k.contype, 3"
    ),
    "indexes": (
        "SELECT n.nspname, t.relname, i.relname, pg_get_indexdef(x.indexrelid) "
        "FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid "
        "JOIN pg_class t ON t.oid = x.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = ANY($1) AND t.relkind IN ('r', 'p', 'm') "
        "AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = x.indexrelid) "
        "ORDER BY 1, 2, 3"
    ),
    "views": (
        "SELECT n.nspname, c.relname, c.relkind, pg_get_viewdef(c.oid, true) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind IN ('v', 'm') AND n.nspname = ANY($1) AND "
        + _NOT_FROM_EXTENSION.format(oid="c.oid")
        + " ORDER BY c.oid"
    ),
    "functions": (
        "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), "
        "pg_get_functiondef(p.oid), l.lanname, p.prokind "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "JOIN pg_language l ON l.oid = p.prolang "
        "WHERE n.nspname = ANY($1) AND p.prokind IN ('f', 'p') AND "
        + _NOT_FROM_EXTENSION.format(oid="p.oid")
        + " ORDER BY p.oid"
    ),
    "triggers": (
        "SELECT n.nspname, c.relname, t.tgname, pg_get_triggerdef(t.oid, true) "
        "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE NOT t.tgisinternal AND n.nspname = ANY($1) ORDER BY 1, 2, 3"
    ),
    "policies": (
        "SELECT schemaname, tablename, policyname, permissive, roles::text[], cmd, qual, "
        "with_check FROM pg_policies WHERE schemaname = ANY($1) ORDER BY 1, 2, 3"
    ),
}


def _snapshot_objects(
    rows: Mapping[str, Sequence[Sequence[object]]], schemas: Sequence[str]
) -> tuple[SchemaObject, ...]:
    """The catalog's rows as objects, in the order a schema is built."""
    q = _quoted
    found: list[SchemaObject] = []
    for name, home in rows["extensions"]:
        found.append(
            SchemaObject(
                ObjectKind.EXTENSION,
                str(home),
                str(name),
                f"CREATE EXTENSION IF NOT EXISTS {q(str(name))}",
                details={"schema": str(home)},
            )
        )
    for namespace in schemas:
        found.append(
            SchemaObject(
                ObjectKind.SCHEMA,
                namespace,
                namespace,
                f"CREATE SCHEMA IF NOT EXISTS {q(namespace)}",
            )
        )
    for schema, name, labels in rows["enums"]:
        values = ", ".join(_literal(str(label)) for label in cast(Sequence[object], labels))
        found.append(
            SchemaObject(
                ObjectKind.ENUM,
                str(schema),
                str(name),
                f"CREATE TYPE {q(str(schema))}.{q(str(name))} AS ENUM ({values})",
                details={"labels": [str(label) for label in cast(Sequence[object], labels)]},
            )
        )
    for (
        schema,
        name,
        kind,
        start,
        increment,
        low,
        high,
        cycle,
        owner,
        owner_column,
        identity,
    ) in rows["sequences"]:
        if identity:
            # An identity column's sequence is the column's: made with it.
            continue
        found.append(
            SchemaObject(
                ObjectKind.SEQUENCE,
                str(schema),
                str(name),
                f"CREATE SEQUENCE {q(str(schema))}.{q(str(name))} AS {kind} "
                f"INCREMENT BY {increment} MINVALUE {low} MAXVALUE {high} START WITH {start}"
                + (" CYCLE" if cycle else ""),
                details={
                    "owned_by": None if owner is None else f"{schema}.{owner}.{owner_column}",
                },
            )
        )
    columns: dict[tuple[str, str], list[SnapshotColumn]] = {}
    for schema, table, column, kind, not_null, default, identity, generated in rows["columns"]:
        columns.setdefault((str(schema), str(table)), []).append(
            SnapshotColumn(
                name=str(column),
                data_type=str(kind),
                nullable=not not_null,
                default=None if default is None or _char(generated) == "s" else str(default),
                identity={"a": "always", "d": "by default"}.get(_char(identity)),
                generated=str(default) if _char(generated) == "s" and default is not None else None,
            )
        )
    for schema, name, security, forced in rows["tables"]:
        listed = tuple(columns.get((str(schema), str(name)), ()))
        found.append(
            SchemaObject(
                ObjectKind.TABLE,
                str(schema),
                str(name),
                f"CREATE TABLE {q(str(schema))}.{q(str(name))} ("
                + ", ".join(_column_definition(column) for column in listed)
                + ")",
                columns=listed,
                details={"row_security": bool(security), "force_row_security": bool(forced)},
            )
        )
    for schema, name, language, definition, kind in (
        (row[0], row[1], row[4], row[3], row[5]) for row in rows["functions"]
    ):
        found.append(
            SchemaObject(
                ObjectKind.FUNCTION,
                str(schema),
                str(name),
                str(definition),
                details={"language": str(language), "procedure": _char(kind) == "p"},
            )
        )
    for schema, table, name, kind, definition, ref_schema, ref_table in rows["constraints"]:
        found.append(
            SchemaObject(
                ObjectKind.CONSTRAINT,
                str(schema),
                str(name),
                f"ALTER TABLE {q(str(schema))}.{q(str(table))} ADD CONSTRAINT {q(str(name))} "
                f"{definition}",
                table=str(table),
                details={
                    "type": {
                        "p": "primary key",
                        "u": "unique",
                        "c": "check",
                        "f": "foreign key",
                    }.get(_char(kind), "exclusion"),
                    "references": None if ref_table is None else f"{ref_schema}.{ref_table}",
                },
            )
        )
    for schema, table, name, definition in rows["indexes"]:
        found.append(
            SchemaObject(
                ObjectKind.INDEX, str(schema), str(name), str(definition), table=str(table)
            )
        )
    for schema, name, kind, definition in rows["views"]:
        materialized = _char(kind) == "m"
        body = str(definition).strip().rstrip(";")
        found.append(
            SchemaObject(
                ObjectKind.MATERIALIZED_VIEW if materialized else ObjectKind.VIEW,
                str(schema),
                str(name),
                f"CREATE {'MATERIALIZED VIEW' if materialized else 'VIEW'} "
                f"{q(str(schema))}.{q(str(name))} AS {body}",
            )
        )
    for schema, table, name, definition in rows["triggers"]:
        found.append(
            SchemaObject(
                ObjectKind.TRIGGER, str(schema), str(name), str(definition), table=str(table)
            )
        )
    for schema, table, name, permissive, roles, command, using, with_check in rows["policies"]:
        clauses = [
            f"CREATE POLICY {q(str(name))} ON {q(str(schema))}.{q(str(table))}",
            f"AS {str(permissive).upper()}",
            f"FOR {str(command).upper()}",
            "TO " + ", ".join(str(role) for role in cast(Sequence[object], roles or ["public"])),
        ]
        if using is not None:
            clauses.append(f"USING ({using})")
        if with_check is not None:
            clauses.append(f"WITH CHECK ({with_check})")
        found.append(
            SchemaObject(
                ObjectKind.POLICY,
                str(schema),
                str(name),
                " ".join(clauses),
                table=str(table),
                details={"roles": [str(role) for role in cast(Sequence[object], roles or [])]},
            )
        )
    return tuple(found)


def _column_definition(column: SnapshotColumn) -> str:
    """One column as `CREATE TABLE` writes it: constraints are added on their own."""
    parts = [_quoted(column.name), column.data_type]
    if column.generated is not None:
        parts.append(f"GENERATED ALWAYS AS ({column.generated}) STORED")
    elif column.identity is not None:
        parts.append(f"GENERATED {column.identity.upper()} AS IDENTITY")
    elif column.default is not None:
        parts.append(f"DEFAULT {column.default}")
    if not column.nullable:
        parts.append("NOT NULL")
    return " ".join(parts)


def _char(value: object) -> str:
    """A catalog `"char"` column, which the driver hands back as bytes; NUL is empty."""
    if value is None:
        return ""
    text = value.decode() if isinstance(value, bytes) else str(value)
    return text.strip("\x00")


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
