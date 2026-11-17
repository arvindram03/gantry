# SPDX-License-Identifier: Apache-2.0
"""MySQL execution, with read-only transactions and an enforced timeout.

Three things here are MySQL-specific rather than PostgreSQL with different
spelling, and each was measured against MySQL 8.4 rather than assumed.

`max_execution_time` bounds `SELECT` and nothing else: a
`CREATE TABLE ... AS SELECT` ran to completion under a 200ms limit. So the
timeout is enforced twice — the server variable for reads, and a wait plus
`KILL QUERY` from a second connection for everything else. `KILL QUERY` aborts
a real write in well under a second, and MySQL 8's atomic DDL means the
destination is not left half-built.

`EXPLAIN` cannot describe DDL, so native validation uses `PREPARE` and
`DEALLOCATE`, which parses and resolves the statement without running it.
`PREPARE` on a `CREATE TABLE ... AS SELECT` creates nothing.

A MySQL "schema" is a database. `information_schema.tables.table_schema` holds
the database name and `table_catalog` is always `def`, so what Gantry calls a
schema maps to a database and the catalog carries no information.
"""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.output import OutputKind, OutputRef
from gantry.sql.affected import DRY_RUN, ROW_WRITES, AffectedRowLimitError, DryRunComplete, check
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import MySQLDialect
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
from gantry.sql.snapshot import Column as SnapshotColumn
from gantry.sql.snapshot import ObjectKind, SchemaObject, SchemaSnapshot
from gantry.sql.target import SQLTarget

# Databases that belong to the server rather than to anyone using it.
_SYSTEM_DATABASES = ("mysql", "information_schema", "performance_schema", "sys")

# Statements MySQL can EXPLAIN. Everything else is validated by PREPARE alone.
_EXPLAINABLE = frozenset(
    {
        SQLOperation.SELECT,
        SQLOperation.INSERT,
        SQLOperation.UPDATE,
        SQLOperation.DELETE,
    }
)

_DIALECT = MySQLDialect()


class MySQLAdapter:
    """MySQL execution with per-query read-only transactions and bounds."""

    def __init__(self, target: SQLTarget) -> None:
        try:
            self._driver = importlib.import_module("aiomysql")
        except ImportError as error:
            raise ImportError(
                'MySQL support requires `pip install "data-gantry[mysql]"`'
            ) from error
        self._target = target
        self._config = _connect_kwargs(target.config)
        self._jobs: dict[str, asyncio.Task[ExecutionResult]] = {}
        self._threads: dict[str, int] = {}
        self._loads: dict[str, RowLoad] = {}

    def capabilities(self) -> SQLCapabilities:
        return SQLCapabilities(
            describe_schema=True,
            explain=True,
            cancellation=True,
            read_only_session=True,
            statement_timeout=True,
            row_limit=True,
            affected_row_limit=True,
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
        placeholders = ", ".join(["%s"] * len(_SYSTEM_DATABASES))
        rows = await self._fetch_all(
            f"""
            SELECT c.table_schema, c.table_name, t.table_type,
                   c.column_name, c.data_type, c.is_nullable
            FROM information_schema.columns AS c
            JOIN information_schema.tables AS t
              ON t.table_schema = c.table_schema
             AND t.table_name = c.table_name
            WHERE c.table_schema NOT IN ({placeholders})
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
            """,
            _SYSTEM_DATABASES,
        )
        grouped: dict[tuple[str, str, str], list[Column]] = {}
        for schema, table, kind, name, data_type, nullable in rows:
            key = (str(schema), str(table), str(kind).lower())
            grouped.setdefault(key, []).append(
                Column(str(name), str(data_type), str(nullable).upper() == "YES")
            )
        tables = tuple(
            # No catalog: MySQL reports `def` for every row, which names nothing.
            Table(name, schema, None, tuple(columns), kind=kind)
            for (schema, name, kind), columns in grouped.items()
        )
        return DatabaseSchema(
            catalogs=(),
            schemas=tuple(dict.fromkeys(table.schema for table in tables if table.schema)),
            tables=tables,
        )

    async def column_null_rates(
        self,
        reference: TableRef,
        target: SQLTarget,
        columns: Sequence[str],
    ) -> Mapping[str, float]:
        """Measure how often each column is null, in one pass over the table."""
        if not columns:
            return {}
        schema = reference.schema or self._config.get("db")
        if not isinstance(schema, str):
            return {}
        projections = ", ".join(
            f"AVG(CASE WHEN {_quoted(column)} IS NULL THEN 1.0 ELSE 0.0 END)" for column in columns
        )
        rows = await self._fetch_all(
            f"SELECT {projections} FROM {_quoted(schema)}.{_quoted(reference.name)}"
        )
        if not rows:
            return {}
        # AVG over an empty table is NULL; omitted rather than reported as zero,
        # which would be a measurement nobody took.
        return {
            column: float(value)
            for column, value in zip(columns, rows[0], strict=False)
            if value is not None
        }

    async def validate(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
        policy: SQLPolicy,
    ) -> ValidationResult:
        """Ask MySQL to parse and resolve the statement without running it.

        `PREPARE` rather than `EXPLAIN` because `EXPLAIN` rejects DDL outright,
        and a materialization is DDL. The statement travels in a user variable
        so the driver does the quoting; building a `PREPARE ... FROM '...'`
        string here would mean escaping caller SQL by hand.
        """
        load = load_of(context.metadata)
        if load is not None:
            # The statement as it will run, with MySQL's own `?` placeholders,
            # so the names in it are resolved against the real table.
            sql = load.statement(_quoted, lambda _index: "?")
        connection = await self._open()
        try:
            async with connection.cursor() as cursor:
                await cursor.execute("SET @gantry_validate = %s", (sql,))
                await cursor.execute("PREPARE gantry_probe FROM @gantry_validate")
                await cursor.execute("DEALLOCATE PREPARE gantry_probe")
        except Exception as error:
            return ValidationResult.rejected(str(error))
        finally:
            await self._close(connection)
        return ValidationResult.accepted()

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult:
        if _DIALECT.classify(sql).operation not in _EXPLAINABLE:
            # MySQL cannot EXPLAIN DDL. Saying so is better than an estimate of
            # zero, which a caller would read as "scans nothing".
            return ExplainResult(supported=False)
        connection = await self._open()
        try:
            rows = await self._fetch(connection, f"EXPLAIN {sql}")
        except Exception:
            return ExplainResult(supported=False)
        finally:
            await self._close(connection)
        return ExplainResult(supported=True, native={"rows": tuple(tuple(row) for row in rows)})

    async def submit(self, sql: str, target: SQLTarget, context: Context) -> ExecutionHandle:
        policy = context.metadata.get("gantry.sql.policy")
        if not isinstance(policy, SQLPolicy):
            raise ValueError("governed SQL policy is missing from execution context")
        gantry_id = f"run_{uuid4().hex}"
        metadata: dict[str, object] = {"provider": target.provider}
        if context.metadata.get(DRY_RUN) is True:
            metadata[DRY_RUN] = True
        plan = context.metadata.get(_PLAN_METADATA)
        if isinstance(plan, MaterializationPlan):
            metadata[_DESTINATION_METADATA] = plan.destination.qualified_name
            metadata[_OPERATION_METADATA] = plan.operation.value
        handle = ExecutionHandle(
            gantry_id,
            "sql",
            target.provider,
            f"mysql_{uuid4().hex}",
            metadata=metadata,
        )
        load = load_of(context.metadata)
        if load is not None:
            self._loads[gantry_id] = load
        self._jobs[gantry_id] = asyncio.create_task(self._execute(handle, sql, policy))
        return handle

    async def status(self, handle: ExecutionHandle) -> Execution:
        task = self._jobs.get(handle.gantry_id)
        if task is None:
            return _unknown(handle, "MySQL job is not available")
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
                handle, Failure(FailureKind.UNKNOWN, False, "MySQL job is not available")
            )
        return await task

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        """Stop the statement on the server, not just stop waiting for it.

        Closing the connection would abandon the query while MySQL kept running
        it, so this issues `KILL QUERY` from a second connection first.
        """
        await self._kill(handle.gantry_id)
        task = self._jobs.get(handle.gantry_id)
        if task is not None and not task.done():
            task.cancel()
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, f"MySQL statement cancelled ({mode})"),
        )

    async def snapshot(self, schemas: Sequence[str]) -> SchemaSnapshot:
        """Every table, constraint, index, view, trigger and routine in `schemas` (databases).

        Tables carry MySQL's own `SHOW CREATE TABLE`; the rest are read from
        `information_schema` and written in MySQL's dialect. MySQL's schema
        changes are not transactional, so this is read as it stands, object by
        object.
        """
        wanted = list(schemas)
        marks = ", ".join(["%s"] * len(wanted))
        connection = await self._open()
        try:
            rows = {
                name: await self._fetch(connection, query.format(marks=marks), wanted)
                for name, query in _SNAPSHOT_QUERIES.items()
            }
            created: dict[tuple[str, str], str] = {}
            for schema, table in rows["tables"]:
                found = await self._fetch(
                    connection,
                    f"SHOW CREATE TABLE {_quoted(_text(schema))}.{_quoted(_text(table))}",
                )
                created[(_text(schema), _text(table))] = _text(found[0][1])
        finally:
            await self._close(connection)
        return SchemaSnapshot("mysql", tuple(wanted), _snapshot_objects(rows, created, wanted))

    async def inspect_table(
        self,
        reference: TableRef,
        target: SQLTarget,
        *,
        include_row_count: bool = False,
    ) -> Table | None:
        """Look a destination up by name, for create-only and verification."""
        schema = reference.schema or self._config.get("db")
        if not isinstance(schema, str):
            return None
        rows = await self._fetch_all(
            """
            SELECT c.column_name, c.data_type, c.is_nullable, t.table_type
            FROM information_schema.columns AS c
            JOIN information_schema.tables AS t
              ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
            """,
            (schema, reference.name),
        )
        if not rows:
            return None
        columns = tuple(
            Column(str(name), str(data_type), str(nullable).upper() == "YES")
            for name, data_type, nullable, _ in rows
        )
        kind = str(rows[0][3]).lower()
        metadata: dict[str, object] = {}
        if include_row_count:
            counted = await self._fetch_all(
                f"SELECT COUNT(*) FROM {_quoted(schema)}.{_quoted(reference.name)}"
            )
            # The key is "rows": `gantry.verify.RowCount` reads that and nothing
            # else, so "row_count" here silently produced "row count unavailable".
            metadata["rows"] = int(cast(int, counted[0][0]))
        return Table(reference.name, schema, None, columns, kind=kind, metadata=metadata)

    async def _execute(
        self, handle: ExecutionHandle, sql: str, policy: SQLPolicy
    ) -> ExecutionResult:
        started = asyncio.get_running_loop().time()
        connection = None
        try:
            connection = await self._open()
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT CONNECTION_ID()")
                row = await cursor.fetchone()
                self._threads[handle.gantry_id] = int(row[0])
            outputs, rows_read, rows_written = await asyncio.wait_for(
                self._run(connection, handle, sql, policy),
                timeout=policy.timeout_seconds,
            )
        except TimeoutError:
            # Stop the server doing the work, not merely stop waiting for it.
            await self._kill(handle.gantry_id)
            return ExecutionResult.failed(
                handle,
                Failure(
                    FailureKind.TIMEOUT,
                    True,
                    f"MySQL statement exceeded {policy.timeout_seconds} seconds",
                ),
            )
        except asyncio.CancelledError:
            return ExecutionResult.failed(
                handle, Failure(FailureKind.CANCELLED, False, "MySQL statement was cancelled")
            )
        except AffectedRowLimitError as error:
            # `_run` rolled the transaction back before letting this out.
            return ExecutionResult.failed(handle, error.failure())
        except DryRunComplete as counted:
            return ExecutionResult.succeeded(
                handle,
                metrics=ExecutionMetrics(
                    rows_written=counted.affected,
                    runtime_seconds=asyncio.get_running_loop().time() - started,
                    native={"dry_run": True, "at_least": counted.at_least},
                ),
            )
        except Exception as error:
            return ExecutionResult.failed(handle, _failure(error))
        finally:
            self._threads.pop(handle.gantry_id, None)
            self._loads.pop(handle.gantry_id, None)
            if connection is not None:
                await self._close(connection)
        runtime = asyncio.get_running_loop().time() - started
        return ExecutionResult.succeeded(
            handle,
            outputs=outputs,
            metrics=ExecutionMetrics(
                rows_read=rows_read, rows_written=rows_written, runtime_seconds=runtime
            ),
        )

    async def _run(
        self, connection: Any, handle: ExecutionHandle, sql: str, policy: SQLPolicy
    ) -> tuple[tuple[OutputRef, ...], int | None, int | None]:
        operation = _DIALECT.classify(sql).operation
        async with connection.cursor() as cursor:
            # Server-side for reads. It does not cover writes, which is why the
            # caller wraps this in a deadline and kills the query on expiry.
            await cursor.execute(
                "SET SESSION max_execution_time = %s", (max(1, int(policy.timeout_seconds * 1000)),)
            )
            await cursor.execute(
                "START TRANSACTION READ ONLY" if policy.read_only else "START TRANSACTION"
            )
            try:
                load = self._loads.get(handle.gantry_id)
                if load is not None:
                    # Bound before sending: one row per row, so the count is
                    # known. The values go as parameters; the driver quotes them.
                    check(policy.max_rows_affected, len(load.rows))
                    await cursor.executemany(
                        load.statement(_quoted, lambda _index: "%s"), load.rows
                    )
                    affected = max(0, int(cursor.rowcount))
                    check(policy.max_rows_affected, affected)
                    await connection.commit()
                    return (), None, affected
                await cursor.execute(sql)
                if operation in ROW_WRITES:
                    # Counted before the commit, so a statement over its limit
                    # is rolled back by the handler below and changes nothing.
                    # This holds for transactional tables, which InnoDB's are;
                    # a MyISAM table has no transaction to roll back.
                    #
                    # MySQL counts rows *changed*: an `UPDATE` that sets a row
                    # to the value it already has does not count it.
                    affected = max(0, int(cursor.rowcount))
                    if handle.metadata.get(DRY_RUN) is True:
                        raise DryRunComplete(affected)
                    check(policy.max_rows_affected, affected)
                    await connection.commit()
                    return (), None, affected
                if operation is not SQLOperation.SELECT:
                    await connection.commit()
                    # A materialization has to come back as a reference to what
                    # it built, or the caller cannot verify the destination.
                    return _destination_output(handle), None, None
                columns = tuple(str(column[0]) for column in (cursor.description or ()))
                records = await cursor.fetchmany(policy.max_rows + 1)
                rows = tuple(tuple(value for value in row) for row in records[: policy.max_rows])
                inline = InlineRows(columns, rows, truncated=len(records) > policy.max_rows)
                await connection.commit()
            except BaseException:
                with suppress(Exception):
                    await connection.rollback()
                raise
        output = OutputRef(
            OutputKind.INLINE, f"inline://{handle.gantry_id}", metadata={"inline": inline}
        )
        return (output,), len(rows), None

    async def _kill(self, gantry_id: str) -> None:
        """`KILL QUERY` the statement from a connection of its own."""
        thread = self._threads.get(gantry_id)
        if thread is None:
            return
        with suppress(Exception):
            killer = await self._open()
            try:
                async with killer.cursor() as cursor:
                    await cursor.execute(f"KILL QUERY {int(thread)}")
            finally:
                await self._close(killer)

    async def _open(self) -> Any:
        return await self._driver.connect(**self._config)

    async def _close(self, connection: Any) -> None:
        with suppress(Exception):
            connection.close()

    async def _fetch(self, connection: Any, sql: str, args: Sequence[object] = ()) -> Sequence[Any]:
        async with connection.cursor() as cursor:
            await cursor.execute(sql, tuple(args) or None)
            rows: Sequence[Any] = await cursor.fetchall()
            return rows

    async def _fetch_all(self, sql: str, args: Sequence[object] = ()) -> Sequence[Any]:
        connection = await self._open()
        try:
            return await self._fetch(connection, sql, args)
        finally:
            await self._close(connection)


def _destination_output(handle: ExecutionHandle) -> tuple[OutputRef, ...]:
    """A `mysql://database/table` reference, when this run was a materialization.

    The slash form is not decoration: `_materialized_output` matches the
    destination against the URI with dots replaced by slashes, so a dotted name
    here would not be recognised as the table that was asked for.
    """
    destination = handle.metadata.get(_DESTINATION_METADATA)
    if not isinstance(destination, str):
        return ()
    kind = handle.metadata.get(_OPERATION_METADATA)
    object_kind = "view" if isinstance(kind, str) and "VIEW" in kind.upper() else "table"
    return (
        OutputRef(
            OutputKind.TABLE,
            f"mysql://{destination.replace('.', '/')}",
            metadata={"object_kind": object_kind},
        ),
    )


def _quoted(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _connect_kwargs(config: object) -> dict[str, Any]:
    """Turn provider config into aiomysql keywords.

    aiomysql takes host/port/user/password/db rather than a URL, so a `url=`
    is split here. Everything else is passed through, which keeps driver
    options — `ssl`, `connect_timeout`, `charset` — available without this
    adapter having to know each one.
    """
    values = dict(cast(dict[str, Any], config))
    url = values.pop("url", None)
    values.pop("read_only", None)
    if isinstance(url, str) and url.strip():
        parts = urlsplit(url)
        database = parts.path.lstrip("/")
        parsed: dict[str, Any] = {"host": parts.hostname or "localhost"}
        if parts.port:
            parsed["port"] = parts.port
        if parts.username:
            parsed["user"] = unquote(parts.username)
        if parts.password:
            parsed["password"] = unquote(parts.password)
        if database:
            parsed["db"] = unquote(database)
        # Explicit fields win over anything the URL carried.
        parsed.update(values)
        values = parsed
    values.setdefault("port", 3306)
    values.setdefault("autocommit", True)
    return values


def _failure(error: Exception) -> Failure:
    """Map a MySQL error onto Gantry's taxonomy, by error number where possible."""
    code = getattr(error, "args", (None,))[0]
    text = str(error)
    if code in {1044, 1045, 1142, 1143}:
        kind = FailureKind.AUTH_ERROR
    elif code in {1146, 1049, 1051, 1054}:
        kind = FailureKind.OBJECT_NOT_FOUND
    elif code == 1064:
        kind = FailureKind.SYNTAX_ERROR
    elif code in {3024, 1028}:
        kind = FailureKind.TIMEOUT
    elif code == 1317:
        kind = FailureKind.CANCELLED
    elif code == 1050:
        kind = FailureKind.DESTINATION_EXISTS
    elif code == 1792:
        # "Cannot execute statement in a READ ONLY transaction."
        kind = FailureKind.POLICY_REJECTED
    else:
        kind = FailureKind.ENGINE_ERROR
    return Failure(
        kind,
        kind is FailureKind.TIMEOUT,
        text,
        native_code=None if code is None else str(code),
        native_message=text,
    )


def _unknown(handle: ExecutionHandle, message: str) -> Execution:
    return Execution(
        handle, ExecutionState.UNKNOWN, failure=Failure(FailureKind.UNKNOWN, False, message)
    )


# ------------------------------------------------------------------ snapshot

_SNAPSHOT_QUERIES: dict[str, str] = {
    "tables": (
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ({marks}) ORDER BY 1, 2"
    ),
    "columns": (
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, "
        "COLUMN_DEFAULT, EXTRA, GENERATION_EXPRESSION FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA IN ({marks}) ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
    ),
    "constraints": (
        "SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, "
        "GROUP_CONCAT(k.COLUMN_NAME ORDER BY k.ORDINAL_POSITION), "
        "MAX(k.REFERENCED_TABLE_SCHEMA), MAX(k.REFERENCED_TABLE_NAME), "
        "GROUP_CONCAT(k.REFERENCED_COLUMN_NAME ORDER BY k.ORDINAL_POSITION) "
        "FROM information_schema.TABLE_CONSTRAINTS c "
        "LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON k.CONSTRAINT_SCHEMA = "
        "c.CONSTRAINT_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME AND k.CONSTRAINT_NAME = "
        "c.CONSTRAINT_NAME WHERE c.TABLE_SCHEMA IN ({marks}) "
        "GROUP BY c.TABLE_SCHEMA, c.TABLE_NAME, c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE "
        "ORDER BY 1, 2, 3"
    ),
    "indexes": (
        "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, MAX(NON_UNIQUE), "
        "GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA IN ({marks}) GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME "
        "ORDER BY 1, 2, 3"
    ),
    "views": (
        "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS "
        "WHERE TABLE_SCHEMA IN ({marks}) ORDER BY 1, 2"
    ),
    "triggers": (
        "SELECT TRIGGER_SCHEMA, EVENT_OBJECT_TABLE, TRIGGER_NAME, ACTION_TIMING, "
        "EVENT_MANIPULATION, ACTION_STATEMENT FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA IN ({marks}) ORDER BY 1, 2, 3"
    ),
    "routines": (
        "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES "
        "WHERE ROUTINE_SCHEMA IN ({marks}) ORDER BY 1, 2"
    ),
}

_CONSTRAINT_TYPES = {
    "PRIMARY KEY": "primary key",
    "UNIQUE": "unique",
    "FOREIGN KEY": "foreign key",
    "CHECK": "check",
}


def _snapshot_objects(
    rows: Mapping[str, Sequence[Sequence[object]]],
    created: Mapping[tuple[str, str], str],
    schemas: Sequence[str],
) -> tuple[SchemaObject, ...]:
    q = _quoted
    found: list[SchemaObject] = [
        SchemaObject(ObjectKind.SCHEMA, name, name, f"CREATE DATABASE IF NOT EXISTS {q(name)}")
        for name in schemas
    ]
    columns: dict[tuple[str, str], list[SnapshotColumn]] = {}
    for schema, table, name, kind, nullable, default, extra, expression in rows["columns"]:
        extra_text = _text(extra).lower()
        generated = "generated" in extra_text and "default_generated" not in extra_text
        columns.setdefault((_text(schema), _text(table)), []).append(
            SnapshotColumn(
                name=_text(name),
                data_type=_text(kind),
                nullable=_text(nullable) == "YES",
                default=None if default is None or generated else _text(default),
                identity="by default" if "auto_increment" in extra_text else None,
                generated=_text(expression) if generated and expression else None,
            )
        )
    for schema, table in rows["tables"]:
        key = (_text(schema), _text(table))
        found.append(
            SchemaObject(
                ObjectKind.TABLE,
                key[0],
                key[1],
                created.get(key),
                columns=tuple(columns.get(key, ())),
            )
        )
    constraint_names: set[tuple[str, str, str]] = set()
    for schema, table, name, kind, names, ref_schema, ref_table, ref_names in rows["constraints"]:
        kind_text = _CONSTRAINT_TYPES.get(_text(kind), "check")
        listed = ", ".join(q(part) for part in _text(names).split(",") if part)
        if kind_text == "primary key":
            body = f"PRIMARY KEY ({listed})"
        elif kind_text == "unique":
            body = f"UNIQUE ({listed})"
        elif kind_text == "foreign key":
            referenced = ", ".join(q(part) for part in _text(ref_names).split(",") if part)
            body = (
                f"FOREIGN KEY ({listed}) REFERENCES {q(_text(ref_schema))}."
                f"{q(_text(ref_table))} ({referenced})"
            )
        else:
            body = "CHECK"
        constraint_names.add((_text(schema), _text(table), _text(name)))
        found.append(
            SchemaObject(
                ObjectKind.CONSTRAINT,
                _text(schema),
                _text(name),
                f"ALTER TABLE {q(_text(schema))}.{q(_text(table))} ADD CONSTRAINT "
                f"{q(_text(name))} {body}",
                table=_text(table),
                details={
                    "type": kind_text,
                    "columns": [part for part in _text(names).split(",") if part],
                    "references": None
                    if ref_table is None
                    else f"{_text(ref_schema)}.{_text(ref_table)}",
                },
            )
        )
    for schema, table, name, non_unique, names in rows["indexes"]:
        index = (_text(schema), _text(table), _text(name))
        if index[2] == "PRIMARY" or index in constraint_names:
            continue
        listed = ", ".join(q(part) for part in _text(names).split(",") if part)
        unique = "" if int(cast(int, non_unique)) else "UNIQUE "
        found.append(
            SchemaObject(
                ObjectKind.INDEX,
                index[0],
                index[2],
                f"CREATE {unique}INDEX {q(index[2])} ON {q(index[0])}.{q(index[1])} ({listed})",
                table=index[1],
                details={"columns": [part for part in _text(names).split(",") if part]},
            )
        )
    for schema, name, definition in rows["views"]:
        found.append(
            SchemaObject(
                ObjectKind.VIEW,
                _text(schema),
                _text(name),
                f"CREATE VIEW {q(_text(schema))}.{q(_text(name))} AS {_text(definition)}",
            )
        )
    for schema, routine, kind in rows["routines"]:
        found.append(
            SchemaObject(
                ObjectKind.FUNCTION,
                _text(schema),
                _text(routine),
                details={"procedure": _text(kind) == "PROCEDURE"},
            )
        )
    for schema, table, name, timing, event, statement in rows["triggers"]:
        found.append(
            SchemaObject(
                ObjectKind.TRIGGER,
                _text(schema),
                _text(name),
                f"CREATE TRIGGER {q(_text(name))} {_text(timing)} {_text(event)} ON "
                f"{q(_text(schema))}.{q(_text(table))} FOR EACH ROW {_text(statement)}",
                table=_text(table),
            )
        )
    return tuple(found)


def _text(value: object) -> str:
    """A catalog value as text: the driver hands some back as bytes."""
    if value is None:
        return ""
    return value.decode() if isinstance(value, bytes) else str(value)
