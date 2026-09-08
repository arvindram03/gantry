# SPDX-License-Identifier: Apache-2.0
"""Google BigQuery job adapter with dry-run admission and referenced results."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable, Iterable, Sequence
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


class _Field(Protocol):
    name: str
    field_type: str
    is_nullable: bool


class _TableRef(Protocol):
    project: str
    dataset_id: str
    table_id: str


class _Table(Protocol):
    project: str
    dataset_id: str
    table_id: str
    table_type: str
    schema: Sequence[_Field]
    num_rows: int | None
    num_bytes: int | None


class _Dataset(Protocol):
    dataset_id: str


class _RowIterator(Iterable[Sequence[object]], Protocol):
    schema: Sequence[_Field]
    total_rows: int | None


class _Job(Protocol):
    job_id: str
    state: str | None
    error_result: object | None
    errors: object | None
    destination: _TableRef | None
    total_bytes_processed: int | None
    total_bytes_billed: int | None
    slot_millis: int | None
    statement_type: str | None

    def result(self, *, max_results: int | None = None) -> _RowIterator: ...

    def reload(self) -> None: ...

    def cancel(self) -> bool: ...


class _Client(Protocol):
    def query(self, sql: str, **kwargs: object) -> _Job: ...

    def get_job(self, job_id: str, **kwargs: object) -> _Job: ...

    def list_datasets(self) -> Iterable[_Dataset]: ...

    def list_tables(self, dataset: str) -> Iterable[_TableRef]: ...

    def get_table(self, table: _TableRef) -> _Table: ...


class BigQueryAdapter:
    """Reconnectable BigQuery jobs; large output always retains a table reference."""

    def __init__(self, target: SQLTarget) -> None:
        module: ModuleType
        try:
            module = importlib.import_module("google.cloud.bigquery")
        except ImportError as error:
            raise ImportError(
                'BigQuery support requires `pip install "gantry[bigquery]"`'
            ) from error
        self._job_config = cast(Callable[..., object], module.QueryJobConfig)
        client_type = cast(Callable[..., _Client], module.Client)
        project = target.config.get("project")
        credentials = target.config.get("credentials")
        client_options = target.config.get("client_options")
        kwargs: dict[str, object] = {"project": project}
        if credentials is not None:
            kwargs["credentials"] = credentials
        if client_options is not None:
            kwargs["client_options"] = client_options
        location = target.config.get("location")
        if isinstance(location, str):
            kwargs["location"] = location
        self._client = client_type(**kwargs)
        self._target = target
        price = target.config.get("price_per_tb_usd")
        if price is not None and not isinstance(price, (int, float)):
            raise ValueError("BigQuery price_per_tb_usd must be numeric")
        self._price_per_tb = None if price is None else float(price)

    def capabilities(self) -> SQLCapabilities:
        return SQLCapabilities(
            describe_schema=True,
            explain=True,
            dry_run=True,
            async_jobs=True,
            reconnect=True,
            cancellation=True,
            read_only_session=True,
            statement_timeout=False,
            row_limit=True,
            cost_estimate=self._price_per_tb is not None,
            cost_limit=self._price_per_tb is not None,
            bytes_scanned=True,
            query_metrics=True,
            result_reference=True,
        )

    async def describe(self, target: SQLTarget) -> DatabaseSchema:
        tables = await asyncio.to_thread(self._describe_sync)
        return DatabaseSchema(
            catalogs=(str(target.config["project"]),),
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
            job = await asyncio.to_thread(self._dry_run, sql)
        except Exception as error:
            return ValidationResult.rejected(str(error))
        statement_type = (job.statement_type or "").upper()
        if policy.read_only and statement_type != "SELECT":
            return ValidationResult.rejected(
                f"BigQuery classified statement as {statement_type}, not read-only"
            )
        return ValidationResult.accepted(
            metadata={"estimated_bytes": job.total_bytes_processed or 0}
        )

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult:
        job = await asyncio.to_thread(self._dry_run, sql)
        estimated_bytes = job.total_bytes_processed or 0
        estimated_cost = (
            None
            if self._price_per_tb is None
            else estimated_bytes / 1_000_000_000_000 * self._price_per_tb
        )
        return ExplainResult(
            supported=True,
            estimated_bytes=estimated_bytes,
            estimated_cost=estimated_cost,
            summary={"statement_type": job.statement_type or "UNKNOWN"},
            native={"job_id": job.job_id},
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
        job_config = self._job_config(
            maximum_bytes_billed=policy.max_bytes_scanned,
            job_timeout_ms=max(1, int(policy.timeout_seconds * 1_000)),
        )
        location = target.config.get("location")
        kwargs = {"job_config": job_config}
        if isinstance(location, str):
            kwargs["location"] = location
        job = await asyncio.to_thread(self._client.query, sql, **kwargs)
        return ExecutionHandle(
            f"run_{uuid4().hex}",
            "sql",
            target.provider,
            job.job_id,
            metadata={
                "project": target.config["project"],
                "location": location or "",
                "max_rows": policy.max_rows,
                "timeout_seconds": policy.timeout_seconds,
            },
        )

    async def status(self, handle: ExecutionHandle) -> Execution:
        try:
            job = await self._get_job(handle)
            await asyncio.to_thread(job.reload)
        except Exception as error:
            return _unknown(handle, str(error))
        if job.state != "DONE":
            return Execution(
                handle,
                ExecutionState.RUNNING,
                started_at=handle.submitted_at,
                native={"state": job.state or "UNKNOWN"},
            )
        if job.error_result:
            failure = _failure(job.error_result)
            return Execution(
                handle,
                ExecutionState.FAILED,
                failure=failure,
                native={"errors": job.errors},
            )
        return Execution(
            handle,
            ExecutionState.SUCCEEDED,
            started_at=handle.submitted_at,
            updated_at=datetime.now(UTC),
            metrics=self._metrics(job),
            native={"state": job.state or "UNKNOWN"},
        )

    async def result(self, handle: ExecutionHandle) -> ExecutionResult:
        try:
            job = await self._get_job(handle)
            maximum = handle.metadata.get("max_rows", 1_000)
            max_rows = maximum if isinstance(maximum, int) else 1_000
            iterator = await asyncio.to_thread(job.result, max_results=max_rows + 1)
            records = list(iterator)
        except Exception as error:
            return ExecutionResult.failed(handle, _failure(error))
        columns = tuple(field.name for field in iterator.schema)
        rows = tuple(tuple(value for value in row) for row in records[:max_rows])
        inline = InlineRows(columns, rows, truncated=len(records) > max_rows)
        outputs = [
            OutputRef(
                OutputKind.INLINE,
                f"inline://{handle.gantry_id}",
                metadata={"inline": inline},
            )
        ]
        if job.destination is not None:
            destination = job.destination
            outputs.append(
                OutputRef(
                    OutputKind.TABLE,
                    f"bigquery://{destination.project}/{destination.dataset_id}/{destination.table_id}",
                )
            )
        return ExecutionResult.succeeded(
            handle,
            outputs=tuple(outputs),
            metrics=self._metrics(job),
        )

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        try:
            job = await self._get_job(handle)
            cancelled = await asyncio.to_thread(job.cancel)
        except Exception as error:
            return _unknown(handle, str(error))
        if not cancelled:
            return _unknown(handle, "BigQuery did not confirm cancellation")
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, f"BigQuery job cancelled ({mode})"),
        )

    async def _get_job(self, handle: ExecutionHandle) -> _Job:
        kwargs: dict[str, object] = {}
        location = handle.metadata.get("location")
        if isinstance(location, str) and location:
            kwargs["location"] = location
        return await asyncio.to_thread(self._client.get_job, handle.native_id, **kwargs)

    def _dry_run(self, sql: str) -> _Job:
        config = self._job_config(dry_run=True, use_query_cache=False)
        location = self._target.config.get("location")
        kwargs: dict[str, object] = {"job_config": config}
        if isinstance(location, str):
            kwargs["location"] = location
        return self._client.query(sql, **kwargs)

    def _describe_sync(self) -> tuple[Table, ...]:
        project = str(self._target.config["project"])
        selected = self._target.config.get("dataset")
        datasets = (
            (str(selected),)
            if isinstance(selected, str)
            else tuple(dataset.dataset_id for dataset in self._client.list_datasets())
        )
        tables: list[Table] = []
        for dataset in datasets:
            for reference in self._client.list_tables(f"{project}.{dataset}"):
                native = self._client.get_table(reference)
                columns = tuple(
                    Column(field.name, field.field_type, field.is_nullable)
                    for field in native.schema
                )
                tables.append(
                    Table(
                        native.table_id,
                        native.dataset_id,
                        native.project,
                        columns,
                        kind=native.table_type.lower(),
                        metadata={"rows": native.num_rows, "bytes": native.num_bytes},
                    )
                )
        return tuple(tables)

    def _metrics(self, job: _Job) -> ExecutionMetrics:
        estimated_cost = (
            None
            if self._price_per_tb is None or job.total_bytes_processed is None
            else job.total_bytes_processed / 1_000_000_000_000 * self._price_per_tb
        )
        return ExecutionMetrics(
            bytes_read=job.total_bytes_processed,
            estimated_cost_usd=estimated_cost,
            native={
                "bytes_billed": job.total_bytes_billed,
                "slot_millis": job.slot_millis,
            },
        )


def _failure(value: object) -> Failure:
    message = str(value)
    lowered = message.lower()
    if "access denied" in lowered or "permission" in lowered or "forbidden" in lowered:
        kind = FailureKind.AUTH_ERROR
    elif "not found" in lowered:
        kind = FailureKind.OBJECT_NOT_FOUND
    elif "syntax" in lowered:
        kind = FailureKind.SYNTAX_ERROR
    elif "quota" in lowered or "resource" in lowered:
        kind = FailureKind.RESOURCE_EXHAUSTED
    elif "timeout" in lowered or "deadline" in lowered:
        kind = FailureKind.TIMEOUT
    else:
        kind = FailureKind.ENGINE_ERROR
    return Failure(kind, kind in {FailureKind.TIMEOUT, FailureKind.RESOURCE_EXHAUSTED}, message)


def _unknown(handle: ExecutionHandle, message: str) -> Execution:
    return Execution(
        handle,
        ExecutionState.UNKNOWN,
        failure=Failure(FailureKind.UNKNOWN, False, message),
    )
