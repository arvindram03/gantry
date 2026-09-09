# SPDX-License-Identifier: Apache-2.0
"""Small public surface for governed SQL connections."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gantry.artifact import Artifact
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.result import Result, ResultStatus
from gantry.runtime import ControlPlane
from gantry.sql.adapter import SQLAdapter
from gantry.sql.bridge import SQLExecutionAdapter
from gantry.sql.dialect import SQLDialect
from gantry.sql.explain import ExplainResult
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.sql.registry import resolve_dialect, resolve_provider
from gantry.sql.result import SQLResult
from gantry.sql.schema import DatabaseSchema, Table
from gantry.sql.target import SQLTarget
from gantry.store import MemoryExecutionStore
from gantry.target import ExecutionTarget
from gantry.verifier import Verifier

if TYPE_CHECKING:
    from gantry.sql.capabilities import SQLCapabilities
    from gantry.sql.materialization import SQLMaterializer, TableRef
    from gantry.sql.query import SQLQuery
    from gantry.verify import MaterializationCheck


class SQLConnection:
    """A provider-neutral, governed SQL connection.

    Configuration and the native adapter remain private so an agent tool cannot
    accidentally expose credentials or a raw database connection.
    """

    def __init__(
        self,
        target: SQLTarget,
        adapter: SQLAdapter,
        dialect: SQLDialect,
    ) -> None:
        self._target = target
        self._adapter = adapter
        self._dialect = dialect
        self._plane = ControlPlane(store=MemoryExecutionStore())

    @property
    def provider(self) -> str:
        return self._target.provider

    @property
    def dialect(self) -> str:
        return self._target.dialect

    def capabilities(self) -> SQLCapabilities:
        """Return the adapter's declared SQL capabilities without credentials."""

        return self._adapter.capabilities()

    async def describe(self) -> DatabaseSchema:
        """Return normalized catalogs, schemas, tables, and columns."""

        if not self._adapter.capabilities().describe_schema:
            raise NotImplementedError(f"{self.provider} does not support schema discovery")
        return await self._adapter.describe(self._target)

    async def validate(
        self,
        sql: str,
        *,
        policy: SQLPolicy | None = None,
        context: Context | None = None,
    ) -> ValidationResult:
        active_policy = policy or SQLPolicy()
        bridge = self._bridge(active_policy)
        return await bridge.validate(
            artifact=Artifact(sql, "sql"),
            target=self._execution_target(),
            context=context or Context(),
            policy=self._adapter.capabilities().policy_requirements(active_policy),
        )

    async def explain(self, sql: str) -> ExplainResult:
        if not self._adapter.capabilities().explain:
            return ExplainResult(supported=False)
        return await self._adapter.explain(sql, self._target)

    async def submit(
        self,
        sql: str,
        *,
        policy: SQLPolicy | None = None,
        context: Context | None = None,
    ) -> ExecutionHandle:
        active_policy = policy or SQLPolicy()
        bridge = self._install_bridge(active_policy)
        return await self._plane.submit(
            Artifact(sql, "sql"),
            target=self._execution_target(),
            context=context or Context(),
            policy=bridge.adapter.capabilities().policy_requirements(active_policy),
        )

    async def execute(
        self,
        sql: str,
        *,
        policy: SQLPolicy | None = None,
        context: Context | None = None,
        verify: Sequence[Verifier] = (),
        poll_interval_seconds: float = 0.05,
    ) -> Result:
        active_policy = policy or SQLPolicy()
        bridge = self._install_bridge(active_policy)
        return await self._plane.run(
            Artifact(sql, "sql"),
            target=self._execution_target(),
            context=context or Context(),
            policy=bridge.adapter.capabilities().policy_requirements(active_policy),
            verify=verify,
            poll_interval_seconds=poll_interval_seconds,
        )

    def query(
        self,
        *,
        read_only: bool = True,
        schemas: Sequence[str] = (),
        tables: Sequence[str] = (),
        denied_tables: Sequence[str] = (),
        max_rows: int = 1_000,
        timeout: float = 30,
        max_bytes_scanned: int | None = None,
        max_cost_usd: float | None = None,
        allow_multiple_statements: bool = False,
        verify: Sequence[Verifier] = (),
    ) -> SQLQuery:
        """Configure a governed query operation."""

        from gantry.sql.query import SQLQuery

        configured_policy = SQLPolicy(
            read_only=read_only,
            allowed_schemas=schemas,
            allowed_tables=tables,
            denied_tables=denied_tables,
            max_rows=max_rows,
            timeout_seconds=timeout,
            max_bytes_scanned=max_bytes_scanned,
            max_cost_usd=max_cost_usd,
            allow_multiple_statements=allow_multiple_statements,
        )
        return SQLQuery(self, configured_policy, tuple(verify))

    async def _query(
        self,
        sql: str,
        *,
        policy: SQLPolicy,
        context: Context | None = None,
        verify: Sequence[Verifier] = (),
    ) -> SQLResult:
        """Execute SQL for a configured query operation."""

        result = await self.execute(sql, policy=policy, context=context, verify=verify)
        inline = _find_inline(result, policy.max_rows)
        return SQLResult(
            status=result.status,
            handle=result.handle,
            inline=inline,
            outputs=result.outputs,
            metrics=result.metrics,
            verification=result.verification,
            failure=result.failure,
        )

    async def status(self, handle: ExecutionHandle) -> Execution:
        return await self._adapter.status(handle)

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> Execution:
        return await self._adapter.cancel(handle, mode)

    async def result(self, handle: ExecutionHandle) -> SQLResult:
        """Recover a completed result directly from its provider-native handle."""

        engine_result = await self._adapter.result(handle)
        maximum = handle.metadata.get("max_rows", 1_000)
        max_rows = maximum if isinstance(maximum, int) and maximum > 0 else 1_000
        return _from_engine_result(engine_result, max_rows)

    async def wait(
        self,
        handle: ExecutionHandle,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> SQLResult:
        """Observe a submitted job and recover its result, including after reconnect."""

        if poll_interval_seconds < 0:
            raise ValueError("poll interval must not be negative")
        while True:
            execution = await self.status(handle)
            if execution.state is ExecutionState.SUCCEEDED:
                return await self.result(handle)
            if execution.terminal:
                status = {
                    ExecutionState.CANCELLED: ResultStatus.CANCELLED,
                    ExecutionState.UNKNOWN: ResultStatus.UNKNOWN,
                }.get(execution.state, ResultStatus.FAILED)
                return SQLResult(
                    status,
                    handle=handle,
                    metrics=execution.metrics,
                    failure=execution.failure,
                )
            timeout = handle.metadata.get("timeout_seconds")
            if (
                isinstance(timeout, (int, float))
                and not isinstance(timeout, bool)
                and (datetime.now(UTC) - handle.submitted_at).total_seconds() >= timeout
            ):
                await self.cancel(handle)
                return SQLResult(
                    ResultStatus.FAILED,
                    handle=handle,
                    metrics=execution.metrics,
                    failure=Failure(
                        FailureKind.TIMEOUT,
                        True,
                        f"SQL execution exceeded {timeout} seconds",
                    ),
                )
            await asyncio.sleep(poll_interval_seconds)

    def materialize(
        self,
        *,
        sources: Sequence[str] = (),
        destinations: Sequence[str],
        create_only: bool = True,
        max_bytes_scanned: int | None = None,
        max_cost_usd: float | None = None,
        timeout: float = 300,
        verify: Sequence[MaterializationCheck] = (),
    ) -> SQLMaterializer:
        """Configure a governed, create-only native SQL materialization operation."""

        from gantry.sql.materialization import (
            MaterializationPolicy,
            SQLMaterializer,
        )
        from gantry.verify import MaterializationCheck

        checks: list[MaterializationCheck] = []
        for check in verify:
            if not isinstance(check, MaterializationCheck):
                raise TypeError("verify must contain materialization verification checks")
            checks.append(check)
        policy = MaterializationPolicy(
            sources=sources,
            destinations=destinations,
            create_only=create_only,
            timeout_seconds=timeout,
            max_bytes_scanned=max_bytes_scanned,
            max_cost_usd=max_cost_usd,
        )
        return SQLMaterializer(self, policy, checks)

    async def _inspect_table(
        self,
        reference: TableRef,
        *,
        include_row_count: bool = False,
    ) -> Table | None:
        from gantry.sql.materialization import MaterializationAdapter

        if not isinstance(self._adapter, MaterializationAdapter):
            raise NotImplementedError(f"{self.provider} does not support table introspection")
        return await self._adapter.inspect_table(
            reference,
            self._target,
            include_row_count=include_row_count,
        )

    def _bridge(self, policy: SQLPolicy) -> SQLExecutionAdapter:
        return SQLExecutionAdapter(self._adapter, self._dialect, self._target, policy)

    def _install_bridge(self, policy: SQLPolicy) -> SQLExecutionAdapter:
        bridge = self._bridge(policy)
        self._plane.register_adapter(self.provider, bridge)
        return bridge

    def _execution_target(self) -> ExecutionTarget:
        # Deliberately exclude provider configuration and credentials.
        return ExecutionTarget(self.provider, {"dialect": self.dialect})


def connect(provider: str, **config: object) -> SQLConnection:
    """Resolve a provider preset and create a governed SQL connection."""

    preset = resolve_provider(provider)
    preset.validate_config(config)
    target = SQLTarget(
        provider=preset.name,
        dialect=preset.dialect,
        driver=preset.driver,
        config=dict(config),
        metadata=preset.metadata,
    )
    return SQLConnection(target, preset.adapter_factory(target), resolve_dialect(preset.dialect))


def _find_inline(result: Result, max_rows: int) -> InlineRows | None:
    for output in result.outputs:
        candidate = output.metadata.get("inline")
        if isinstance(candidate, InlineRows):
            rows = candidate.rows[:max_rows]
            return InlineRows(
                candidate.columns,
                rows,
                truncated=candidate.truncated or len(candidate.rows) > max_rows,
            )
    return None


def _from_engine_result(result: ExecutionResult, max_rows: int) -> SQLResult:
    inline = None
    for output in result.outputs:
        candidate = output.metadata.get("inline")
        if isinstance(candidate, InlineRows):
            rows = candidate.rows[:max_rows]
            inline = InlineRows(
                candidate.columns,
                rows,
                truncated=candidate.truncated or len(candidate.rows) > max_rows,
            )
            break
    status = ResultStatus.ACCEPTED
    if not result.ok:
        status = (
            ResultStatus.CANCELLED
            if result.failure is not None and result.failure.kind is FailureKind.CANCELLED
            else ResultStatus.FAILED
        )
    return SQLResult(
        status,
        handle=result.handle,
        inline=inline,
        outputs=result.outputs,
        metrics=result.metrics,
        failure=result.failure,
    )
