# SPDX-License-Identifier: Apache-2.0
"""Public Flink SQL connection API."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gantry.capabilities import AdapterCapabilities
from gantry.context import Context
from gantry.execution import Execution, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.flink.adapter import FlinkAdapter
from gantry.flink.artifact import FlinkMode, FlinkSQLArtifact
from gantry.flink.execution import FlinkResult, StreamingHealth
from gantry.flink.metrics import FlinkMetrics
from gantry.flink.target import FlinkTarget
from gantry.flink.verification import FlinkHealthCheck, JobRunning, verify_health
from gantry.handle import ExecutionHandle
from gantry.policy import PolicyRequirements
from gantry.result import ResultStatus
from gantry.runtime import ControlPlane, SubmissionError
from gantry.store import MemoryExecutionStore
from gantry.verifier import VerificationResult

if TYPE_CHECKING:
    from gantry.flink.tool import FlinkTool


class FlinkConnection:
    """A credential-isolating control-plane connection to an existing cluster."""

    def __init__(self, target: FlinkTarget, adapter: FlinkAdapter | None = None) -> None:
        self._target = target
        self._adapter = adapter or FlinkAdapter(target)
        self._plane = ControlPlane(store=MemoryExecutionStore())
        self._plane.register_adapter(target.name, self._adapter)

    @property
    def target_name(self) -> str:
        return self._target.name

    def capabilities(self) -> AdapterCapabilities:
        return self._adapter.capabilities()

    async def validate(
        self,
        artifact: FlinkSQLArtifact | str,
        *,
        context: Context | None = None,
    ) -> ValidationResult:
        value = _coerce_artifact(artifact)
        return await self._adapter.validate(
            artifact=value.to_artifact(),
            target=self._target.execution_target(),
            context=context or Context(),
            policy=_policy(),
        )

    async def submit(
        self,
        artifact: FlinkSQLArtifact | str,
        *,
        context: Context | None = None,
    ) -> ExecutionHandle:
        value = _coerce_artifact(artifact)
        return await self._plane.submit(
            value.to_artifact(),
            target=self._target.execution_target(),
            context=context or Context(),
            policy=_policy(),
        )

    async def status(self, handle: ExecutionHandle) -> Execution:
        """Observe a job directly by native JobID; no local run record is required."""

        return await self._adapter.status(handle=handle)

    async def cancel(self, handle: ExecutionHandle) -> Execution:
        return await self._adapter.cancel(handle=handle)

    async def metrics(self, handle: ExecutionHandle) -> FlinkMetrics:
        return await self._adapter.metrics(handle)

    async def health(
        self,
        handle: ExecutionHandle,
        *,
        checks: Sequence[FlinkHealthCheck] = (),
    ) -> StreamingHealth:
        execution = await self.status(handle)
        return _streaming_health(execution, checks)

    async def verify(
        self,
        handle: ExecutionHandle,
        *,
        checks: Sequence[FlinkHealthCheck] = (),
    ) -> VerificationResult:
        return (await self.health(handle, checks=checks)).verification

    async def wait(
        self,
        handle: ExecutionHandle,
        *,
        checks: Sequence[FlinkHealthCheck] = (),
        poll_interval_seconds: float = 1.0,
        timeout_seconds: float | None = None,
    ) -> FlinkResult:
        """Wait for batch completion or streaming readiness at RUNNING."""

        if poll_interval_seconds < 0:
            raise ValueError("poll interval must not be negative")
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        mode = _mode_from_handle(handle)
        deadline = (
            None if timeout_seconds is None else datetime.now(UTC).timestamp() + timeout_seconds
        )
        while True:
            execution = await self.status(handle)
            metrics = FlinkMetrics.from_execution_metrics(execution.metrics)
            if mode is FlinkMode.STREAMING and execution.state is ExecutionState.RUNNING:
                health = _streaming_health(execution, checks)
                if not health.healthy:
                    return _verification_failure(handle, health)
                return FlinkResult(
                    ResultStatus.ACCEPTED,
                    handle=handle,
                    execution=health.execution,
                    outputs=self._adapter.outputs(handle),
                    metrics=health.metrics,
                    verification=health.verification,
                    health=health,
                )
            if mode is FlinkMode.BATCH and execution.state is ExecutionState.SUCCEEDED:
                verification = verify_health(execution, metrics, tuple(checks))
                if not verification.ok:
                    return FlinkResult(
                        ResultStatus.VERIFICATION_FAILED,
                        handle,
                        execution,
                        self._adapter.outputs(handle),
                        metrics,
                        verification,
                        failure=_verification_failure_value(verification),
                    )
                return FlinkResult(
                    ResultStatus.ACCEPTED,
                    handle,
                    execution,
                    self._adapter.outputs(handle),
                    metrics,
                    verification,
                )
            terminal = _terminal_result(execution, metrics)
            if terminal is not None:
                return terminal
            if deadline is not None and datetime.now(UTC).timestamp() >= deadline:
                await self.cancel(handle)
                return FlinkResult(
                    ResultStatus.FAILED,
                    handle=handle,
                    execution=execution,
                    metrics=metrics,
                    failure=Failure(
                        FailureKind.TIMEOUT,
                        True,
                        f"Flink job did not become ready within {timeout_seconds} seconds",
                    ),
                )
            await asyncio.sleep(poll_interval_seconds)

    async def run(
        self,
        artifact: FlinkSQLArtifact | str | None = None,
        *,
        sql: str | None = None,
        mode: FlinkMode | str = FlinkMode.STREAMING,
        declared_inputs: Sequence[str] = (),
        declared_outputs: Sequence[str] = (),
        context: Context | None = None,
        checks: Sequence[FlinkHealthCheck] = (),
        poll_interval_seconds: float = 1.0,
        timeout_seconds: float | None = None,
    ) -> FlinkResult:
        value = _coerce_run_artifact(
            artifact,
            sql=sql,
            mode=mode,
            declared_inputs=declared_inputs,
            declared_outputs=declared_outputs,
        )
        try:
            handle = await self.submit(value, context=context)
        except SubmissionError as error:
            result = error.result
            return FlinkResult(
                result.status,
                handle=result.handle,
                execution=result.execution,
                metrics=FlinkMetrics.from_execution_metrics(result.metrics),
                verification=result.verification,
                failure=result.failure,
            )
        return await self.wait(
            handle,
            checks=checks,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        )

    def as_tool(
        self,
        *,
        mode: FlinkMode | str = FlinkMode.STREAMING,
        checks: Sequence[FlinkHealthCheck] = (),
        timeout: float | None = None,
    ) -> FlinkTool:
        from gantry.flink.tool import FlinkTool

        return FlinkTool(self, FlinkMode(mode), tuple(checks), timeout)


def connect(
    endpoint: str,
    *,
    config: Mapping[str, object] | None = None,
    **options: object,
) -> FlinkConnection:
    """Connect to SQL Gateway, optionally using a separate JobManager endpoint."""

    values = dict(config or {})
    overlap = set(values) & set(options)
    if overlap:
        raise ValueError(f"duplicate Flink configuration fields: {', '.join(sorted(overlap))}")
    values.update(options)
    return FlinkConnection(FlinkTarget(endpoint, values))


def _coerce_artifact(value: FlinkSQLArtifact | str) -> FlinkSQLArtifact:
    return value if isinstance(value, FlinkSQLArtifact) else FlinkSQLArtifact(value)


def _coerce_run_artifact(
    artifact: FlinkSQLArtifact | str | None,
    *,
    sql: str | None,
    mode: FlinkMode | str,
    declared_inputs: Sequence[str],
    declared_outputs: Sequence[str],
) -> FlinkSQLArtifact:
    if artifact is not None and sql is not None:
        raise ValueError("pass either artifact or sql, not both")
    if artifact is None:
        if sql is None:
            raise ValueError("artifact or sql is required")
        return FlinkSQLArtifact(sql, mode, declared_inputs, declared_outputs)
    if sql is not None or mode != FlinkMode.STREAMING or declared_inputs or declared_outputs:
        raise ValueError("mode and declarations must be part of an explicit artifact")
    return _coerce_artifact(artifact)


def _policy() -> PolicyRequirements:
    return PolicyRequirements(
        allow_writes=True,
        require_cancel=True,
        require_reconnect=True,
        require_scoped_credentials=True,
        require_metrics=True,
        require_result_reference=True,
    )


def _mode_from_handle(handle: ExecutionHandle) -> FlinkMode:
    value = handle.metadata.get("mode", "streaming")
    try:
        if not isinstance(value, str):
            raise TypeError
        return FlinkMode(value)
    except (TypeError, ValueError):
        return FlinkMode.STREAMING


def _health_checks(checks: Sequence[FlinkHealthCheck]) -> tuple[FlinkHealthCheck, ...]:
    values = tuple(checks)
    if any(isinstance(check, JobRunning) for check in values):
        return values
    return (JobRunning(), *values)


def _streaming_health(execution: Execution, checks: Sequence[FlinkHealthCheck]) -> StreamingHealth:
    metrics = FlinkMetrics.from_execution_metrics(execution.metrics)
    verification = verify_health(execution, metrics, _health_checks(checks))
    return StreamingHealth(
        healthy=execution.state is ExecutionState.RUNNING and verification.ok,
        execution=execution,
        metrics=metrics,
        verification=verification,
    )


def _terminal_result(execution: Execution, metrics: FlinkMetrics) -> FlinkResult | None:
    if execution.state not in {
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
        ExecutionState.UNKNOWN,
    }:
        return None
    status = {
        ExecutionState.CANCELLED: ResultStatus.CANCELLED,
        ExecutionState.UNKNOWN: ResultStatus.UNKNOWN,
    }.get(execution.state, ResultStatus.FAILED)
    return FlinkResult(
        status,
        handle=execution.handle,
        execution=execution,
        metrics=metrics,
        failure=execution.failure,
    )


def _verification_failure(handle: ExecutionHandle, health: StreamingHealth) -> FlinkResult:
    return FlinkResult(
        ResultStatus.VERIFICATION_FAILED,
        handle,
        health.execution,
        metrics=health.metrics,
        verification=health.verification,
        health=health,
        failure=_verification_failure_value(health.verification),
    )


def _verification_failure_value(verification: VerificationResult) -> Failure:
    message = next(
        (check.message for check in verification.checks if not check.ok and check.message),
        "Flink health verification failed",
    )
    return Failure(FailureKind.VERIFICATION_FAILED, False, message)
