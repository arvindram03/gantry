# SPDX-License-Identifier: Apache-2.0
"""Durable multi-engine submission, observation, recovery, and verification."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime

from gantry.adapter import ExecutionAdapter
from gantry.admission import AdmissionDecision, admit
from gantry.artifact import Artifact
from gantry.capabilities import AdapterCapabilities
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.policy import PolicyRequirements
from gantry.result import Result, ResultStatus
from gantry.store import ExecutionStore, MemoryExecutionStore, RunRecord
from gantry.target import ExecutionTarget
from gantry.verifier import CheckResult, VerificationResult, Verifier


def _exception_failure(kind: FailureKind, stage: str, error: Exception) -> Failure:
    detail = str(error)
    suffix = f": {detail}" if detail else ""
    return Failure(
        kind=kind, retryable=False, message=f"{stage} raised {type(error).__name__}{suffix}"
    )


def _unknown(message: str, *, handle: ExecutionHandle | None = None) -> Result:
    return Result.terminal_failure(
        ResultStatus.UNKNOWN,
        Failure(kind=FailureKind.UNKNOWN, retryable=False, message=message),
        handle=handle,
    )


class SubmissionError(Exception):
    """Structured rejection or submission failure from :meth:`ControlPlane.submit`."""

    def __init__(self, result: Result) -> None:
        self.result = result
        message = result.failure.message if result.failure is not None else result.status.value
        super().__init__(message)


class ControlPlane:
    """Routes executions to adapters by target kind and records their state.

    Holds the adapter registry and the execution store behind the
    module-level `submit`/`get`/`wait`/`cancel`/`run` helpers. Construct one
    directly to keep adapters and history isolated from the process default.
    """

    def __init__(
        self,
        adapters: Mapping[str, ExecutionAdapter] | None = None,
        store: ExecutionStore | None = None,
    ) -> None:
        self._adapters = dict(adapters or {})
        self._store = store or MemoryExecutionStore()

    def register_adapter(self, target_kind: str, adapter: ExecutionAdapter) -> None:
        if not target_kind.strip():
            raise ValueError("target kind must not be empty")
        self._adapters[target_kind] = adapter

    def _adapter(self, target_kind: str) -> ExecutionAdapter | None:
        return self._adapters.get(target_kind)

    async def submit(
        self,
        artifact: Artifact,
        *,
        target: ExecutionTarget,
        context: Context,
        policy: PolicyRequirements,
    ) -> ExecutionHandle:
        adapter = self._adapter(target.kind)
        if adapter is None:
            failure = Failure(
                kind=FailureKind.VALIDATION_ERROR,
                retryable=False,
                message=f"no adapter registered for target: {target.kind}",
            )
            admission = AdmissionDecision(
                allowed=False,
                validation=ValidationResult.rejected(failure.message),
                capabilities=_empty_capabilities(),
                reasons=(failure,),
            )
            raise SubmissionError(Result.rejected(admission))

        try:
            capabilities_value: object = adapter.capabilities()
        except Exception as error:
            failure = _exception_failure(
                FailureKind.VALIDATION_ERROR, "adapter capabilities", error
            )
            admission = AdmissionDecision(
                allowed=False,
                validation=ValidationResult.rejected(failure.message),
                capabilities=_empty_capabilities(),
                reasons=(failure,),
            )
            raise SubmissionError(Result.rejected(admission)) from error
        if not isinstance(capabilities_value, AdapterCapabilities):
            failure = Failure(
                kind=FailureKind.VALIDATION_ERROR,
                retryable=False,
                message="adapter returned invalid capabilities",
            )
            admission = AdmissionDecision(
                allowed=False,
                validation=ValidationResult.rejected(failure.message),
                capabilities=_empty_capabilities(),
                reasons=(failure,),
            )
            raise SubmissionError(Result.rejected(admission))
        capabilities = capabilities_value

        try:
            validation_value: object = await adapter.validate(
                artifact=artifact,
                target=target,
                context=context,
                policy=policy,
            )
        except Exception as error:
            validation_value = ValidationResult.rejected(
                _exception_failure(
                    FailureKind.VALIDATION_ERROR, "adapter validation", error
                ).message
            )
        validation = (
            validation_value
            if isinstance(validation_value, ValidationResult)
            else ValidationResult.rejected("adapter returned an invalid validation result")
        )
        admission = admit(validation, capabilities, policy)
        if not admission.allowed:
            raise SubmissionError(Result.rejected(admission))

        try:
            handle_value: object = await adapter.submit(
                artifact=artifact,
                target=target,
                context=context,
            )
        except Exception as error:
            failure = _exception_failure(FailureKind.SUBMISSION_ERROR, "adapter submission", error)
            raise SubmissionError(
                Result.terminal_failure(ResultStatus.FAILED, failure, admission=admission)
            ) from error
        if not isinstance(handle_value, ExecutionHandle):
            failure = Failure(
                kind=FailureKind.SUBMISSION_ERROR,
                retryable=False,
                message="adapter returned an invalid execution handle",
            )
            raise SubmissionError(
                Result.terminal_failure(ResultStatus.FAILED, failure, admission=admission)
            )
        handle = handle_value
        if handle.target != target.kind:
            failure = Failure(
                kind=FailureKind.SUBMISSION_ERROR,
                retryable=False,
                message="execution handle target does not match the submitted target",
            )
            raise SubmissionError(
                Result.terminal_failure(
                    ResultStatus.FAILED,
                    failure,
                    handle=handle,
                    admission=admission,
                )
            )

        record = RunRecord(handle, artifact, target, context, policy, admission)
        try:
            await self._store.put(record)
        except Exception as error:
            with suppress(Exception):
                await adapter.cancel(handle=handle)
            failure = _exception_failure(FailureKind.SUBMISSION_ERROR, "execution store", error)
            raise SubmissionError(
                Result.terminal_failure(
                    ResultStatus.FAILED,
                    failure,
                    handle=handle,
                    admission=admission,
                )
            ) from error
        return handle

    async def get(self, handle: ExecutionHandle) -> Execution:
        adapter = self._adapter(handle.target)
        if adapter is None:
            return _unknown_execution(handle, f"no adapter registered for target: {handle.target}")
        try:
            execution_value: object = await adapter.status(handle=handle)
        except Exception as error:
            return _unknown_execution(
                handle,
                _exception_failure(FailureKind.UNKNOWN, "adapter status", error).message,
            )
        if not isinstance(execution_value, Execution):
            return _unknown_execution(handle, "adapter returned an invalid execution")
        if execution_value.handle != handle:
            return _unknown_execution(handle, "execution handle does not match the requested job")
        return execution_value

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> Execution:
        adapter = self._adapter(handle.target)
        if adapter is None:
            return _unknown_execution(handle, f"no adapter registered for target: {handle.target}")
        try:
            execution_value: object = await adapter.cancel(handle=handle, mode=mode)
        except Exception as error:
            return _unknown_execution(
                handle,
                _exception_failure(FailureKind.UNKNOWN, "adapter cancellation", error).message,
            )
        if not isinstance(execution_value, Execution) or execution_value.handle != handle:
            return _unknown_execution(handle, "adapter returned an invalid cancelled execution")
        return execution_value

    async def wait(
        self,
        handle: ExecutionHandle,
        *,
        verify: Sequence[Verifier] = (),
        poll_interval_seconds: float = 1.0,
    ) -> Result:
        if poll_interval_seconds < 0:
            raise ValueError("poll interval must not be negative")
        try:
            record = await self._store.get(handle.gantry_id)
        except Exception as error:
            return _unknown(
                _exception_failure(FailureKind.UNKNOWN, "execution store", error).message,
                handle=handle,
            )
        if record is None or record.handle != handle:
            return _unknown("no persisted execution record matches the handle", handle=handle)

        adapter = self._adapter(handle.target)
        if adapter is None:
            return _unknown(f"no adapter registered for target: {handle.target}", handle=handle)

        while True:
            execution = await self.get(handle)
            if execution.state is ExecutionState.SUCCEEDED:
                break
            if execution.state is ExecutionState.CANCELLED:
                failure = execution.failure or Failure(
                    kind=FailureKind.CANCELLED,
                    retryable=False,
                    message="execution was cancelled",
                )
                return Result.terminal_failure(
                    ResultStatus.CANCELLED,
                    failure,
                    handle=handle,
                    execution=execution,
                    admission=record.admission,
                )
            if execution.state is ExecutionState.FAILED:
                failure = execution.failure or Failure(
                    kind=FailureKind.ENGINE_ERROR,
                    retryable=False,
                    message="engine execution failed",
                )
                return Result.terminal_failure(
                    ResultStatus.FAILED,
                    failure,
                    handle=handle,
                    execution=execution,
                    admission=record.admission,
                )
            if execution.state is ExecutionState.UNKNOWN:
                return Result.terminal_failure(
                    ResultStatus.UNKNOWN,
                    execution.failure
                    or Failure(FailureKind.UNKNOWN, False, "execution state is unknown"),
                    handle=handle,
                    execution=execution,
                    admission=record.admission,
                )
            if _timed_out(execution, record.policy, record.admission.capabilities):
                await self.cancel(handle)
                failure = Failure(
                    kind=FailureKind.TIMEOUT,
                    retryable=True,
                    message=f"execution exceeded {record.policy.max_runtime_seconds} seconds",
                )
                return Result.terminal_failure(
                    ResultStatus.FAILED,
                    failure,
                    handle=handle,
                    execution=execution,
                    admission=record.admission,
                )
            await asyncio.sleep(poll_interval_seconds)

        try:
            engine_result_value: object = await adapter.result(handle=handle)
        except Exception as error:
            failure = _exception_failure(FailureKind.ENGINE_ERROR, "adapter result", error)
            return Result.terminal_failure(
                ResultStatus.FAILED,
                failure,
                handle=handle,
                execution=execution,
                admission=record.admission,
            )
        if not isinstance(engine_result_value, ExecutionResult):
            failure = Failure(FailureKind.ENGINE_ERROR, False, "adapter returned an invalid result")
            return Result.terminal_failure(
                ResultStatus.FAILED,
                failure,
                handle=handle,
                execution=execution,
                admission=record.admission,
            )
        engine_result = engine_result_value
        if engine_result.handle != handle:
            failure = Failure(
                FailureKind.ENGINE_ERROR, False, "result handle does not match execution"
            )
            return Result.terminal_failure(
                ResultStatus.FAILED,
                failure,
                handle=handle,
                execution=execution,
                admission=record.admission,
            )
        if not engine_result.ok:
            return Result.terminal_failure(
                ResultStatus.FAILED,
                engine_result.failure
                or Failure(FailureKind.ENGINE_ERROR, False, "engine result failed"),
                handle=handle,
                execution=execution,
                admission=record.admission,
            )

        verification = await _verify_all(
            verify,
            artifact=record.artifact,
            context=record.context,
            execution=execution,
            engine_result=engine_result,
        )
        return Result.from_execution(
            execution=execution,
            engine_result=engine_result,
            verification=verification,
            admission=record.admission,
        )

    async def run(
        self,
        artifact: Artifact,
        *,
        target: ExecutionTarget,
        context: Context,
        policy: PolicyRequirements,
        verify: Sequence[Verifier] = (),
        poll_interval_seconds: float = 1.0,
    ) -> Result:
        try:
            handle = await self.submit(artifact, target=target, context=context, policy=policy)
        except SubmissionError as error:
            return error.result
        return await self.wait(
            handle,
            verify=verify,
            poll_interval_seconds=poll_interval_seconds,
        )


def _empty_capabilities() -> AdapterCapabilities:
    return AdapterCapabilities()


def _unknown_execution(handle: ExecutionHandle, message: str) -> Execution:
    return Execution(
        handle=handle,
        state=ExecutionState.UNKNOWN,
        updated_at=datetime.now(UTC),
        failure=Failure(FailureKind.UNKNOWN, False, message),
    )


def _timed_out(
    execution: Execution,
    policy: PolicyRequirements,
    capabilities: AdapterCapabilities,
) -> bool:
    if policy.max_runtime_seconds is None or capabilities.runtime_limit:
        return False
    started = execution.started_at or execution.handle.submitted_at
    return (datetime.now(UTC) - started).total_seconds() >= policy.max_runtime_seconds


async def _verify_all(
    verifiers: Sequence[Verifier],
    *,
    artifact: Artifact,
    context: Context,
    execution: Execution,
    engine_result: ExecutionResult,
) -> VerificationResult:
    checks: list[CheckResult] = []
    ok = True
    for verifier in verifiers:
        try:
            value: object = await verifier.verify(
                artifact=artifact,
                context=context,
                execution=execution,
                result=engine_result,
            )
        except Exception as error:
            value = VerificationResult.failed(
                _exception_failure(FailureKind.VERIFICATION_FAILED, "verifier", error).message
            )
        if not isinstance(value, VerificationResult):
            value = VerificationResult.failed("verifier returned an invalid result")
        ok = ok and value.ok
        checks.extend(value.checks)
    return VerificationResult(ok=ok and all(check.ok for check in checks), checks=tuple(checks))


_default = ControlPlane()


def register_adapter(target_kind: str, adapter: ExecutionAdapter) -> None:
    """Register `adapter` on the process-default control plane for `target_kind`.

    Raises `ValueError` if the target kind is empty. A later registration for
    the same kind replaces the earlier one.
    """
    _default.register_adapter(target_kind, adapter)


def configure(
    *, adapters: Mapping[str, ExecutionAdapter], store: ExecutionStore | None = None
) -> None:
    """Replace the default control plane's adapters and execution store."""
    global _default
    _default = ControlPlane(adapters, store)


async def submit(
    artifact: Artifact,
    *,
    target: ExecutionTarget,
    context: Context,
    policy: PolicyRequirements,
) -> ExecutionHandle:
    """Validate, admit and start an artifact, returning a durable handle.

    The handle outlives this call, so work can be polled or cancelled from
    another process. Admission happens here: a policy the adapter cannot
    enforce is refused before anything runs.
    """
    return await _default.submit(artifact, target=target, context=context, policy=policy)


async def get(handle: ExecutionHandle) -> Execution:
    """Return the current `Execution` for `handle` without waiting.

    Refreshes state from the adapter registered for the handle's target. Never
    raises: a missing adapter, an adapter that fails, or a reply for a
    different handle all come back as an `Execution` in state `UNKNOWN`
    carrying the reason as its failure.
    """
    return await _default.get(handle)


async def wait(
    handle: ExecutionHandle,
    *,
    verify: Sequence[Verifier] = (),
    poll_interval_seconds: float = 1.0,
) -> Result:
    """Block until an execution finishes, then verify it.

    A terminal engine state is not the answer on its own — the verifiers decide
    whether the result may be believed, and their outcome is part of the
    `Result`.
    """
    return await _default.wait(
        handle,
        verify=verify,
        poll_interval_seconds=poll_interval_seconds,
    )


async def cancel(handle: ExecutionHandle, *, mode: str = "default") -> Execution:
    """Ask the adapter owning `handle` to stop that execution.

    `mode` is passed through to the adapter; engines that distinguish a
    graceful stop from a hard kill interpret it. Returns the `Execution` the
    adapter reports after the request, which may still be running if the
    engine stops asynchronously. Like `get`, a missing or failing adapter
    yields an `Execution` in state `UNKNOWN` rather than an exception.
    """
    return await _default.cancel(handle, mode=mode)


async def run(
    artifact: Artifact,
    *,
    target: ExecutionTarget,
    context: Context,
    policy: PolicyRequirements,
    verify: Sequence[Verifier] = (),
    poll_interval_seconds: float = 1.0,
) -> Result:
    """Submit an artifact and wait for it, returning the verified result.

    The common case. Use `submit` and `wait` separately when the work outlives
    the request that started it.
    """
    return await _default.run(
        artifact,
        target=target,
        context=context,
        policy=policy,
        verify=verify,
        poll_interval_seconds=poll_interval_seconds,
    )
