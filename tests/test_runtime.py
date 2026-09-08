# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from gantry import (
    AdapterCapabilities,
    AdmissionDecision,
    Artifact,
    CheckResult,
    Context,
    ControlPlane,
    Execution,
    ExecutionAdapter,
    ExecutionHandle,
    ExecutionMetrics,
    ExecutionResult,
    ExecutionState,
    ExecutionTarget,
    Failure,
    FailureKind,
    MemoryExecutionStore,
    OutputKind,
    OutputRef,
    PolicyRequirements,
    ResultStatus,
    RunRecord,
    SubmissionError,
    ValidationResult,
    VerificationResult,
    admit,
    configure,
)
from gantry import (
    cancel as public_cancel,
)
from gantry import (
    get as public_get,
)
from gantry import (
    register_adapter as public_register_adapter,
)
from gantry import (
    run as public_run,
)
from gantry import (
    submit as public_submit,
)
from gantry import (
    wait as public_wait,
)


class StubAdapter:
    def __init__(self) -> None:
        self.caps = AdapterCapabilities(
            reconnect=True,
            cancellation=True,
            read_only_execution=True,
            metrics=True,
            result_reference=True,
        )
        self.handle = ExecutionHandle("run-123", "sql", "bigquery", "query-456")
        self.executions = [Execution(self.handle, ExecutionState.SUCCEEDED)]
        self.engine_result = ExecutionResult.succeeded(
            self.handle,
            outputs=(OutputRef(OutputKind.TABLE, "bigquery://acme.analytics.result"),),
            metrics=ExecutionMetrics(rows_written=84_291, bytes_read=2_000_000),
        )
        self.validation: object = ValidationResult.accepted()
        self.submission: object = self.handle
        self.events: list[str] = []
        self.cancel_mode: str | None = None
        self.raise_stage: str | None = None
        self.invalid_capabilities = False

    def capabilities(self) -> AdapterCapabilities:
        self.events.append("capabilities")
        if self.raise_stage == "capabilities":
            raise RuntimeError("capabilities unavailable")
        if self.invalid_capabilities:
            return cast(AdapterCapabilities, object())
        return self.caps

    async def validate(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
        policy: PolicyRequirements,
    ) -> ValidationResult:
        self.events.append("validate")
        if self.raise_stage == "validate":
            raise RuntimeError("validation unavailable")
        return cast(ValidationResult, self.validation)

    async def submit(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
    ) -> ExecutionHandle:
        self.events.append("submit")
        if self.raise_stage == "submit":
            raise RuntimeError("submission unavailable")
        return cast(ExecutionHandle, self.submission)

    async def status(self, *, handle: ExecutionHandle) -> Execution:
        self.events.append("status")
        if self.raise_stage == "status":
            raise RuntimeError("status unavailable")
        if len(self.executions) > 1:
            return self.executions.pop(0)
        return self.executions[0]

    async def result(self, *, handle: ExecutionHandle) -> ExecutionResult:
        self.events.append("result")
        if self.raise_stage == "result":
            raise RuntimeError("result unavailable")
        return self.engine_result

    async def cancel(self, *, handle: ExecutionHandle, mode: str = "default") -> Execution:
        self.events.append("cancel")
        if self.raise_stage == "cancel":
            raise RuntimeError("cancellation unavailable")
        self.cancel_mode = mode
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, "cancelled"),
        )


class RowCountVerifier:
    def __init__(self, ok: bool = True) -> None:
        self.ok = ok

    async def verify(
        self,
        *,
        artifact: Artifact,
        context: Context,
        execution: Execution,
        result: ExecutionResult,
    ) -> VerificationResult:
        actual = result.metrics.rows_written
        check = CheckResult("non_empty", self.ok, "> 0", actual, None if self.ok else "empty")
        return VerificationResult(ok=self.ok, checks=(check,))


class BrokenVerifier:
    def __init__(self, *, raises: bool) -> None:
        self.raises = raises

    async def verify(
        self,
        *,
        artifact: Artifact,
        context: Context,
        execution: Execution,
        result: ExecutionResult,
    ) -> VerificationResult:
        if self.raises:
            raise RuntimeError("verification unavailable")
        return cast(VerificationResult, object())


class EmptyFailedVerifier:
    async def verify(
        self,
        *,
        artifact: Artifact,
        context: Context,
        execution: Execution,
        result: ExecutionResult,
    ) -> VerificationResult:
        return VerificationResult(ok=False)


class FailingStore(MemoryExecutionStore):
    def __init__(self, stage: str) -> None:
        super().__init__()
        self.stage = stage

    async def put(self, record: RunRecord) -> None:
        if self.stage == "put":
            raise RuntimeError("store unavailable")
        await super().put(record)

    async def get(self, gantry_id: str) -> RunRecord | None:
        if self.stage == "get":
            raise RuntimeError("store unavailable")
        return await super().get(gantry_id)


def request() -> tuple[Artifact, ExecutionTarget, Context, PolicyRequirements]:
    return (
        Artifact("select count(*) from events", "sql"),
        ExecutionTarget("bigquery", {"project": "acme"}),
        Context(resources={"credentials": "scoped"}),
        PolicyRequirements(
            read_only=True,
            require_reconnect=True,
            require_result_reference=True,
        ),
    )


async def submit(plane: ControlPlane) -> ExecutionHandle:
    artifact, target, context, policy = request()
    return await plane.submit(artifact, target=target, context=context, policy=policy)


async def test_submit_returns_and_persists_durable_handle() -> None:
    adapter = StubAdapter()
    store = MemoryExecutionStore()
    plane = ControlPlane({"bigquery": adapter}, store)

    handle = await submit(plane)

    assert handle.native_id == "query-456"
    assert await store.get("run-123") is not None
    assert adapter.events == ["capabilities", "validate", "submit"]


async def test_new_control_plane_reconnects_with_only_handle_and_store() -> None:
    adapter = StubAdapter()
    store = MemoryExecutionStore()
    first_process = ControlPlane({"bigquery": adapter}, store)
    handle = await submit(first_process)
    second_process = ControlPlane({"bigquery": adapter}, store)

    execution = await second_process.get(handle)
    result = await second_process.wait(handle, verify=[RowCountVerifier()], poll_interval_seconds=0)

    assert execution.state is ExecutionState.SUCCEEDED
    assert result.status is ResultStatus.ACCEPTED
    assert result.is_accepted
    assert result.outputs[0].uri == "bigquery://acme.analytics.result"
    assert result.metrics.bytes_read == 2_000_000


async def test_wait_observes_pending_submitted_and_running_states() -> None:
    adapter = StubAdapter()
    adapter.executions = [
        Execution(adapter.handle, ExecutionState.PENDING),
        Execution(adapter.handle, ExecutionState.SUBMITTED),
        Execution(
            adapter.handle, ExecutionState.RUNNING, metrics=ExecutionMetrics(worker_count=12)
        ),
        Execution(adapter.handle, ExecutionState.SUCCEEDED),
    ]
    plane = ControlPlane({"bigquery": adapter})
    handle = await submit(plane)

    result = await plane.wait(handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.ACCEPTED
    assert adapter.events.count("status") == 4


@pytest.mark.parametrize(
    ("state", "status", "kind"),
    [
        (ExecutionState.FAILED, ResultStatus.FAILED, FailureKind.USER_CODE_ERROR),
        (ExecutionState.CANCELLED, ResultStatus.CANCELLED, FailureKind.CANCELLED),
        (ExecutionState.UNKNOWN, ResultStatus.UNKNOWN, FailureKind.UNKNOWN),
    ],
)
async def test_terminal_engine_states_are_normalized(
    state: ExecutionState,
    status: ResultStatus,
    kind: FailureKind,
) -> None:
    adapter = StubAdapter()
    adapter.executions = [Execution(adapter.handle, state, failure=Failure(kind, False, "stopped"))]
    plane = ControlPlane({"bigquery": adapter})
    handle = await submit(plane)

    result = await plane.wait(handle, poll_interval_seconds=0)

    assert result.status is status
    assert result.failure is not None
    assert result.failure.kind is kind
    assert "result" not in adapter.events


async def test_verification_failure_is_not_engine_failure() -> None:
    plane = ControlPlane({"bigquery": StubAdapter()})
    handle = await submit(plane)

    result = await plane.wait(handle, verify=[RowCountVerifier(False)], poll_interval_seconds=0)

    assert result.status is ResultStatus.VERIFICATION_FAILED
    assert result.execution is not None
    assert result.execution.state is ExecutionState.SUCCEEDED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.VERIFICATION_FAILED


async def test_cancel_forwards_adapter_specific_mode() -> None:
    adapter = StubAdapter()
    plane = ControlPlane({"bigquery": adapter})
    handle = await submit(plane)

    execution = await plane.cancel(handle, mode="drain")

    assert execution.state is ExecutionState.CANCELLED
    assert adapter.cancel_mode == "drain"


async def test_gantry_enforces_timeout_when_adapter_can_reconnect_and_cancel() -> None:
    adapter = StubAdapter()
    old = datetime.now(UTC) - timedelta(seconds=30)
    adapter.handle = ExecutionHandle("run-123", "sql", "bigquery", "query-456", old)
    adapter.submission = adapter.handle
    adapter.executions = [Execution(adapter.handle, ExecutionState.RUNNING, started_at=old)]
    adapter.engine_result = ExecutionResult.succeeded(adapter.handle)
    plane = ControlPlane({"bigquery": adapter})
    artifact, target, context, _ = request()
    handle = await plane.submit(
        artifact,
        target=target,
        context=context,
        policy=PolicyRequirements(max_runtime_seconds=1),
    )

    result = await plane.wait(handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.FAILED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.TIMEOUT
    assert "cancel" in adapter.events


async def test_native_runtime_limit_is_left_to_adapter() -> None:
    adapter = StubAdapter()
    adapter.caps = AdapterCapabilities(runtime_limit=True)
    old = datetime.now(UTC) - timedelta(seconds=30)
    adapter.handle = ExecutionHandle("run-123", "sql", "bigquery", "query-456", old)
    adapter.submission = adapter.handle
    adapter.executions = [
        Execution(adapter.handle, ExecutionState.RUNNING, started_at=old),
        Execution(adapter.handle, ExecutionState.SUCCEEDED, started_at=old),
    ]
    adapter.engine_result = ExecutionResult.succeeded(adapter.handle)
    plane = ControlPlane({"bigquery": adapter})
    artifact, target, context, _ = request()
    handle = await plane.submit(
        artifact,
        target=target,
        context=context,
        policy=PolicyRequirements(max_runtime_seconds=1),
    )

    result = await plane.wait(handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.ACCEPTED
    assert "cancel" not in adapter.events


async def test_run_is_submit_plus_wait() -> None:
    artifact, target, context, policy = request()
    result = await ControlPlane({"bigquery": StubAdapter()}).run(
        artifact,
        target=target,
        context=context,
        policy=policy,
        verify=[RowCountVerifier()],
        poll_interval_seconds=0,
    )

    assert result.status is ResultStatus.ACCEPTED


async def test_run_returns_submission_rejection() -> None:
    artifact, target, context, policy = request()

    result = await ControlPlane().run(
        artifact,
        target=target,
        context=context,
        policy=policy,
        poll_interval_seconds=0,
    )

    assert result.status is ResultStatus.REJECTED


@pytest.mark.parametrize("failure_mode", ["missing_adapter", "validation", "capability", "submit"])
async def test_submission_failures_are_structured(failure_mode: str) -> None:
    adapter = StubAdapter()
    adapters: dict[str, ExecutionAdapter] = {"bigquery": adapter}
    if failure_mode == "missing_adapter":
        adapters = {}
    elif failure_mode == "validation":
        adapter.validation = ValidationResult.rejected("SQL is not read-only")
    elif failure_mode == "capability":
        adapter.caps = AdapterCapabilities()
    else:
        adapter.submission = object()
    plane = ControlPlane(adapters)

    with pytest.raises(SubmissionError) as caught:
        await submit(plane)

    assert caught.value.result.status in {ResultStatus.REJECTED, ResultStatus.FAILED}
    assert caught.value.result.failure is not None


async def test_handle_target_must_match_submission() -> None:
    adapter = StubAdapter()
    adapter.handle = ExecutionHandle("run-123", "sql", "snowflake", "query-456")
    adapter.submission = adapter.handle
    plane = ControlPlane({"bigquery": adapter})

    with pytest.raises(SubmissionError, match="target"):
        await submit(plane)


async def test_missing_persisted_record_cannot_be_verified() -> None:
    adapter = StubAdapter()
    result = await ControlPlane({"bigquery": adapter}).wait(adapter.handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.UNKNOWN


async def test_wait_requires_adapter_after_reconnection() -> None:
    adapter = StubAdapter()
    store = MemoryExecutionStore()
    handle = await submit(ControlPlane({"bigquery": adapter}, store))

    result = await ControlPlane(store=store).wait(handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.UNKNOWN


async def test_unknown_adapter_cannot_observe_or_cancel() -> None:
    handle = StubAdapter().handle
    plane = ControlPlane()

    observed = await plane.get(handle)
    cancelled = await plane.cancel(handle)

    assert observed.state is ExecutionState.UNKNOWN
    assert cancelled.state is ExecutionState.UNKNOWN


async def test_negative_poll_interval_is_invalid() -> None:
    adapter = StubAdapter()
    plane = ControlPlane({"bigquery": adapter})
    handle = await submit(plane)

    with pytest.raises(ValueError, match="negative"):
        await plane.wait(handle, poll_interval_seconds=-1)


def test_adapter_registration_validates_target_kind() -> None:
    plane = ControlPlane()
    with pytest.raises(ValueError, match="target kind"):
        plane.register_adapter("", StubAdapter())

    plane.register_adapter("bigquery", StubAdapter())


@pytest.mark.parametrize("stage", ["capabilities", "validate", "submit"])
async def test_adapter_submission_exceptions_fail_closed(stage: str) -> None:
    adapter = StubAdapter()
    adapter.raise_stage = stage

    with pytest.raises(SubmissionError) as caught:
        await submit(ControlPlane({"bigquery": adapter}))

    assert caught.value.result.failure is not None


async def test_invalid_capabilities_fail_closed() -> None:
    adapter = StubAdapter()
    adapter.invalid_capabilities = True

    with pytest.raises(SubmissionError, match="invalid capabilities"):
        await submit(ControlPlane({"bigquery": adapter}))


async def test_store_failure_after_submission_attempts_cancellation() -> None:
    adapter = StubAdapter()

    with pytest.raises(SubmissionError, match="execution store"):
        await submit(ControlPlane({"bigquery": adapter}, FailingStore("put")))

    assert "cancel" in adapter.events


async def test_store_read_failure_returns_unknown() -> None:
    adapter = StubAdapter()
    store = FailingStore("get")
    plane = ControlPlane({"bigquery": adapter}, store)
    artifact, target, context, policy = request()
    await store.put(
        RunRecord(adapter.handle, artifact, target, context, policy, _admission(adapter))
    )

    result = await plane.wait(adapter.handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.UNKNOWN
    assert result.failure is not None
    assert "execution store" in result.failure.message


@pytest.mark.parametrize("mode", ["raises", "invalid", "mismatch"])
async def test_bad_status_response_becomes_unknown(mode: str) -> None:
    adapter = StubAdapter()
    if mode == "raises":
        adapter.raise_stage = "status"
    elif mode == "invalid":
        adapter.executions = [cast(Execution, object())]
    else:
        other = ExecutionHandle("other", "sql", "bigquery", "other")
        adapter.executions = [Execution(other, ExecutionState.RUNNING)]

    execution = await ControlPlane({"bigquery": adapter}).get(adapter.handle)

    assert execution.state is ExecutionState.UNKNOWN


@pytest.mark.parametrize("mode", ["raises", "invalid"])
async def test_bad_cancellation_response_becomes_unknown(mode: str) -> None:
    adapter = StubAdapter()
    if mode == "raises":
        adapter.raise_stage = "cancel"
    else:

        async def invalid_cancel(*, handle: ExecutionHandle, mode: str = "default") -> Execution:
            return cast(Execution, object())

        adapter.cancel = invalid_cancel  # type: ignore[method-assign]

    execution = await ControlPlane({"bigquery": adapter}).cancel(adapter.handle)

    assert execution.state is ExecutionState.UNKNOWN


@pytest.mark.parametrize("mode", ["raises", "invalid", "mismatch", "failed"])
async def test_bad_engine_result_is_normalized(mode: str) -> None:
    adapter = StubAdapter()
    plane = ControlPlane({"bigquery": adapter})
    handle = await submit(plane)
    if mode == "raises":
        adapter.raise_stage = "result"
    elif mode == "invalid":
        adapter.engine_result = cast(ExecutionResult, object())
    elif mode == "mismatch":
        other = ExecutionHandle("other", "sql", "bigquery", "other")
        adapter.engine_result = ExecutionResult.succeeded(other)
    else:
        adapter.engine_result = ExecutionResult.failed(
            handle,
            Failure(FailureKind.DATA_ERROR, False, "bad data"),
        )

    result = await plane.wait(handle, poll_interval_seconds=0)

    assert result.status is ResultStatus.FAILED


@pytest.mark.parametrize("raises", [True, False])
async def test_bad_verifier_is_structured(raises: bool) -> None:
    plane = ControlPlane({"bigquery": StubAdapter()})
    handle = await submit(plane)

    result = await plane.wait(
        handle,
        verify=[BrokenVerifier(raises=raises)],
        poll_interval_seconds=0,
    )

    assert result.status is ResultStatus.VERIFICATION_FAILED


async def test_verifier_failure_without_checks_remains_a_failure() -> None:
    plane = ControlPlane({"bigquery": StubAdapter()})
    handle = await submit(plane)

    result = await plane.wait(handle, verify=[EmptyFailedVerifier()], poll_interval_seconds=0)

    assert result.status is ResultStatus.VERIFICATION_FAILED


async def test_public_api_uses_configured_control_plane() -> None:
    adapter = StubAdapter()
    configure(adapters={})
    public_register_adapter("bigquery", adapter)
    artifact, target, context, policy = request()
    handle = await public_submit(artifact, target=target, context=context, policy=policy)

    assert (await public_get(handle)).state is ExecutionState.SUCCEEDED
    assert (await public_wait(handle, poll_interval_seconds=0)).status is ResultStatus.ACCEPTED
    assert (await public_cancel(handle)).state is ExecutionState.CANCELLED

    adapter = StubAdapter()
    configure(adapters={"bigquery": adapter})
    assert (
        await public_run(
            artifact,
            target=target,
            context=context,
            policy=policy,
            poll_interval_seconds=0,
        )
    ).status is ResultStatus.ACCEPTED


def _admission(adapter: StubAdapter) -> AdmissionDecision:
    return admit(ValidationResult.accepted(), adapter.caps, PolicyRequirements())
