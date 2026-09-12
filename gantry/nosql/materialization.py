# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from fnmatch import fnmatchcase
from hashlib import sha256
from typing import NoReturn, Protocol, runtime_checkable

from gantry.context import Context
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.pipeline import CollectionRef, Pipeline, classify_pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.result import NoSQLResult
from gantry.nosql.verify import CollectionSnapshot, DocumentCheck
from gantry.output import OutputKind, OutputRef
from gantry.result import ResultStatus
from gantry.runs.model import Run
from gantry.runs.status import RunStatus
from gantry.runtime import SubmissionError
from gantry.tool import Tool
from gantry.verifier import CheckResult, CheckSource, VerificationResult
from gantry.verify import (
    VerificationConflict,
    VerificationInputError,
    VerificationUnsupported,
    check_config,
    check_type,
    detect_conflicts,
    parse_agent_checks,
    sourced_result,
    validate_agent_checks,
    verification_schema,
)

_PLAN_METADATA = "gantry.nosql.materialization.plan"
_DESTINATION_METADATA = "gantry.nosql.materialization.destination"
_OPERATION_METADATA = "gantry.nosql.materialization.operation"


class MaterializationOperation(StrEnum):
    OUT = "OUT"
    MERGE = "MERGE"


@dataclass(frozen=True, slots=True)
class MaterializationPlan:
    operation: MaterializationOperation
    source_collection: str
    sources: tuple[CollectionRef, ...]
    destination: CollectionRef


@dataclass(frozen=True, slots=True)
class MaterializationCapabilities:
    create_table_as: bool = False
    create_view_as: bool = False
    durable_jobs: bool = False
    cancel: bool = False
    estimate_bytes_scanned: bool = False
    destination_introspection: bool = False
    result_reference: bool = False


@runtime_checkable
class MaterializationAdapter(Protocol):
    async def inspect_collection(
        self,
        reference: CollectionRef,
        target: object,
        *,
        include_document_count: bool = False,
    ) -> CollectionSnapshot | None: ...


@dataclass(frozen=True, slots=True)
class MaterializationPolicy:
    sources: Collection[str]
    destinations: Collection[str]
    timeout_seconds: float = 300
    max_bytes_scanned: int | None = None
    max_cost_usd: float | None = None

    def __post_init__(self) -> None:
        for field_name in ("sources", "destinations"):
            values = getattr(self, field_name)
            if isinstance(values, str):
                raise TypeError(f"{field_name} must be a collection of names, not a string")
            if any(not isinstance(value, str) for value in values):
                raise TypeError(f"{field_name} must contain only strings")
            if any(not value.strip() for value in values):
                raise ValueError(f"{field_name} must not contain empty names")
            object.__setattr__(self, field_name, tuple(value.lower() for value in values))
        if not self.destinations:
            raise ValueError("destinations must not be empty")
        if isinstance(self.timeout_seconds, bool) or not isinstance(
            self.timeout_seconds, (int, float)
        ):
            raise TypeError("timeout must be numeric")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        if self.max_bytes_scanned is not None and (
            isinstance(self.max_bytes_scanned, bool) or not isinstance(self.max_bytes_scanned, int)
        ):
            raise TypeError("max bytes scanned must be an integer")
        if self.max_bytes_scanned is not None and self.max_bytes_scanned < 0:
            raise ValueError("max bytes scanned must not be negative")
        if self.max_cost_usd is not None and (
            isinstance(self.max_cost_usd, bool) or not isinstance(self.max_cost_usd, (int, float))
        ):
            raise TypeError("max cost must be numeric")
        if self.max_cost_usd is not None and self.max_cost_usd < 0:
            raise ValueError("max cost must not be negative")


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    status: ResultStatus
    handle: ExecutionHandle | None = None
    outputs: tuple[OutputRef, ...] = ()
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    verification: VerificationResult | None = None
    failure: Failure | None = None
    run: object | None = None
    evidence: EvidenceBundle | None = None

    @property
    def ok(self) -> bool:
        return self.status is ResultStatus.ACCEPTED

    @property
    def uri(self) -> str | None:
        return next(
            (output.uri for output in self.outputs if output.kind is not OutputKind.INLINE),
            None,
        )


class MaterializationError(Exception):
    def __init__(self, failure: Failure, status: ResultStatus = ResultStatus.REJECTED) -> None:
        super().__init__(failure.message)
        self.failure = failure
        self.status = status


class _MaterializerConnection(Protocol):
    def capabilities(self) -> NoSQLCapabilities: ...

    async def submit(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy | None = None,
        context: Context | None = None,
    ) -> ExecutionHandle: ...

    async def wait(
        self, handle: ExecutionHandle, *, poll_interval_seconds: float = 1.0
    ) -> NoSQLResult: ...

    async def status(self, handle: ExecutionHandle) -> object: ...

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> object: ...

    async def _inspect_collection(
        self, reference: CollectionRef, *, include_document_count: bool = False
    ) -> CollectionSnapshot | None: ...


@dataclass(slots=True)
class NoSQLMaterializer:
    name = "materialize_nosql"
    description = "Create a governed derived collection from approved MongoDB sources."
    _agent_capabilities = frozenset(
        {"destination_exists", "not_empty", "document_count", "required_fields"}
    )

    _connection: _MaterializerConnection
    _policy: MaterializationPolicy
    _verify: Sequence[DocumentCheck]
    _plans: dict[str, MaterializationPlan] = field(default_factory=dict, init=False)
    _agent_checks: dict[str, tuple[DocumentCheck, ...]] = field(default_factory=dict, init=False)
    _proposal_hashes: dict[str, str] = field(default_factory=dict, init=False)

    def __init__(
        self,
        connection: _MaterializerConnection,
        policy: MaterializationPolicy,
        verify: Sequence[DocumentCheck],
    ) -> None:
        for check in verify:
            if not isinstance(check, DocumentCheck):
                raise TypeError("verify checks must implement the DocumentCheck protocol")
        self._connection = connection
        self._policy = policy
        self._verify = tuple(verify)
        self._plans = {}
        self._agent_checks = {}
        self._proposal_hashes = {}

    @property
    def input_schema(self) -> Mapping[str, object]:
        return {
            "type": "object",
            "properties": {
                "collection": {"type": "string"},
                "pipeline": {"type": ["object", "array"]},
                "verify": verification_schema(self._agent_capabilities),
            },
            "required": ["collection", "pipeline"],
            "additionalProperties": False,
        }

    def tool(
        self,
        *,
        name: str = "materialize_nosql",
        description: str = "Create a governed derived collection from approved MongoDB sources.",
    ) -> Tool[Run]:
        return Tool(
            name=name,
            description=description,
            input_schema=self.input_schema,
            _handler=self._invoke_tool,
        )

    @property
    def capabilities(self) -> MaterializationCapabilities:
        capabilities = self._connection.capabilities()
        return MaterializationCapabilities(
            create_table_as=capabilities.out_merge_writes,
            durable_jobs=capabilities.reconnect,
            cancel=capabilities.cancellation,
            estimate_bytes_scanned=capabilities.bytes_scanned,
            destination_introspection=capabilities.destination_introspection,
            result_reference=capabilities.materialization_reference,
        )

    def inspect(self, collection: str, pipeline: Pipeline) -> MaterializationPlan:
        classification = classify_pipeline(collection, pipeline)
        if classification.write_destination is None or classification.write_stage is None:
            _reject(
                FailureKind.OPERATION_NOT_ALLOWED,
                "materialization pipeline must end in $out or $merge",
            )
        operation = (
            MaterializationOperation.OUT
            if classification.write_stage == "$out"
            else MaterializationOperation.MERGE
        )
        sources = tuple(
            c for c in classification.collections if c != classification.write_destination
        )
        return MaterializationPlan(operation, collection, sources, classification.write_destination)

    async def submit(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        verify: Sequence[DocumentCheck] = (),
    ) -> ExecutionHandle:
        agent_checks = validate_agent_checks(verify, capabilities=self._agent_capabilities)
        detect_conflicts(self._verify, agent_checks)
        try:
            plan = self.inspect(collection, pipeline)
            await self._admit(plan)
            context = Context(metadata={_PLAN_METADATA: plan})
            handle = await self._connection.submit(
                collection, pipeline, policy=self._nosql_policy(), context=context
            )
        except SubmissionError as error:
            failure = error.result.failure or Failure(
                FailureKind.SUBMISSION_ERROR,
                False,
                "materialization submission failed",
            )
            raise MaterializationError(
                _normalize_submission_failure(failure),
                error.result.status,
            ) from error
        digest = sha256(json.dumps(pipeline, sort_keys=True, default=str).encode()).hexdigest()
        handle = replace(
            handle,
            metadata={
                **handle.metadata,
                "gantry.proposal_hash": digest,
                "gantry.agent_verification": [check_config(check) for check in agent_checks],
            },
        )
        self._plans[handle.gantry_id] = plan
        self._proposal_hashes[handle.gantry_id] = digest
        self._agent_checks[handle.gantry_id] = agent_checks  # type: ignore[assignment]
        return handle

    async def wait(self, handle: ExecutionHandle, *, poll_interval_seconds: float = 1.0) -> Run:
        plan = self._plans.get(handle.gantry_id) or _plan_from_handle(handle)
        nosql_result = await self._connection.wait(
            handle, poll_interval_seconds=poll_interval_seconds
        )
        agent_checks = self._agent_checks_for(handle)
        if nosql_result.status is not ResultStatus.ACCEPTED or plan is None:
            result = MaterializationResult(
                nosql_result.status,
                handle=nosql_result.handle,
                outputs=nosql_result.outputs,
                metrics=nosql_result.metrics,
                verification=nosql_result.verification,
                failure=nosql_result.failure,
                evidence=(
                    None
                    if plan is None
                    else _evidence(
                        plan,
                        handle,
                        nosql_result.outputs,
                        nosql_result.metrics,
                        nosql_result.verification,
                        nosql_result.status,
                        self._proposal_hash(handle),
                        agent_checks,
                    )
                ),
            )
            return _recorded(result)
        verification = await self._verification(plan.destination, agent_checks)
        unsupported = verification.unsupported_checks
        agent_unsupported = any(check.source == CheckSource.AGENT for check in unsupported)
        status = (
            ResultStatus.ACCEPTED
            if verification.ok
            else ResultStatus.VERIFICATION_UNSUPPORTED
            if agent_unsupported
            else ResultStatus.VERIFICATION_FAILED
        )
        output = _materialized_output(nosql_result.outputs, plan.destination)
        reasons = unsupported or verification.failed_checks
        failure = None
        if not verification.ok:
            message = next(
                (check.message for check in reasons if check.message),
                "MongoDB materialization verification failed",
            )
            failure = Failure(
                FailureKind.VERIFICATION_UNSUPPORTED
                if agent_unsupported
                else FailureKind.UNSUPPORTED_VERIFICATION
                if unsupported
                else FailureKind.VERIFICATION_FAILED,
                False,
                message,
            )
        outputs = (output,) if output is not None else nosql_result.outputs
        result = MaterializationResult(
            status,
            handle=nosql_result.handle,
            outputs=outputs,
            metrics=nosql_result.metrics,
            verification=verification,
            failure=failure,
            evidence=_evidence(
                plan,
                handle,
                outputs,
                nosql_result.metrics,
                verification,
                status,
                self._proposal_hash(handle),
                agent_checks,
            ),
        )
        return _recorded(result)

    async def status(self, handle: ExecutionHandle) -> object:
        return await self._connection.status(handle)

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> object:
        return await self._connection.cancel(handle, mode=mode)

    async def __call__(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        verify: Sequence[DocumentCheck] = (),
    ) -> Run:
        try:
            handle = await self.submit(collection, pipeline, verify=verify)
        except VerificationUnsupported as error:
            return _refuse(error, RunStatus.VERIFICATION_UNSUPPORTED)
        except VerificationConflict as error:
            return _refuse(error, RunStatus.VERIFICATION_CONFLICT)
        except (VerificationInputError, TypeError, ValueError) as error:
            return _refuse(error, RunStatus.POLICY_REJECTED)
        except MaterializationError as error:
            return _refuse(error.failure.message, RunStatus.POLICY_REJECTED, error.failure)
        return await self.wait(handle, poll_interval_seconds=0.05)

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> Run:
        unexpected = set(arguments) - {"collection", "pipeline", "verify"}
        if unexpected:
            names = ", ".join(sorted(unexpected))
            raise ValueError(f"unexpected materialize tool arguments: {names}")
        collection = arguments.get("collection")
        if not isinstance(collection, str):
            raise TypeError("collection must be a string")
        pipeline = arguments.get("pipeline")
        if not (
            isinstance(pipeline, (Mapping, Sequence)) and not isinstance(pipeline, (str, bytes))
        ):
            raise TypeError("pipeline must be a mapping or a sequence of stage mappings")
        try:
            checks = parse_agent_checks(
                arguments.get("verify"), capabilities=self._agent_capabilities
            )
        except VerificationUnsupported as error:
            return _refuse(error, RunStatus.VERIFICATION_UNSUPPORTED)
        except VerificationInputError as error:
            return _refuse(error, RunStatus.POLICY_REJECTED)
        return await self(collection, pipeline, verify=checks)  # type: ignore[arg-type]

    async def _admit(self, plan: MaterializationPlan) -> None:
        capabilities = self._connection.capabilities()
        if not capabilities.out_merge_writes:
            _reject(
                FailureKind.OPERATION_NOT_ALLOWED,
                "adapter does not support $out/$merge materialization",
            )
        if not capabilities.write_execution:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot enforce governed write execution",
            )
        if not capabilities.materialization_reference:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot return a materialized output reference",
            )
        if not capabilities.destination_introspection:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot inspect materialization destinations",
            )
        if self._policy.max_bytes_scanned is not None and not capabilities.bytes_scanned:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot enforce maximum bytes scanned",
            )
        if self._policy.max_cost_usd is not None and not capabilities.cost_limit:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT, "adapter cannot enforce maximum cost"
            )
        disallowed = [
            source for source in plan.sources if not _matches(source, self._policy.sources)
        ]
        if disallowed:
            names = ", ".join(source.name for source in disallowed)
            _reject(FailureKind.SOURCE_NOT_ALLOWED, f"source is not allowed: {names}")
        if not _matches(CollectionRef(plan.source_collection), self._policy.sources):
            _reject(
                FailureKind.SOURCE_NOT_ALLOWED,
                f"source is not allowed: {plan.source_collection}",
            )
        if not _matches(plan.destination, self._policy.destinations):
            _reject(
                FailureKind.DESTINATION_NOT_ALLOWED,
                f"destination is not allowed: {plan.destination.name}",
            )
        if plan.operation is MaterializationOperation.OUT:
            try:
                existing = await self._connection._inspect_collection(plan.destination)
            except NotImplementedError as error:
                _reject(
                    FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                    f"adapter cannot inspect materialization destinations: {error}",
                )
            except Exception as error:
                _reject(
                    FailureKind.VALIDATION_ERROR,
                    f"destination inspection raised {type(error).__name__}: {error}",
                )
            if existing is not None:
                _reject(
                    FailureKind.DESTINATION_EXISTS,
                    f"destination already exists: {plan.destination.name}",
                )

    async def _verification(
        self,
        destination: CollectionRef,
        agent_checks: Sequence[DocumentCheck] = (),
    ) -> VerificationResult:
        effective = (*self._verify, *agent_checks)
        include_document_count = any(
            getattr(check, "requires_document_count", False) for check in effective
        )
        try:
            snapshot = await self._connection._inspect_collection(
                destination, include_document_count=include_document_count
            )
        except Exception as error:
            message = f"destination inspection raised {type(error).__name__}: {error}"
            effective_results: tuple[CheckResult, ...]
            if not effective:
                effective_results = (
                    sourced_result(
                        CheckResult(
                            "destination_introspection",
                            False,
                            message=message,
                            supported=False,
                        ),
                        CheckSource.TRUSTED,
                        observation_source="destination",
                    ),
                )
            else:
                effective_results = tuple(
                    sourced_result(
                        CheckResult(
                            check_type(check) or type(check).__name__,
                            False,
                            message=message,
                            supported=False,
                        ),
                        CheckSource.TRUSTED if index < len(self._verify) else CheckSource.AGENT,
                        observation_source="destination",
                    )
                    for index, check in enumerate(effective)
                )
            return VerificationResult(ok=False, checks=effective_results)
        results: list[CheckResult] = []
        for index, check in enumerate(effective):
            source = CheckSource.TRUSTED if index < len(self._verify) else CheckSource.AGENT
            try:
                result = check.evaluate(snapshot)
            except Exception as error:
                result = CheckResult(
                    check_type(check) or type(check).__name__,
                    False,
                    message=f"verification raised {type(error).__name__}: {error}",
                )
            results.append(sourced_result(result, source, observation_source="destination"))
        return VerificationResult(ok=all(check.ok for check in results), checks=tuple(results))

    def _agent_checks_for(self, handle: ExecutionHandle) -> tuple[DocumentCheck, ...]:
        local = self._agent_checks.get(handle.gantry_id)
        if local is not None:
            return local
        try:
            checks = parse_agent_checks(
                handle.metadata.get("gantry.agent_verification"),
                capabilities=self._agent_capabilities,
            )
        except VerificationInputError:
            return ()
        return checks  # type: ignore[return-value]

    def _proposal_hash(self, handle: ExecutionHandle) -> str | None:
        stored = handle.metadata.get("gantry.proposal_hash")
        return self._proposal_hashes.get(handle.gantry_id) or (
            stored if isinstance(stored, str) else None
        )

    def _nosql_policy(self) -> NoSQLPolicy:
        return NoSQLPolicy(
            read_only=False,
            max_documents=1,
            timeout_seconds=self._policy.timeout_seconds,
            max_bytes_scanned=self._policy.max_bytes_scanned,
            max_cost_usd=self._policy.max_cost_usd,
        )


def _matches(reference: CollectionRef, patterns: Collection[str]) -> bool:
    name = reference.name.lower()
    return any(fnmatchcase(name, pattern) for pattern in patterns)


def _materialized_output(
    outputs: Sequence[OutputRef], destination: CollectionRef
) -> OutputRef | None:
    slug = destination.name.lower().replace(".", "/")
    for output in outputs:
        if output.kind is not OutputKind.INLINE and slug in output.uri.replace(".", "/"):
            return output
    return next((output for output in outputs if output.kind is not OutputKind.INLINE), None)


def _plan_from_handle(handle: ExecutionHandle) -> MaterializationPlan | None:
    destination = handle.metadata.get(_DESTINATION_METADATA)
    operation = handle.metadata.get(_OPERATION_METADATA)
    if not isinstance(destination, str) or not isinstance(operation, str):
        return None
    return MaterializationPlan(
        MaterializationOperation(operation), destination, (), CollectionRef(destination)
    )


def _normalize_submission_failure(failure: Failure) -> Failure:
    lowered = failure.message.lower()
    if "bytes scanned" in lowered or "cost" in lowered:
        return Failure(FailureKind.COST_LIMIT_EXCEEDED, failure.retryable, failure.message)
    return failure


def _evidence(
    plan: MaterializationPlan,
    handle: ExecutionHandle,
    outputs: Sequence[OutputRef],
    metrics: ExecutionMetrics,
    verification: VerificationResult | None,
    decision: ResultStatus,
    proposal_hash: str | None,
    agent_checks: Sequence[DocumentCheck],
) -> EvidenceBundle:
    observations: list[Observation] = []
    for name, value, unit in (
        ("rows_read", metrics.rows_read, "documents"),
        ("rows_written", metrics.rows_written, "documents"),
        ("bytes_read", metrics.bytes_read, "bytes"),
        ("runtime_seconds", metrics.runtime_seconds, "seconds"),
    ):
        if value is not None:
            observations.append(Observation(name, value, ObservationSource.ENGINE, unit=unit))
    for check in verification.checks if verification is not None else ():
        if check.actual is not None:
            observations.append(Observation(check.name, check.actual, ObservationSource.OUTPUT))
    return EvidenceBundle(
        run_id=handle.gantry_id,
        engine=handle.engine,
        operation=plan.operation.value,
        decision=decision.value,
        native_execution_id=handle.native_id,
        proposal_hash=proposal_hash,
        inputs=tuple(source.name for source in plan.sources),
        outputs=tuple(output.uri for output in outputs),
        started_at=handle.submitted_at,
        execution={"status": decision.value, "engine": handle.engine},
        proposal={
            "hash": proposal_hash,
            "agent_verification": [check_config(check) for check in agent_checks],
        },
        observations=tuple(observations),
        checks=() if verification is None else verification.checks,
    )


def _refuse(error: Exception | str, status: RunStatus, failure: Failure | None = None) -> Run:
    """Record a run for a proposal refused before it reached MongoDB."""
    from gantry.runs.lifecycle import RunRecorder
    from gantry.runs.model import OperationKind

    recorder = RunRecorder(kind=OperationKind.MATERIALIZE, engine="mongodb", provider="mongodb")
    kind = {
        RunStatus.VERIFICATION_UNSUPPORTED: FailureKind.VERIFICATION_UNSUPPORTED,
        RunStatus.VERIFICATION_CONFLICT: FailureKind.VERIFICATION_CONFLICT,
    }.get(status, FailureKind.VALIDATION_ERROR)
    return recorder.rejected((str(error),), status=status).with_failure(
        failure or Failure(kind, False, str(error))
    )


def _recorded(result: MaterializationResult) -> Run:
    """Record the run for a MongoDB materialization and return it."""
    from gantry.runs.lifecycle import RunRecorder, run_from_evidence
    from gantry.runs.model import OperationKind

    run = run_from_evidence(
        result.evidence,
        kind=OperationKind.MATERIALIZE,
        engine="mongodb",
        provider="mongodb",
        status=result.status,
        verification=result.verification,
        handle=result.handle,
        failure=result.failure,
    )
    if run is None:
        recorder = RunRecorder(kind=OperationKind.MATERIALIZE, engine="mongodb", provider="mongodb")
        run = recorder.rejected(
            (result.failure.message if result.failure else "refused",),
            status=RunStatus.POLICY_REJECTED,
            verification=result.verification,
        )
    return run.with_failure(result.failure)


def _failed(status: ResultStatus, failure: Failure) -> MaterializationResult:
    return MaterializationResult(status, failure=_normalize_submission_failure(failure))


def _reject(kind: FailureKind, message: str) -> NoReturn:
    raise MaterializationError(Failure(kind, False, message))
