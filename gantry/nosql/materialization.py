# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from fnmatch import fnmatchcase
from typing import NoReturn, Protocol, runtime_checkable

from gantry.context import Context
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
from gantry.runtime import SubmissionError
from gantry.tool import Tool
from gantry.verifier import VerificationResult

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

    _connection: _MaterializerConnection
    _policy: MaterializationPolicy
    _verify: Sequence[DocumentCheck]
    _plans: dict[str, MaterializationPlan] = field(default_factory=dict, init=False)

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

    @property
    def input_schema(self) -> Mapping[str, object]:
        return {
            "type": "object",
            "properties": {
                "collection": {"type": "string"},
                "pipeline": {"type": ["object", "array"]},
            },
            "required": ["collection", "pipeline"],
            "additionalProperties": False,
        }

    def tool(
        self,
        *,
        name: str = "materialize_nosql",
        description: str = "Create a governed derived collection from approved MongoDB sources.",
    ) -> Tool[MaterializationResult]:
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

    async def submit(self, collection: str, pipeline: Pipeline) -> ExecutionHandle:
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
        self._plans[handle.gantry_id] = plan
        return handle

    async def wait(
        self, handle: ExecutionHandle, *, poll_interval_seconds: float = 1.0
    ) -> MaterializationResult:
        plan = self._plans.get(handle.gantry_id) or _plan_from_handle(handle)
        nosql_result = await self._connection.wait(
            handle, poll_interval_seconds=poll_interval_seconds
        )
        if nosql_result.status is not ResultStatus.ACCEPTED or plan is None:
            return MaterializationResult(
                nosql_result.status,
                handle=nosql_result.handle,
                outputs=nosql_result.outputs,
                metrics=nosql_result.metrics,
                verification=nosql_result.verification,
                failure=nosql_result.failure,
            )
        verification = await self._verification(plan.destination)
        status = ResultStatus.ACCEPTED if verification.ok else ResultStatus.VERIFICATION_FAILED
        output = _materialized_output(nosql_result.outputs, plan.destination)
        return MaterializationResult(
            status,
            handle=nosql_result.handle,
            outputs=(output,) if output is not None else nosql_result.outputs,
            metrics=nosql_result.metrics,
            verification=verification,
        )

    async def status(self, handle: ExecutionHandle) -> object:
        return await self._connection.status(handle)

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> object:
        return await self._connection.cancel(handle, mode=mode)

    async def __call__(self, collection: str, pipeline: Pipeline) -> MaterializationResult:
        try:
            handle = await self.submit(collection, pipeline)
        except MaterializationError as error:
            return _failed(error.status, error.failure)
        except (TypeError, ValueError) as error:
            return _failed(
                ResultStatus.REJECTED, Failure(FailureKind.VALIDATION_ERROR, False, str(error))
            )
        return await self.wait(handle, poll_interval_seconds=0.05)

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> MaterializationResult:
        unexpected = set(arguments) - {"collection", "pipeline"}
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
        return await self(collection, pipeline)

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

    async def _verification(self, destination: CollectionRef) -> VerificationResult:
        include_document_count = any(
            getattr(check, "requires_document_count", False) for check in self._verify
        )
        snapshot = await self._connection._inspect_collection(
            destination, include_document_count=include_document_count
        )
        checks = tuple(check.evaluate(snapshot) for check in self._verify)
        return VerificationResult(ok=all(check.ok for check in checks), checks=checks)

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


def _failed(status: ResultStatus, failure: Failure) -> MaterializationResult:
    return MaterializationResult(status, failure=_normalize_submission_failure(failure))


def _reject(kind: FailureKind, message: str) -> NoReturn:
    raise MaterializationError(Failure(kind, False, message))
