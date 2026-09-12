# SPDX-License-Identifier: Apache-2.0
"""Small public surface for governed NoSQL connections."""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from hashlib import sha256

from gantry.artifact import Artifact
from gantry.context import Context
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.execution import Execution
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.nosql.adapter import NoSQLAdapter
from gantry.nosql.bridge import NoSQLExecutionAdapter
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.inspection import query_request
from gantry.nosql.materialization import (
    MaterializationAdapter,
    MaterializationPolicy,
    NoSQLMaterializer,
)
from gantry.nosql.output import InlineDocuments
from gantry.nosql.pipeline import CollectionRef, Pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.query import NoSQLQuery
from gantry.nosql.registry import resolve_provider
from gantry.nosql.result import NoSQLResult
from gantry.nosql.target import NoSQLTarget
from gantry.nosql.verify import CollectionSnapshot, DocumentCheck
from gantry.output import OutputKind, OutputRef
from gantry.policy.evaluator import evaluate
from gantry.policy.model import Policy
from gantry.result import Result, ResultStatus
from gantry.runs.model import Run
from gantry.runs.status import RunStatus
from gantry.runtime import ControlPlane
from gantry.target import ExecutionTarget
from gantry.verifier import CheckResult, CheckSource, VerificationResult, Verifier
from gantry.verify import check_config, check_type, sourced_result


class NoSQLConnection:
    """A provider-neutral, governed MongoDB connection.

    Configuration and the native adapter remain private so an agent tool
    cannot accidentally expose credentials or a raw database handle.
    """

    def __init__(
        self, target: NoSQLTarget, adapter: NoSQLAdapter, policy: Policy | None = None
    ) -> None:
        self._target = target
        self._adapter = adapter
        self._policy = policy
        self._plane = ControlPlane()

    @property
    def provider(self) -> str:
        return self._target.provider

    @property
    def policy(self) -> Policy | None:
        """The reusable policy this connection admits against, if any."""
        return self._policy

    @property
    def database(self) -> str | None:
        """The database every collection on this connection belongs to.

        Policy names a MongoDB resource `database.collection`, so the half the
        connection knows has to come from here rather than from the proposal.
        """
        database = self._target.config.get("database")
        return database if isinstance(database, str) and database.strip() else None

    def capabilities(self) -> NoSQLCapabilities:
        return self._adapter.capabilities()

    def query(
        self,
        *,
        read_only: bool = True,
        collections: Collection[str] = (),
        denied_collections: Collection[str] = (),
        max_documents: int = 1_000,
        timeout: float = 30,
        max_bytes_scanned: int | None = None,
        max_cost_usd: float | None = None,
        checks: Sequence[Verifier | DocumentCheck] = (),
        verify: Sequence[Verifier | DocumentCheck] | None = None,
    ) -> NoSQLQuery:
        """Configure a governed query operation."""

        policy = NoSQLPolicy(
            read_only=read_only,
            allowed_collections=collections,
            denied_collections=denied_collections,
            max_documents=max_documents,
            timeout_seconds=timeout,
            max_bytes_scanned=max_bytes_scanned,
            max_cost_usd=max_cost_usd,
        )
        if verify is not None and checks:
            raise TypeError("pass trusted checks with checks= (verify= is a compatibility alias)")
        trusted = checks if verify is None else verify
        return NoSQLQuery(self, policy, tuple(trusted))

    def materialize(
        self,
        *,
        sources: Collection[str] = (),
        destinations: Collection[str],
        timeout: float = 300,
        max_bytes_scanned: int | None = None,
        max_cost_usd: float | None = None,
        checks: Sequence[DocumentCheck] = (),
        verify: Sequence[DocumentCheck] | None = None,
    ) -> NoSQLMaterializer:
        """Configure a governed, create-only MongoDB materialization operation."""

        policy = MaterializationPolicy(
            sources=sources,
            destinations=destinations,
            timeout_seconds=timeout,
            max_bytes_scanned=max_bytes_scanned,
            max_cost_usd=max_cost_usd,
        )
        if verify is not None and checks:
            raise TypeError("pass trusted checks with checks= (verify= is a compatibility alias)")
        trusted = checks if verify is None else verify
        return NoSQLMaterializer(self, policy, tuple(trusted))

    async def _query(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy,
        context: Context | None = None,
        trusted_verify: Sequence[Verifier | DocumentCheck] = (),
        agent_verify: Sequence[DocumentCheck] = (),
    ) -> Run:
        """Execute a pipeline for a configured query operation, and record the run.

        Returns the `Run`, as the SQL paths do. The documents are on
        `run.documents`, and are dropped on the way to storage."""

        verifiers = tuple(check for check in trusted_verify if isinstance(check, Verifier))
        trusted_checks = tuple(check for check in trusted_verify if not isinstance(check, Verifier))

        # Authority, before the pipeline reaches the driver. `$lookup` and
        # `$out` are resolved out of the pipeline, so a stage that reads or
        # writes somewhere unauthorized is refused rather than executed.
        request = query_request(
            collection,
            pipeline,
            provider=self.provider,
            database=self.database,
            policy=policy,
        )
        decision = None
        if self._policy is not None:
            decision = evaluate(self._policy, request)
            if not decision.allowed:
                from gantry.runs.lifecycle import RunRecorder
                from gantry.runs.model import OperationKind

                recorder = RunRecorder(
                    kind=OperationKind.QUERY, engine="mongodb", provider=self.provider
                )
                return recorder.policy_rejected(decision, request)

        raw = await self._execute_result(
            collection, pipeline, policy=policy, context=context, verify=verifiers
        )
        result = _from_engine_result(raw, policy.max_documents)
        base = _source_existing(result.verification, CheckSource.TRUSTED)
        extra = (
            ()
            if raw.status is not ResultStatus.ACCEPTED
            else (
                *_document_checks(trusted_checks, result.inline, CheckSource.TRUSTED),
                *_document_checks(agent_verify, result.inline, CheckSource.AGENT),
            )
        )
        verification = _merge(base, extra)
        status, failure = _decide(raw, verification)
        response = replace(
            result,
            status=status,
            verification=verification,
            failure=failure,
            evidence=_query_evidence(
                collection, pipeline, raw, result.inline, verification, status, agent_verify
            ),
        )
        from gantry.runs.lifecycle import run_from_evidence
        from gantry.runs.model import OperationKind, QueryResultRef

        run = run_from_evidence(
            response.evidence,
            kind=OperationKind.QUERY,
            engine="mongodb",
            provider=self.provider,
            status=response.status,
            verification=response.verification,
            handle=response.handle,
            inline=response.inline,
            decision=decision,
            request=request,
            result_ref=None
            if response.inline is None
            else QueryResultRef(
                rows=len(response.inline.documents),
                inline=True,
                truncated=response.inline.truncated,
            ),
            failure=response.failure,
        )
        if run is None:
            from gantry.runs.lifecycle import RunRecorder

            run = RunRecorder(
                kind=OperationKind.QUERY, engine="mongodb", provider=self.provider
            ).rejected(
                (response.failure.message if response.failure else "refused",),
                status=RunStatus.POLICY_REJECTED,
                verification=response.verification,
            )
        return run.with_failure(response.failure)

    async def submit(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy | None = None,
        context: Context | None = None,
    ) -> ExecutionHandle:
        active_policy = policy or NoSQLPolicy()
        bridge = self._install_bridge(active_policy)
        nosql_context = _with_collection(context, collection)
        return await self._plane.submit(
            Artifact(pipeline, "nosql"),
            target=self._execution_target(),
            context=nosql_context,
            policy=bridge.adapter.capabilities().policy_requirements(active_policy),
        )

    async def execute(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy | None = None,
        context: Context | None = None,
        verify: Sequence[Verifier] = (),
        poll_interval_seconds: float = 0.05,
    ) -> NoSQLResult:
        active_policy = policy or NoSQLPolicy()
        result = await self._execute_result(
            collection,
            pipeline,
            policy=active_policy,
            context=context,
            verify=verify,
            poll_interval_seconds=poll_interval_seconds,
        )
        return _from_engine_result(result, active_policy.max_documents)

    async def _execute_result(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy,
        context: Context | None = None,
        verify: Sequence[Verifier] = (),
        poll_interval_seconds: float = 0.05,
    ) -> Result:
        active_policy = policy
        bridge = self._install_bridge(active_policy)
        nosql_context = _with_collection(context, collection)
        return await self._plane.run(
            Artifact(pipeline, "nosql"),
            target=self._execution_target(),
            context=nosql_context,
            policy=bridge.adapter.capabilities().policy_requirements(active_policy),
            verify=verify,
            poll_interval_seconds=poll_interval_seconds,
        )

    async def status(self, handle: ExecutionHandle) -> Execution:
        return await self._adapter.status(handle)

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> Execution:
        return await self._adapter.cancel(handle, mode)

    async def wait(
        self,
        handle: ExecutionHandle,
        *,
        poll_interval_seconds: float = 0.05,
    ) -> NoSQLResult:
        """Observe a submitted job and recover its result."""

        result = await self._plane.wait(handle, poll_interval_seconds=poll_interval_seconds)
        max_documents = handle.metadata.get("max_documents", 1_000)
        return _from_engine_result(
            result, max_documents if isinstance(max_documents, int) else 1_000
        )

    async def _inspect_collection(
        self,
        reference: CollectionRef,
        *,
        include_document_count: bool = False,
    ) -> CollectionSnapshot | None:
        if not isinstance(self._adapter, MaterializationAdapter):
            raise NotImplementedError(f"{self.provider} does not support destination inspection")
        return await self._adapter.inspect_collection(
            reference, self._target, include_document_count=include_document_count
        )

    def _bridge(self, policy: NoSQLPolicy) -> NoSQLExecutionAdapter:
        return NoSQLExecutionAdapter(self._adapter, self._target, policy)

    def _install_bridge(self, policy: NoSQLPolicy) -> NoSQLExecutionAdapter:
        bridge = self._bridge(policy)
        self._plane.register_adapter(self.provider, bridge)
        return bridge

    def _execution_target(self) -> ExecutionTarget:
        # Deliberately exclude provider configuration and credentials.
        return ExecutionTarget(self.provider, {})


def _with_collection(context: Context | None, collection: str) -> Context:
    base = context or Context()
    return Context(
        resources=base.resources,
        metadata={**base.metadata, "gantry.nosql.collection": collection},
    )


def _find_inline(outputs: Sequence[OutputRef], max_documents: int) -> InlineDocuments | None:
    for output in outputs:
        if output.kind is OutputKind.INLINE:
            inline = output.metadata.get("inline")
            if isinstance(inline, InlineDocuments):
                documents = inline.documents[:max_documents]
                return InlineDocuments(
                    documents,
                    truncated=inline.truncated or len(inline.documents) > max_documents,
                )
    return None


def _from_engine_result(result: Result, max_documents: int) -> NoSQLResult:
    return NoSQLResult(
        status=result.status,
        handle=result.handle,
        inline=_find_inline(result.outputs, max_documents),
        outputs=result.outputs,
        metrics=result.metrics,
        verification=result.verification,
        failure=result.failure,
    )


def _document_checks(
    checks: Sequence[DocumentCheck],
    inline: InlineDocuments | None,
    source: CheckSource,
) -> tuple[CheckResult, ...]:
    fields: set[str] = set()
    if inline is not None:
        for document in inline.documents:
            if isinstance(document, Mapping):
                fields.update(str(key) for key in document)
    snapshot = (
        None
        if inline is None
        else CollectionSnapshot(
            "(result set)",
            metadata={"document_count": len(inline.documents)},
            fields=tuple(sorted(fields)),
        )
    )
    results: list[CheckResult] = []
    for check in checks:
        if getattr(check, "requires_destination", False):
            result = CheckResult(
                name=check_type(check) or type(check).__name__,
                ok=False,
                message="destination verification is unavailable for a query result",
                supported=False,
            )
        elif (
            inline is not None
            and inline.truncated
            and getattr(check, "requires_document_count", False)
            and not getattr(check, "allows_truncated", False)
        ):
            result = CheckResult(
                name=check_type(check) or type(check).__name__,
                ok=False,
                message=(
                    "the result was truncated by max_documents, so its document count "
                    "does not describe the complete result"
                ),
                supported=False,
            )
        else:
            try:
                result = check.evaluate(snapshot)
            except Exception as error:
                result = CheckResult(
                    name=check_type(check) or type(check).__name__,
                    ok=False,
                    message=f"verification raised {type(error).__name__}: {error}",
                )
        results.append(sourced_result(result, source, observation_source="result set"))
    return tuple(results)


def _source_existing(
    verification: VerificationResult | None, source: CheckSource
) -> VerificationResult | None:
    if verification is None:
        return None
    checks = tuple(
        sourced_result(check, source, observation_source="verifier")
        for check in verification.checks
    )
    return replace(verification, checks=checks)


def _merge(
    existing: VerificationResult | None, extra: tuple[CheckResult, ...]
) -> VerificationResult | None:
    if existing is None and not extra:
        return None
    checks = (() if existing is None else existing.checks) + extra
    return VerificationResult(ok=all(check.ok for check in checks), checks=checks)


def _decide(
    result: Result, verification: VerificationResult | None
) -> tuple[ResultStatus, Failure | None]:
    if result.status is not ResultStatus.ACCEPTED:
        return result.status, result.failure
    if verification is None or verification.ok:
        return result.status, result.failure
    unsupported = verification.unsupported_checks
    reasons = unsupported or verification.failed_checks
    message = next((check.message for check in reasons if check.message), "verification failed")
    agent_unsupported = any(check.source == CheckSource.AGENT for check in unsupported)
    return (
        ResultStatus.VERIFICATION_UNSUPPORTED
        if agent_unsupported
        else ResultStatus.VERIFICATION_FAILED
    ), Failure(
        FailureKind.VERIFICATION_UNSUPPORTED
        if agent_unsupported
        else FailureKind.UNSUPPORTED_VERIFICATION
        if unsupported
        else FailureKind.VERIFICATION_FAILED,
        False,
        message,
    )


def _query_evidence(
    collection: str,
    pipeline: Pipeline,
    result: Result,
    inline: InlineDocuments | None,
    verification: VerificationResult | None,
    decision: ResultStatus,
    agent_verify: Sequence[DocumentCheck],
) -> EvidenceBundle | None:
    handle = result.handle
    if handle is None:
        return None
    digest = sha256(json.dumps(pipeline, sort_keys=True, default=str).encode()).hexdigest()
    observations: list[Observation] = []
    if result.execution is not None:
        observations.append(
            Observation("execution_state", result.execution.state.value, ObservationSource.ENGINE)
        )
    if inline is not None:
        observations.extend(
            (
                Observation(
                    "documents_returned",
                    len(inline.documents),
                    ObservationSource.OUTPUT,
                    unit="documents",
                ),
                Observation("truncated", inline.truncated, ObservationSource.OUTPUT),
            )
        )
    for check in verification.checks if verification is not None else ():
        if check.actual is not None:
            observations.append(Observation(check.name, check.actual, ObservationSource.OUTPUT))
    return EvidenceBundle(
        run_id=handle.gantry_id,
        engine=handle.engine,
        operation="query",
        decision=decision.value,
        native_execution_id=handle.native_id,
        proposal_hash=digest,
        inputs=(collection,),
        outputs=tuple(output.uri for output in result.outputs),
        started_at=(
            None
            if result.execution is None
            else (result.execution.started_at or handle.submitted_at)
        ),
        finished_at=None if result.execution is None else result.execution.updated_at,
        execution={
            "status": "UNKNOWN" if result.execution is None else result.execution.state.value,
            "engine": handle.engine,
        },
        proposal={
            "hash": digest,
            "agent_verification": [check_config(check) for check in agent_verify],
        },
        observations=tuple(observations),
        checks=() if verification is None else verification.checks,
    )


def connect(provider: str, *, policy: Policy | None = None, **config: object) -> NoSQLConnection:
    """Resolve a provider preset and create a governed NoSQL connection.

    `policy` attaches a reusable policy to everything this connection runs, on
    top of the per-operation constraints.
    """

    if policy is not None and not isinstance(policy, Policy):
        raise TypeError("policy must be a gantry.Policy")
    entry = resolve_provider(provider)
    entry.validate_config(config)
    target = NoSQLTarget(provider, entry.driver, config)
    return NoSQLConnection(target, entry.adapter_factory(target), policy=policy)
