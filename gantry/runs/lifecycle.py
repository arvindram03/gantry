# SPDX-License-Identifier: Apache-2.0
"""Driving one run through its states, on behalf of a governed operation.

Providers execute work and report on it. They do not own run identity or run
persistence — this does, so that every operation records the same things in the
same order whatever engine is underneath.

The order is the point. The run is created before anything external happens, so
an engine job cannot exist without a control-plane record of why it was allowed
to. If that first write fails, nothing is submitted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime

from gantry.actor import ActorRef, current_actor
from gantry.evidence import EvidenceBundle
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.policy.decision import PolicyDecision
from gantry.policy.request import PolicyRequest
from gantry.result import ResultStatus
from gantry.runs.model import (
    AdmissionRecord,
    ExecutionRecord,
    OperationKind,
    OperationRef,
    ProposalRecord,
    ProposalStorage,
    QueryResultRef,
    ResourceRef,
    Run,
    new_run_id,
)
from gantry.runs.status import RunStatus
from gantry.runs.store import create as _create_run
from gantry.runs.store import update as _update_run
from gantry.verifier import VerificationResult


class RunRecorder:
    """The run for one governed operation, and the transitions it goes through.

    Each method persists. A caller that forgets to advance the run leaves it in
    its last recorded state, which is the honest outcome — a run stuck at
    `RUNNING` says the process died mid-flight, and saying so is better than a
    tidy record that was never true.
    """

    def __init__(
        self,
        *,
        kind: OperationKind,
        engine: str,
        provider: str | None = None,
        proposal: str | None = None,
        proposal_kind: str = "sql",
        storage: ProposalStorage = ProposalStorage.FULL,
        actor: ActorRef | None = None,
        agent_verification: Sequence[str] = (),
    ) -> None:
        self.run = Run(
            id=new_run_id(),
            status=RunStatus.PENDING,
            actor=actor or current_actor(),
            operation=OperationRef(kind=kind, engine=engine, provider=provider),
            proposal=None
            if proposal is None
            else ProposalRecord.of(
                proposal,
                kind=proposal_kind,
                storage=storage,
                agent_verification=tuple(agent_verification),
            ),
        )
        # Fail closed: if this raises, the caller must not submit.
        _create_run(self.run)

    def rejected(
        self,
        reasons: Sequence[str],
        *,
        status: RunStatus,
        verification: VerificationResult | None = None,
        evidence: EvidenceBundle | None = None,
        decision: PolicyDecision | None = None,
        request: PolicyRequest | None = None,
    ) -> Run:
        """Refused: by policy, by a contradictory contract, or by a check.

        The checks come along when there are any. A run refused *because* a
        check could not be evaluated has to record which one — otherwise the
        record says it was rejected and cannot say what for, which is the
        question anyone reading it later has.
        """
        return self._save(
            status,
            admission=admission_record(False, reasons=reasons, decision=decision, request=request),
            **({"verification": verification} if verification is not None else {}),
            **({"evidence": evidence} if evidence is not None else {}),
        )

    def policy_rejected(self, decision: PolicyDecision, request: PolicyRequest) -> Run:
        """Refused by policy. Nothing external happens after this.

        The one exit shared by every provider, so that a denial looks the same
        whichever engine was about to be asked — the structured decision on the
        run, a `POLICY_REJECTED` failure on the returned object, and no
        submission.
        """
        reasons = decision.messages() or ("the policy allows nothing for this request",)
        return self.rejected(
            reasons,
            status=RunStatus.POLICY_REJECTED,
            decision=decision,
            request=request,
        ).with_failure(Failure(FailureKind.POLICY_REJECTED, False, "; ".join(reasons)))

    def admitted(
        self,
        *,
        inputs: Sequence[ResourceRef] = (),
        decision: PolicyDecision | None = None,
        request: PolicyRequest | None = None,
    ) -> Run:
        """Authorized. Records which policy said so, not only that something did.

        An allow that keeps no trace is indistinguishable later from an
        operation nobody checked, so the policy name, its version and the rules
        that matched are written here even when the answer was yes.

        Inputs are recorded; outputs are not. A destination that was authorized
        is not a destination that exists, and `run.outputs` means what the run
        produced. What it intended to produce is in the admission request,
        where it cannot be mistaken for a table someone can go and read.
        """
        return self._save(
            self.run.status,
            admission=admission_record(True, decision=decision, request=request),
            inputs=tuple(inputs) or (() if request is None else tuple(request.inputs)),
        )

    def running(self, handle: ExecutionHandle | None) -> Run:
        """Record the engine's own id as soon as there is one to record."""
        return self._save(
            RunStatus.RUNNING,
            handle=handle,
            execution=ExecutionRecord(
                status="RUNNING",
                native_id=None if handle is None else handle.native_id,
                submitted_at=datetime.now(UTC),
                started_at=None if handle is None else handle.submitted_at,
            ),
        )

    def execution_failed(
        self, reason: str | None = None, *, metrics: Mapping[str, object] | None = None
    ) -> Run:
        """The engine could not do work Gantry had admitted.

        `metrics` carries whatever the engine said about the failure. Losing it
        means the record can say a job failed but not what the engine called
        it, which is the first thing anyone looks for.
        """
        return self._save(
            RunStatus.EXECUTION_FAILED,
            execution=replace(self._finished("FAILED"), metrics=dict(metrics or {})),
            admission=self.run.admission or AdmissionRecord(allowed=True),
            **({"failure": reason} if reason else {}),
        )

    def verifying(self) -> Run:
        return self._save(RunStatus.VERIFYING, execution=self._finished("SUCCEEDED"))

    def decided(
        self,
        *,
        verification: VerificationResult | None,
        evidence: EvidenceBundle | None = None,
        outputs: Sequence[ResourceRef] = (),
        result_ref: QueryResultRef | None = None,
        inline: object | None = None,
        metrics: Mapping[str, object] | None = None,
    ) -> Run:
        """The final transition, and the only one that can say ACCEPTED."""
        return self._save(
            _decide(verification),
            verification=verification,
            evidence=evidence,
            outputs=tuple(outputs),
            result_ref=result_ref,
            inline=inline,
            execution=replace(
                self.run.execution or self._finished("SUCCEEDED"),
                metrics=dict(metrics or (self.run.execution.metrics if self.run.execution else {})),
            ),
        )

    def _finished(self, status: str) -> ExecutionRecord:
        """Close the execution record — except for a stream, which has not closed.

        `ACCEPTED` on a stream means it reached the healthy state the contract
        required, not that it stopped. Marking its execution `SUCCEEDED` with a
        finish time would record the opposite of what is true: the job is still
        running, and its record should say so.
        """
        existing = self.run.execution
        streaming = self.run.operation.kind is OperationKind.STREAM
        return ExecutionRecord(
            status="RUNNING" if streaming else status,
            native_id=None if existing is None else existing.native_id,
            submitted_at=None if existing is None else existing.submitted_at,
            started_at=None if existing is None else existing.started_at,
            finished_at=None if streaming else datetime.now(UTC),
            metrics={} if existing is None else existing.metrics,
        )

    def _save(self, status: RunStatus, **changes: object) -> Run:
        self.run = self.run.advanced(status, **changes)
        _update_run(self.run)
        return self.run


def _decide(verification: VerificationResult | None) -> RunStatus:
    """Acceptance is execution *and* verification, with unsupported kept apart.

    A check nobody could evaluate is not a check that failed. Both refuse
    acceptance — an unmeasured bound is not a bound — but only one of them is
    about the data.
    """
    if verification is None or verification.ok:
        return RunStatus.ACCEPTED
    if verification.unsupported_checks:
        return RunStatus.VERIFICATION_UNSUPPORTED
    return RunStatus.REJECTED


def run_from_evidence(
    evidence: EvidenceBundle | None,
    *,
    kind: OperationKind,
    engine: str,
    provider: str | None = None,
    status: ResultStatus | None = None,
    verification: VerificationResult | None = None,
    handle: ExecutionHandle | None = None,
    inline: object | None = None,
    result_ref: QueryResultRef | None = None,
    metrics: Mapping[str, object] | None = None,
    failure: Failure | None = None,
    decision: PolicyDecision | None = None,
    request: PolicyRequest | None = None,
) -> Run | None:
    """Record a terminal run for an operation that reports once, at the end.

    The SQL paths drive a `RunRecorder` through the lifecycle. Flink and
    MongoDB report their outcome in one place, so their run is created and
    completed together. The record is the same shape either way — what differs
    is how many times it was written, which is a property of the operation
    rather than of the model.
    """
    if evidence is None:
        return None
    recorder = RunRecorder(
        kind=kind,
        engine=engine,
        provider=provider,
        proposal=None,
        actor=current_actor(),
    )
    recorder.run = replace(
        recorder.run,
        proposal=ProposalRecord(hash=evidence.proposal_hash or "", kind="sql")
        if evidence.proposal_hash
        else None,
        inputs=tuple(ResourceRef(system=engine, resource=name) for name in evidence.inputs),
    )
    if request is not None:
        # The policy decision that let this run happen. Recorded even though the
        # run is written once, because "allowed by nothing in particular" and
        # "allowed by data-agents v3" are different facts about the same run.
        recorder.admitted(request=request, decision=decision)
    if status is ResultStatus.VERIFICATION_UNSUPPORTED:
        return recorder.rejected(
            ("a required check could not be evaluated",),
            status=RunStatus.VERIFICATION_UNSUPPORTED,
            verification=verification,
            evidence=evidence,
        )
    if status is ResultStatus.VERIFICATION_CONFLICT:
        return recorder.rejected(
            ("the verification contract was contradictory",),
            status=RunStatus.VERIFICATION_CONFLICT,
            verification=verification,
            evidence=evidence,
        )
    if status is not None and status is not ResultStatus.ACCEPTED and verification is None:
        return recorder.execution_failed()
    recorder.running(handle)
    return recorder.decided(
        verification=verification,
        evidence=evidence,
        outputs=tuple(ResourceRef(system=engine, resource=name) for name in evidence.outputs),
        inline=inline,
        result_ref=result_ref,
        metrics=dict(metrics or {}),
    )


__all__ = ["RunRecorder", "run_from_evidence"]


def admission_record(
    allowed: bool,
    *,
    reasons: Sequence[str] = (),
    decision: PolicyDecision | None = None,
    request: PolicyRequest | None = None,
) -> AdmissionRecord:
    """One admission record, whether a reusable policy decided or not.

    Operations configured only with their own constraints produce a record with
    no policy name — which is the truth about them, and reads differently from
    a policy that allowed the work.
    """
    combined = tuple(dict.fromkeys((*reasons, *(decision.messages() if decision else ()))))
    return AdmissionRecord(
        allowed=allowed,
        reasons=combined,
        policy=None if decision is None else decision.policy,
        policy_hash=None if decision is None else decision.policy_version,
        matched_rules=() if decision is None else decision.matched_rules,
        codes=() if decision is None else decision.codes,
        request=None if request is None else request.as_dict(),
    )
