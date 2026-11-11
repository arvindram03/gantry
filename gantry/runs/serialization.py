# SPDX-License-Identifier: Apache-2.0
"""Rebuilding a run from its stored form.

`Run.to_json` has always been public; this is the way back, and it lives here
rather than inside the SQLite store because it is not about SQLite. Anyone
implementing the `RunStore` protocol over their own database needs it, and
asking them to import from a module named for a different backend was telling
them the library did not expect anyone else to store a run.

Round-tripping is the property the whole record rests on: a run nobody can read
back is a log line.
"""

from __future__ import annotations

from datetime import datetime

from gantry.actor import ActorRef, ActorType
from gantry.confirmation.model import ConfirmationReason, ConfirmationRecord
from gantry.confirmation.status import ConfirmationReasonCode, ConfirmationStatus
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.failure import Failure, FailureKind
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
)
from gantry.runs.status import RunStatus
from gantry.verifier import CheckResult, CheckSource, VerificationResult


def run_from_dict(payload: dict[str, object]) -> Run:
    """Rebuild a run from its stored form.

    Round-tripping is the property the whole feature rests on: a record nobody
    can read back is a log line.
    """
    actor_payload = _mapping(payload.get("actor"))
    operation = _mapping(payload.get("operation"))
    return Run(
        id=str(payload["id"]),
        status=RunStatus(str(payload["status"])),
        actor=ActorRef(
            type=ActorType(str(actor_payload.get("type", "unknown"))),
            id=_optional_str(actor_payload.get("id")),
            session_id=_optional_str(actor_payload.get("session_id")),
            metadata=_mapping(actor_payload.get("metadata")),
        ),
        operation=OperationRef(
            kind=OperationKind(str(operation.get("kind", "query"))),
            engine=str(operation.get("engine", "unknown")),
            provider=_optional_str(operation.get("provider")),
        ),
        proposal=_proposal(payload.get("proposal")),
        inputs=_refs(payload.get("inputs")),
        outputs=_refs(payload.get("outputs")),
        result_ref=_result_ref(payload.get("result_ref")),
        admission=_admission(payload.get("admission")),
        confirmation=_confirmation(payload.get("confirmation")),
        execution=_execution(payload.get("execution")),
        verification=_verification(payload.get("verification")),
        failure=_failure(payload.get("failure")),
        evidence=evidence_from_dict(payload.get("evidence")),
        created_at=_time(payload.get("created_at")) or datetime.now(),
        updated_at=_time(payload.get("updated_at")) or datetime.now(),
        completed_at=_time(payload.get("completed_at")),
    )


def _proposal(value: object) -> ProposalRecord | None:
    data = _mapping(value)
    if not data:
        return None
    return ProposalRecord(
        hash=str(data.get("hash", "")),
        kind=str(data.get("kind", "sql")),
        body=_optional_str(data.get("body")),
        storage=ProposalStorage(str(data.get("storage", "full"))),
        agent_verification=tuple(str(item) for item in _sequence(data.get("agent_verification"))),
    )


def _refs(value: object) -> tuple[ResourceRef, ...]:
    return tuple(
        ResourceRef(system=str(item.get("system", "")), resource=str(item.get("resource", "")))
        for item in _sequence(value)
        if isinstance(item, dict)
    )


def _result_ref(value: object) -> QueryResultRef | None:
    data = _mapping(value)
    if not data:
        return None
    return QueryResultRef(
        rows=_optional_int(data.get("rows")) or 0,
        inline=bool(data.get("inline", True)),
        truncated=bool(data.get("truncated", False)),
        affected=_optional_int(data.get("affected")),
    )


def _admission(value: object) -> AdmissionRecord | None:
    data = _mapping(value)
    if not data:
        return None
    decided = _time(data.get("decided_at"))
    return AdmissionRecord(
        allowed=bool(data.get("allowed", False)),
        reasons=tuple(str(item) for item in _sequence(data.get("reasons"))),
        policy=_optional_str(data.get("policy")),
        policy_hash=_optional_str(data.get("policy_hash")),
        matched_rules=tuple(str(item) for item in _sequence(data.get("matched_rules"))),
        codes=tuple(str(item) for item in _sequence(data.get("codes"))),
        request=_mapping(data.get("request")) or None,
        **({"decided_at": decided} if decided is not None else {}),
    )


def _confirmation(value: object) -> ConfirmationRecord | None:
    data = _mapping(value)
    if not data:
        return None
    return ConfirmationRecord(
        status=ConfirmationStatus(str(data.get("status", "not_required"))),
        run_id=str(data.get("run_id", "")),
        proposal_hash=_optional_str(data.get("proposal_hash")),
        reasons=tuple(
            ConfirmationReason(
                code=ConfirmationReasonCode(
                    str(item.get("code", ConfirmationReasonCode.CUSTOM_POLICY_REQUIREMENT.value))
                ),
                message=str(item.get("message", "confirmation required")),
                rule=_optional_str(item.get("rule")),
                details=_mapping(item.get("details")) or None,
            )
            for item in _sequence(data.get("reasons"))
        ),
        required_at=_time(data.get("required_at")),
        confirmed_at=_time(data.get("confirmed_at")),
        declined_at=_time(data.get("declined_at")),
        metadata=_mapping(data.get("metadata")),
    )


def _execution(value: object) -> ExecutionRecord | None:
    data = _mapping(value)
    if not data:
        return None
    return ExecutionRecord(
        status=str(data.get("status", "PENDING")),
        native_id=_optional_str(data.get("native_id")),
        submitted_at=_time(data.get("submitted_at")),
        started_at=_time(data.get("started_at")),
        finished_at=_time(data.get("finished_at")),
        metrics=_mapping(data.get("metrics")),
    )


def _failure(value: object) -> Failure | None:
    """Rebuild a failure, defaulting a kind nobody recognises rather than raising.

    A stored run written by a newer version is still worth reading: losing the
    exact kind is a smaller loss than refusing to deserialize the record.
    """
    data = _mapping(value)
    if not data:
        return None
    raw = str(data.get("kind", FailureKind.ENGINE_ERROR.value))
    try:
        kind = FailureKind(raw)
    except ValueError:
        kind = FailureKind.ENGINE_ERROR
    return Failure(
        kind=kind,
        retryable=bool(data.get("retryable", kind.transient)),
        message=str(data.get("message", "")),
        native_code=_optional_str(data.get("native_code")),
        native_message=_optional_str(data.get("native_message")),
        native=_mapping(data.get("native")),
    )


def _verification(value: object) -> VerificationResult | None:
    data = _mapping(value)
    if not data:
        return None
    return VerificationResult(
        ok=bool(data.get("passed", False)),
        checks=tuple(
            CheckResult(
                name=str(item.get("check", "")),
                ok=bool(item.get("passed", False)),
                expected=item.get("expected"),
                actual=item.get("observed"),
                message=_optional_str(item.get("message")),
                supported=bool(item.get("supported", True)),
                source=_check_source(item.get("source")),
            )
            for item in _sequence(data.get("checks"))
            if isinstance(item, dict)
        ),
    )


def evidence_from_dict(value: object) -> EvidenceBundle | None:
    """An evidence bundle from its stored form.

    Public because `__all__` has claimed it was since the store was written,
    while only a private `_evidence` existed — so `import *` raised. A store
    implementer needs this for the same reason they need `run_from_dict`.
    """
    data = _mapping(value)
    if not data:
        return None
    return EvidenceBundle(
        run_id=str(data.get("run_id", "")),
        engine=str(data.get("engine", "")),
        operation=str(data.get("operation", "")),
        decision=str(data.get("decision", "")),
        native_execution_id=_optional_str(data.get("native_execution_id")),
        proposal_hash=_optional_str(data.get("proposal_hash")),
        inputs=tuple(str(item) for item in _sequence(data.get("inputs"))),
        outputs=tuple(str(item) for item in _sequence(data.get("outputs"))),
        started_at=_time(data.get("started_at")),
        finished_at=_time(data.get("finished_at")),
        execution=_mapping(data.get("execution")),
        observations=tuple(
            Observation(
                name=str(item["name"]),
                value=item.get("value"),
                source=ObservationSource(str(item.get("source", "gantry"))),
                unit=_optional_str(item.get("unit")),
                observed_at=_time(item.get("observed_at")) or datetime.now(),
            )
            for item in _sequence(data.get("observations"))
            if isinstance(item, dict)
        ),
        checks=tuple(
            CheckResult(
                name=str(item.get("check", "")),
                ok=bool(item.get("passed", False)),
                expected=item.get("expected"),
                actual=item.get("observed"),
                message=_optional_str(item.get("message")),
                supported=bool(item.get("supported", True)),
                source=_check_source(item.get("source")),
            )
            for item in _sequence(data.get("checks"))
            if isinstance(item, dict)
        ),
    )


def _sequence(value: object) -> list[dict[str, object]]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, object]:
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def _check_source(value: object) -> CheckSource | str | None:
    """Restore `CheckSource` where the stored value is one.

    The source field carries two kinds of answer — who asked for the check, and
    where the observation came from. Only the first is an enum, and a round
    trip that flattened it to a string made `check.source is CheckSource.AGENT`
    quietly false for a run read back from disk.
    """
    if value is None:
        return None
    text = str(value)
    try:
        return CheckSource(text)
    except ValueError:
        return text


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _time(value: object) -> datetime | None:
    return None if value is None else datetime.fromisoformat(str(value))


__all__ = ["evidence_from_dict", "run_from_dict"]
