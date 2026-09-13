# SPDX-License-Identifier: Apache-2.0
"""The durable record of one piece of governed data work.

A `Run` answers, months later and without the conversation that produced it:
who asked for this, what did they ask for, what was allowed, what actually ran,
what was checked, what supports the answer, and what did Gantry decide.

It is a control-plane record, not the data. Inputs and outputs are references.
Observations are scalars. The one exception is deliberate and local: a query's
rows travel on the returned object, because a caller needs them, and are
dropped on the way to storage, because a durable file is the wrong place for
query results to accumulate.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256

from gantry.actor import ActorRef
from gantry.confirmation.model import ConfirmationRecord
from gantry.confirmation.status import ConfirmationStatus
from gantry.evidence import EvidenceBundle, _plain
from gantry.failure import Failure
from gantry.handle import ExecutionHandle
from gantry.runs.status import RunStatus
from gantry.verifier import CheckResult, VerificationResult

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"  # Crockford base32: no I, L, O, U


def new_run_id(*, now: datetime | None = None) -> str:
    """A sortable, opaque identifier: `run_` plus a ULID-shaped value.

    Time-ordered so that sorting run ids sorts by creation, which makes a
    listing useful without a secondary index. Opaque to callers, and unrelated
    to any engine's own job id — both are recorded, and conflating them would
    tie Gantry's record to a provider's lifetime.
    """
    moment = now or datetime.now(UTC)
    milliseconds = int(moment.timestamp() * 1000)
    stamp = "".join(_ALPHABET[(milliseconds >> shift) & 0x1F] for shift in range(45, -1, -5))
    randomness = "".join(secrets.choice(_ALPHABET) for _ in range(16))
    return f"run_{stamp}{randomness}"


class OperationKind(StrEnum):
    """What was asked for, at the granularity a reader cares about.

    Deliberately not provider API shapes: a BigQuery load job and a Postgres
    `CREATE TABLE AS` are both materializations, and a record that says so is
    readable across engines.
    """

    QUERY = "query"
    MATERIALIZE = "materialize"
    BATCH = "batch"
    STREAM = "stream"


class ProposalStorage(StrEnum):
    """How much of the proposal a run keeps."""

    FULL = "full"
    HASH = "hash"


@dataclass(frozen=True, slots=True)
class OperationRef:
    kind: OperationKind
    engine: str
    provider: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {"kind": self.kind.value, "engine": self.engine, "provider": self.provider}


@dataclass(frozen=True, slots=True)
class ProposalRecord:
    """What the caller asked Gantry to run.

    The hash is always kept; the body depends on configuration. Agent-written
    SQL can carry values from the data it is filtering — a customer id in a
    `WHERE` clause is the ordinary case — so `hash` mode exists for callers who
    would rather a durable file did not accumulate them.
    """

    hash: str
    kind: str = "sql"
    body: str | None = None
    storage: ProposalStorage = ProposalStorage.FULL
    agent_verification: tuple[str, ...] = ()

    @classmethod
    def of(
        cls,
        body: str,
        *,
        kind: str = "sql",
        storage: ProposalStorage = ProposalStorage.FULL,
        agent_verification: Sequence[str] = (),
    ) -> ProposalRecord:
        return cls(
            hash=f"sha256:{sha256(body.encode()).hexdigest()}",
            kind=kind,
            body=body if storage is ProposalStorage.FULL else None,
            storage=storage,
            agent_verification=tuple(agent_verification),
        )

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "hash": self.hash,
            "kind": self.kind,
            "storage": self.storage.value,
        }
        if self.body is not None:
            payload["body"] = self.body
        if self.agent_verification:
            payload["agent_verification"] = list(self.agent_verification)
        return payload


@dataclass(frozen=True, slots=True)
class ResourceRef:
    """A table, collection or topic, named by the system that holds it."""

    system: str
    resource: str

    def as_dict(self) -> dict[str, object]:
        return {"system": self.system, "resource": self.resource}


@dataclass(frozen=True, slots=True)
class QueryResultRef:
    """A bounded query's output, as a reference rather than its rows."""

    rows: int
    inline: bool = True
    truncated: bool = False

    def as_dict(self) -> dict[str, object]:
        return {"rows": self.rows, "inline": self.inline, "truncated": self.truncated}


@dataclass(frozen=True, slots=True)
class AdmissionRecord:
    """Gantry's authority decision, and what it rested on.

    `policy` and `policy_hash` name the exact configuration that decided, so a
    run stays explainable after the policy changes: v0 never re-evaluates work
    that was already admitted, and a record that pointed at "the policy" rather
    than at one version of it would quietly start lying the next time someone
    edited a rule.

    `request` is the normalized question — actor, operation, engine, inputs,
    outputs, environment — as provider inspection derived it, not as the
    proposal described itself. `codes` are the machine-readable reasons;
    `reasons` the same thing in a sentence.
    """

    allowed: bool
    reasons: tuple[str, ...] = ()
    policy: str | None = None
    policy_hash: str | None = None
    matched_rules: tuple[str, ...] = ()
    codes: tuple[str, ...] = ()
    request: Mapping[str, object] | None = None
    decided_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed": self.allowed,
            "reasons": list(self.reasons),
            "policy": self.policy,
            "policy_hash": self.policy_hash,
            "matched_rules": list(self.matched_rules),
            "codes": list(self.codes),
            "request": None if self.request is None else dict(self.request),
            "decided_at": self.decided_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ExecutionRecord:
    """What the engine did, as the engine reported it."""

    status: str = "PENDING"
    native_id: str | None = None
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    metrics: Mapping[str, object] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return int((self.finished_at - self.started_at).total_seconds() * 1000)

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "native_id": self.native_id,
            "submitted_at": _time(self.submitted_at),
            "started_at": _time(self.started_at),
            "finished_at": _time(self.finished_at),
            "duration_ms": self.duration_ms,
            "metrics": {key: _plain(value) for key, value in self.metrics.items()},
        }


@dataclass(frozen=True, slots=True)
class Run:
    """One governed operation, from proposal to decision.

    Built by the operation, not by the caller: `run_id` is allocated and the
    record persisted before anything external happens, so an engine job cannot
    exist without a control-plane record of why it was allowed to.
    """

    id: str
    status: RunStatus
    actor: ActorRef
    operation: OperationRef
    proposal: ProposalRecord | None = None
    inputs: tuple[ResourceRef, ...] = ()
    outputs: tuple[ResourceRef, ...] = ()
    result_ref: QueryResultRef | None = None
    admission: AdmissionRecord | None = None
    confirmation: ConfirmationRecord | None = None
    execution: ExecutionRecord | None = None
    verification: VerificationResult | None = None
    evidence: EvidenceBundle | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None

    # Live-only. Excluded from `as_dict`, and therefore from storage: a run
    # record is not a place for query results to accumulate.
    inline: object | None = field(default=None, compare=False, repr=False)
    """The bounded result, in whatever shape the engine returned it.

    Rows for SQL, documents for MongoDB. Read it through `rows`, `columns` or
    `documents` rather than directly — those narrow it, and they are empty
    rather than wrong when the operation produced nothing.
    """

    failure: Failure | None = field(default=None, compare=False, repr=False)
    handle: ExecutionHandle | None = field(default=None, compare=False, repr=False)
    """The engine handle, for callers that submit and wait separately.

    Live-only, like `inline`. What persists is the engine's own id inside the
    execution record; a handle is a thing to act on now, not a thing to read
    back later.
    """

    @property
    def ok(self) -> bool:
        """Accepted means executed *and* verified. Nothing else is acceptance."""
        return self.status is RunStatus.ACCEPTED

    @property
    def run_id(self) -> str:
        """Alias for `id`, for callers that hold results from several systems."""
        return self.id

    @property
    def rows(self) -> tuple[tuple[object, ...], ...]:
        """A SQL query's rows, when it returned any and they came back inline."""
        return tuple(getattr(self.inline, "rows", ()) or ())

    @property
    def columns(self) -> tuple[str, ...]:
        """The column names a SQL query returned."""
        return tuple(getattr(self.inline, "columns", ()) or ())

    @property
    def truncated(self) -> bool:
        """Whether a bound clipped the result.

        Recorded rather than implied: a truncated answer changes what every
        other observation about it means.
        """
        if self.result_ref is not None:
            return self.result_ref.truncated
        return bool(getattr(self.inline, "truncated", False))

    @property
    def documents(self) -> tuple[object, ...]:
        """A document query's results, when it returned any."""
        return tuple(getattr(self.inline, "documents", ()) or ())

    @property
    def uri(self) -> str | None:
        """The first durable output, in the engine-owned URI form.

        A resource is recorded the way the engine names it —
        `reporting.rollup` — because that is what someone reads. The URI keeps
        the slash form the rest of the library already returns, so a caller
        that was matching on it still matches.
        """
        ref = next(iter(self.outputs), None)
        if ref is None:
            return None
        if "://" in ref.resource:
            return ref.resource
        return f"{ref.system}://{ref.resource.replace('.', '/')}"

    @property
    def trusted_checks(self) -> tuple[CheckResult, ...]:
        return self._checks("trusted")

    @property
    def agent_checks(self) -> tuple[CheckResult, ...]:
        return self._checks("agent")

    def _checks(self, source: str) -> tuple[CheckResult, ...]:
        """Split checks by provenance, without losing any.

        `CheckResult.source` carries two kinds of answer: who asked for the
        check (`trusted`, `agent`) and where its observation came from
        (`postgres`, `result set`). Only `agent` means agent-proposed, so
        everything else is trusted — a check whose source names a provider is
        still one the application required, and grouping on equality alone
        dropped it from the record entirely.
        """
        checks = () if self.verification is None else self.verification.checks
        if source == "agent":
            return tuple(check for check in checks if str(check.source or "") == "agent")
        return tuple(check for check in checks if str(check.source or "") != "agent")

    def with_failure(self, failure: Failure | None) -> Run:
        """Attach the failure a caller needs to read, without persisting it twice.

        The reason is already recorded — in the admission record, the execution
        record, or the failing check. This is the live object carrying it in the
        shape callers already expect.
        """
        return self if failure is None else replace(self, failure=failure)

    def advanced(self, status: RunStatus, **changes: object) -> Run:
        """The same run, moved on. One run evolves; stages are not separate runs."""
        now = datetime.now(UTC)
        completed = now if status.terminal else self.completed_at
        return replace(
            self,
            status=status,
            updated_at=now,
            completed_at=completed,
            **changes,  # type: ignore[arg-type]
        )

    def as_dict(self) -> dict[str, object]:
        """The stored form. Never includes result rows."""
        return {
            "id": self.id,
            "status": self.status.value,
            "actor": self.actor.as_dict(),
            "operation": self.operation.as_dict(),
            "proposal": None if self.proposal is None else self.proposal.as_dict(),
            "inputs": [ref.as_dict() for ref in self.inputs],
            "outputs": [ref.as_dict() for ref in self.outputs],
            "result_ref": None if self.result_ref is None else self.result_ref.as_dict(),
            "admission": None if self.admission is None else self.admission.as_dict(),
            "confirmation": None if self.confirmation is None else self.confirmation.as_dict(),
            "execution": None if self.execution is None else self.execution.as_dict(),
            "verification": _verification_as_dict(self.verification),
            "evidence": None if self.evidence is None else self.evidence.as_dict(),
            "created_at": _time(self.created_at),
            "updated_at": _time(self.updated_at),
            "completed_at": _time(self.completed_at),
        }

    def to_json(self, *, indent: int | None = None) -> str:
        return json.dumps(self.as_dict(), indent=indent)

    def render(self) -> str:
        return render(self)


def _verification_as_dict(verification: VerificationResult | None) -> dict[str, object] | None:
    if verification is None:
        return None
    return {
        "passed": verification.ok,
        "checks": [
            {
                "check": check.name,
                "source": str(check.source or "trusted"),
                "passed": check.ok,
                "supported": check.supported,
                "expected": _plain(check.expected),
                "observed": _plain(check.actual),
                "message": check.message,
            }
            for check in verification.checks
        ],
    }


def _time(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def render(run: Run) -> str:
    """A run as a person reads it.

    Trusted and agent-proposed checks are shown apart, because the difference
    is the point: one set the application required, the other the agent
    offered, and a reader deciding whether to believe the outcome needs to know
    which is which.
    """
    lines = [f"Run {run.id}", "─" * 52, ""]
    lines.extend(["Actor", f"  {run.actor.label}", ""])
    lines.extend(["Operation", f"  {run.operation.kind.value}", ""])
    lines.extend(["Engine", f"  {run.operation.engine}", ""])

    for label, refs in (("Inputs", run.inputs), ("Outputs", run.outputs)):
        if refs:
            lines.extend([label, *(f"  {ref.resource}" for ref in refs), ""])
    if run.result_ref is not None:
        suffix = " (truncated)" if run.result_ref.truncated else ""
        lines.extend(["Result", f"  {run.result_ref.rows} rows{suffix}", ""])

    if run.admission is not None:
        lines.extend(["Admission", *_admission_lines(run.admission), ""])

    if run.confirmation is not None:
        lines.extend(["Confirmation", *_confirmation_lines(run.confirmation), ""])

    if run.execution is not None:
        lines.extend(["Execution", f"  {run.execution.status}"])
        if run.execution.native_id:
            lines.append(f"  native job: {run.execution.native_id}")
        if run.execution.duration_ms is not None:
            lines.append(f"  runtime: {run.execution.duration_ms / 1000:.1f}s")
        lines.append("")

    if run.verification is not None and run.verification.checks:
        lines.append("Verification")
        for label, checks in (
            ("Trusted", run.trusted_checks),
            ("Agent-proposed", run.agent_checks),
        ):
            if not checks:
                continue
            lines.extend(["", f"  {label}"])
            for check in checks:
                mark = "✓" if check.ok else ("?" if not check.supported else "✗")
                lines.append(f"    {mark} {check.name}")
                if check.expected is not None:
                    lines.append(f"        expected: {_scalar(check.expected)}")
                if check.actual is not None:
                    lines.append(f"        observed: {_scalar(check.actual)}")
                if check.message:
                    lines.append(f"        {check.message}")
        lines.append("")

    lines.extend(["Decision", f"  {run.status.value}"])
    return "\n".join(lines)


def _admission_lines(admission: AdmissionRecord) -> list[str]:
    """The admission block: which policy, which resources, which codes.

    Resource-by-resource, because that is the granularity the decision was made
    at. A reader looking at a refusal wants to know which table was the problem,
    not that one of six was.
    """
    lines = [f"  {'✓ allowed' if admission.allowed else '✗ refused'}"]
    if admission.policy is not None:
        lines.append(f"  policy: {admission.policy}")
    request = admission.request or {}
    refused = {str(reason) for reason in admission.reasons}
    denied_resources = {
        resource for resource in _resource_names(request) if _named_in(resource, refused)
    }
    for verb, key in (("read", "inputs"), ("write", "outputs")):
        for resource in _sequence_of_str(request.get(key)):
            mark = "✗" if resource in denied_resources else "✓"
            lines.append(f"    {mark} {verb} {resource}")
    lines.extend(f"    {code}" for code in admission.codes)
    lines.extend(f"      {reason}" for reason in admission.reasons)
    return lines


def _confirmation_lines(confirmation: ConfirmationRecord) -> list[str]:
    """What was asked and what came back, never phrased as an approval.

    "the host confirmed" and "a trusted human approved" are different claims,
    and only the first one is true. A record that reads like the second would
    be the most misleading line in the file.
    """
    marks = {
        ConfirmationStatus.NOT_REQUIRED: "  — not required",
        ConfirmationStatus.REQUIRED: "  ! awaiting user confirmation",
        ConfirmationStatus.CONFIRMED: "  ✓ required\n  ✓ confirmed by the host",
        ConfirmationStatus.DECLINED: "  ✓ required\n  ✗ declined by the host",
    }
    lines = marks[confirmation.status].split("\n")
    lines.extend(f"    {reason.code.value}: {reason.message}" for reason in confirmation.reasons)
    return lines


def _resource_names(request: Mapping[str, object]) -> tuple[str, ...]:
    return (*_sequence_of_str(request.get("inputs")), *_sequence_of_str(request.get("outputs")))


def _sequence_of_str(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(str(item) for item in value)


def _named_in(resource: str, reasons: set[str]) -> bool:
    return any(resource in reason for reason in reasons)


def _scalar(value: object) -> str:
    if isinstance(value, dict):
        parts = [f"{key} {value[key]}" for key in value if value[key] is not None]
        return ", ".join(parts) if parts else "—"
    return str(value)


__all__ = [
    "AdmissionRecord",
    "ExecutionRecord",
    "OperationKind",
    "OperationRef",
    "ProposalRecord",
    "ProposalStorage",
    "QueryResultRef",
    "ResourceRef",
    "Run",
    "new_run_id",
    "render",
]
