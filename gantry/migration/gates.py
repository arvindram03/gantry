# SPDX-License-Identifier: Apache-2.0
"""Cutover gates (RFC 0 §7 Phase 8).

The heart of the workflow, and the part most likely to be built wrong by making
it clever. **A gate is not a heuristic.** It reads a fact the runtime already
measured and compares it to a threshold the spec declared. Nothing in this
module estimates, infers, or asks a model what it thinks.

That constraint is what makes the gates worth having. An operator who is about
to move production traffic needs to know exactly what was true and exactly what
was required — not that something scored well. If a decision needs judgement,
it is an approval, and approvals are recorded with a name attached rather than
computed.

**A gate that passes is recorded as carefully as one that fails.** The question
a post-mortem asks is not "what went wrong" but "what did we believe when we
decided", and a report that keeps only the failures cannot answer it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum

from gantry.core.durations import format_duration
from gantry.core.evidence import Severity, VerificationResult, VerificationStatus
from gantry.migration.model import CutoverGates
from gantry.migration.prepare import PrepareReport
from gantry.migration.reconcile import ReconciliationReport


class GateName(StrEnum):
    ALL_PARTITIONS_VERIFIED = "allPartitionsVerified"
    MAX_CDC_LAG = "maxCdcLag"
    CRITICAL_VERIFICATION_FAILURES = "criticalVerificationFailures"
    TARGET_HEALTHY = "targetHealthy"
    SCHEMA_COMPATIBLE = "schemaCompatible"
    REQUIRE_APPROVAL = "requireApproval"


class GateOutcome(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    # The spec turned this gate off. Recorded rather than omitted: an operator
    # reading the report afterwards needs to see that a check was disabled,
    # not merely fail to see that it ran.
    DISABLED = "disabled"
    # Nothing measured it. Distinct from a failure, because "we did not look"
    # and "we looked and it was wrong" call for different responses — and
    # treating them alike would let an unwired probe read as a green gate.
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class GateResult:
    """One gate: what was required, what was measured, and the verdict."""

    gate: GateName
    outcome: GateOutcome
    required: str
    measured: str
    detail: str | None = None

    @property
    def blocks(self) -> bool:
        """Whether this stops a cutover.

        An unknown gate blocks. A gate nobody measured is not a gate that
        passed, and defaulting the other way would make forgetting to wire a
        probe indistinguishable from wiring one that always says yes.
        """
        return self.outcome in (GateOutcome.FAILED, GateOutcome.UNKNOWN)

    def describe(self) -> str:
        line = f"{self.gate.value}: {self.outcome.value}  measured {self.measured}"
        if self.outcome is not GateOutcome.DISABLED:
            line += f", required {self.required}"
        return line + (f" — {self.detail}" if self.detail else "")


@dataclass
class GateReport:
    """Every gate, evaluated together, for one cutover decision."""

    migration: str
    results: list[GateResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.blocking

    @property
    def blocking(self) -> tuple[GateResult, ...]:
        return tuple(result for result in self.results if result.blocks)

    def gate(self, name: GateName) -> GateResult | None:
        return next((result for result in self.results if result.gate is name), None)

    def describe(self) -> str:
        if self.passed:
            return f"{self.migration}: {len(self.results)} gates, all clear"
        names = ", ".join(result.gate.value for result in self.blocking)
        return f"{self.migration}: blocked by {names}"

    def as_evidence(self) -> dict[str, object]:
        """What gets persisted with the transition.

        Every gate, not only the blocking ones — this is the record of what was
        believed at the moment of the decision.
        """
        return {
            "passed": self.passed,
            "gates": [
                {
                    "gate": result.gate.value,
                    "outcome": result.outcome.value,
                    "required": result.required,
                    "measured": result.measured,
                }
                for result in self.results
            ],
        }


@dataclass(frozen=True)
class GateFacts:
    """What the runtime measured, gathered before any gate is evaluated.

    Every field is optional, and `None` means *nobody measured this* rather
    than zero. That distinction is the reason the type exists: a missing CDC
    lag reading and a lag of zero are opposite situations, and a gate that
    conflated them would wave through a stream nobody was watching.
    """

    partitions_total: int | None = None
    partitions_verified: int | None = None
    # Whether this migration has a change stream at all. A snapshot-only
    # migration has nothing to be behind, and treating its missing lag reading
    # as "unmeasured" would block a cutover on a stream that does not exist.
    # Faking a zero would be worse: it reads as a measurement nobody took.
    streaming: bool | None = None
    cdc_lag: timedelta | None = None
    verification: Sequence[VerificationResult] | None = None
    target_healthy: bool | None = None
    prepare: PrepareReport | None = None
    reconciliation: Sequence[ReconciliationReport] | None = None
    approved_by: str | None = None


def evaluate(migration: str, gates: CutoverGates, facts: GateFacts) -> GateReport:
    """Compare every declared gate against what was measured."""
    report = GateReport(migration=migration)
    report.results.append(_partitions(gates, facts))
    report.results.append(_lag(gates, facts))
    report.results.append(_critical_failures(gates, facts))
    report.results.append(_target_health(gates, facts))
    report.results.append(_schema(gates, facts))
    report.results.append(_approval(gates, facts))
    return report


def _partitions(gates: CutoverGates, facts: GateFacts) -> GateResult:
    if not gates.all_partitions_verified:
        return _disabled(GateName.ALL_PARTITIONS_VERIFIED)
    if facts.partitions_total is None or facts.partitions_verified is None:
        return _unknown(GateName.ALL_PARTITIONS_VERIFIED, "every partition verified")

    measured = f"{facts.partitions_verified}/{facts.partitions_total}"
    # Verified, not merely complete. A partition that copied without being
    # checked is exactly the one a cutover should not trust.
    if facts.partitions_verified >= facts.partitions_total:
        return GateResult(
            gate=GateName.ALL_PARTITIONS_VERIFIED,
            outcome=GateOutcome.PASSED,
            required="every partition verified",
            measured=measured,
        )
    return GateResult(
        gate=GateName.ALL_PARTITIONS_VERIFIED,
        outcome=GateOutcome.FAILED,
        required="every partition verified",
        measured=measured,
        detail=f"{facts.partitions_total - facts.partitions_verified} partition(s) unverified",
    )


def _lag(gates: CutoverGates, facts: GateFacts) -> GateResult:
    required = f"<= {format_duration(gates.max_cdc_lag)}"

    if facts.streaming is False:
        return GateResult(
            gate=GateName.MAX_CDC_LAG,
            outcome=GateOutcome.DISABLED,
            required="not required",
            measured="no change stream",
            detail="this migration is snapshot-only; there is nothing to be behind",
        )

    if facts.cdc_lag is None:
        # A streaming migration whose lag nobody read is not a stream that is
        # caught up. Only an explicit `streaming=False` says there is no stream
        # to measure; silence means nobody looked, and that blocks.
        return _unknown(GateName.MAX_CDC_LAG, required)

    measured = format_duration(facts.cdc_lag)
    if facts.cdc_lag <= gates.max_cdc_lag:
        return GateResult(
            gate=GateName.MAX_CDC_LAG,
            outcome=GateOutcome.PASSED,
            required=required,
            measured=measured,
        )
    return GateResult(
        gate=GateName.MAX_CDC_LAG,
        outcome=GateOutcome.FAILED,
        required=required,
        measured=measured,
        detail="the stream has not caught up; cutting over now would lose the difference",
    )


def _critical_failures(gates: CutoverGates, facts: GateFacts) -> GateResult:
    required = f"<= {gates.critical_verification_failures}"
    if facts.verification is None:
        return _unknown(GateName.CRITICAL_VERIFICATION_FAILURES, required)

    critical = [
        result
        for result in facts.verification
        if result.severity is Severity.CRITICAL and result.status is VerificationStatus.FAILED
    ]
    if len(critical) <= gates.critical_verification_failures:
        return GateResult(
            gate=GateName.CRITICAL_VERIFICATION_FAILURES,
            outcome=GateOutcome.PASSED,
            required=required,
            measured=str(len(critical)),
        )
    return GateResult(
        gate=GateName.CRITICAL_VERIFICATION_FAILURES,
        outcome=GateOutcome.FAILED,
        required=required,
        measured=str(len(critical)),
        detail="; ".join(f"{r.check.value} at {r.scope}" for r in critical[:3]),
    )


def _target_health(gates: CutoverGates, facts: GateFacts) -> GateResult:
    if not gates.target_healthy:
        return _disabled(GateName.TARGET_HEALTHY)
    if facts.target_healthy is None:
        return _unknown(GateName.TARGET_HEALTHY, "reachable and writable")
    return GateResult(
        gate=GateName.TARGET_HEALTHY,
        outcome=GateOutcome.PASSED if facts.target_healthy else GateOutcome.FAILED,
        required="reachable and writable",
        measured="healthy" if facts.target_healthy else "unhealthy",
    )


def _schema(gates: CutoverGates, facts: GateFacts) -> GateResult:
    if not gates.schema_compatible:
        return _disabled(GateName.SCHEMA_COMPATIBLE)
    if facts.prepare is None:
        return _unknown(GateName.SCHEMA_COMPATIBLE, "compatible")

    # Re-checked at cutover, not trusted from planning time. A column altered
    # during the six hours of snapshot is exactly the thing this catches, and
    # a report from before the snapshot cannot see it.
    if facts.prepare.ready:
        return GateResult(
            gate=GateName.SCHEMA_COMPATIBLE,
            outcome=GateOutcome.PASSED,
            required="compatible",
            measured="compatible",
        )
    return GateResult(
        gate=GateName.SCHEMA_COMPATIBLE,
        outcome=GateOutcome.FAILED,
        required="compatible",
        measured=f"{len(facts.prepare.failures)} incompatibility(ies)",
        detail=facts.prepare.failures[0].describe() if facts.prepare.failures else None,
    )


def _approval(gates: CutoverGates, facts: GateFacts) -> GateResult:
    if not gates.require_approval:
        return _disabled(GateName.REQUIRE_APPROVAL)
    approver = (facts.approved_by or "").strip()
    if approver:
        return GateResult(
            gate=GateName.REQUIRE_APPROVAL,
            outcome=GateOutcome.PASSED,
            required="a named operator",
            measured=approver,
        )
    return GateResult(
        gate=GateName.REQUIRE_APPROVAL,
        outcome=GateOutcome.FAILED,
        required="a named operator",
        measured="nobody",
        detail="an agent may propose a cutover; only an operator may approve one",
    )


def _disabled(gate: GateName) -> GateResult:
    return GateResult(
        gate=gate,
        outcome=GateOutcome.DISABLED,
        required="not required",
        measured="not checked",
        detail="turned off in the spec",
    )


def _unknown(gate: GateName, required: str) -> GateResult:
    return GateResult(
        gate=gate,
        outcome=GateOutcome.UNKNOWN,
        required=required,
        measured="not measured",
        detail="nothing supplied a reading; an unmeasured gate blocks",
    )
