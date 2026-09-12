# SPDX-License-Identifier: Apache-2.0
"""The persisted form of a run, and how a person reads it."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from gantry.evidence import EvidenceBundle


@dataclass(frozen=True, slots=True)
class RunRecord:
    """One governed run, as stored.

    A thin wrapper over the evidence rather than a second model of it: the
    bundle already carries everything the spec asks to persist, and keeping one
    shape means a stored run and a live one render identically.
    """

    evidence: EvidenceBundle
    recorded_at: datetime

    @property
    def run_id(self) -> str:
        return self.evidence.run_id

    @property
    def decision(self) -> str:
        return self.evidence.decision

    def render(self) -> str:
        """A run as a person reads it, for a terminal or an audit log."""
        return render(self.evidence)


def render(evidence: EvidenceBundle) -> str:
    """Lay a bundle out for a human, without losing the numbers.

    The check block is the point of the whole exercise: expected beside
    observed, so a reader can disagree with a bound rather than only with a
    verdict.
    """
    width = 56
    lines = [f"Run {evidence.run_id}", "─" * width, ""]
    lines.append(f"Engine       {evidence.engine}")
    lines.append(f"Operation    {evidence.operation}")
    lines.append(f"Status       {evidence.decision}")
    if evidence.proposal_hash:
        lines.append(f"Proposal     {evidence.proposal_hash[:12]}")

    for label, values in (("Inputs", evidence.inputs), ("Outputs", evidence.outputs)):
        if values:
            lines.extend(["", label, *(f"  {value}" for value in values)])

    lines.extend(["", "Execution"])
    state = evidence.execution.get("status", "UNKNOWN")
    lines.append(f"  {state}")
    if evidence.native_execution_id:
        lines.append(f"  job: {evidence.native_execution_id}")
    if evidence.duration_ms is not None:
        lines.append(f"  runtime: {evidence.duration_ms / 1000:.1f}s")

    groups = (
        ("Trusted verification", evidence.trusted_checks),
        ("Agent-proposed verification", evidence.agent_checks),
    )
    grouped = {id(check) for _, checks in groups for check in checks}
    legacy = tuple(check for check in evidence.checks if id(check) not in grouped)
    for label, checks in (*groups, ("Verification", legacy)):
        if not checks:
            continue
        lines.extend(["", label])
        for check in checks:
            mark = "✓" if check.ok else ("?" if not check.supported else "✗")
            lines.append(f"  {mark} {check.name}")
            if check.expected is not None:
                lines.append(f"      expected: {_scalar(check.expected)}")
            if check.actual is not None:
                lines.append(f"      observed: {_scalar(check.actual)}")
            if check.message:
                lines.append(f"      {check.message}")

    lines.extend(["", "Decision", f"  {evidence.decision}"])
    return "\n".join(lines)


def _scalar(value: object) -> str:
    """Render a check's expected or observed side compactly.

    `{"min": 1, "max": None}` reads as `min 1`, because a bound nobody set is
    noise in a report someone is scanning.
    """
    if isinstance(value, dict):
        parts = [f"{key} {value[key]}" for key in value if value[key] is not None]
        return ", ".join(parts) if parts else "—"
    return str(value)
