# SPDX-License-Identifier: Apache-2.0
"""Cutover, the rollback window, and rolling back (RFC 0 §7 Phases 8-10).

Gantry decides whether you *may* cut over, records that decision with the
evidence behind it, and holds the rollback window open. It does not move your
traffic. Redirecting an application is your deploy system's job, and owning it
would make Gantry a proxy — the same category error as owning the bytes on the
wire.

So what a "cutover" is here is narrower and more useful than it sounds: drain
the stream to zero, reconcile one last time, record the exact source position
the two sides agreed at, and note who decided. That position is what makes a
rollback meaningful afterwards — without it, "go back to the source" means
going back to an unknown point.

**Rollback is never automatic.** Divergence during the window may mean the
migration was wrong, or it may mean the application is now writing to the
target correctly and the source is stale *by design*. Nothing in the runtime
can tell those apart from the outside, and guessing would be worse than
reporting. So the window watches, reports, and waits to be told.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from gantry.core.positions import SourcePosition
from gantry.migration.gates import GateReport
from gantry.migration.reconcile import ReconciliationReport


class CutoverStep(StrEnum):
    """What the cutover did, in order."""

    DRAIN = "drain"
    FINAL_RECONCILE = "final_reconcile"
    RECORD_POSITION = "record_position"


@dataclass(frozen=True)
class StepResult:
    step: CutoverStep
    completed: bool
    detail: str

    def describe(self) -> str:
        mark = "ok" if self.completed else "failed"
        return f"{self.step.value}: {mark} — {self.detail}"


@dataclass
class CutoverRecord:
    """What happened at cutover, and the point it happened at."""

    migration: str
    approved_by: str
    reason: str
    at: datetime
    gates: GateReport
    steps: list[StepResult] = field(default_factory=list)
    # The source position both sides agreed at. This is what a rollback goes
    # back to; without it "return to the source" names no particular instant.
    position: SourcePosition | None = None
    reconciliation: tuple[ReconciliationReport, ...] = ()

    @property
    def completed(self) -> bool:
        return all(step.completed for step in self.steps)

    def describe(self) -> str:
        where = f" at {self.position.value}" if self.position else ""
        return (
            f"{self.migration} cut over{where} by {self.approved_by} "
            f"at {self.at.isoformat(timespec='seconds')}"
        )

    def as_evidence(self) -> dict[str, object]:
        return {
            "approved_by": self.approved_by,
            "reason": self.reason,
            "at": self.at.isoformat(),
            "position": None if self.position is None else self.position.value,
            # The kind travels with the value. A position is opaque to the
            # runtime and only comparable within its kind, so reading one back
            # and assuming it was an LSN would be exactly the assumption
            # `SourcePosition` exists to prevent.
            "position_kind": None if self.position is None else self.position.kind.value,
            "steps": [step.describe() for step in self.steps],
            "gates": self.gates.as_evidence(),
        }


class WindowState(StrEnum):
    HOLDING = "holding"
    DIVERGED = "diverged"
    ELAPSED = "elapsed"


@dataclass
class RollbackWindow:
    """The period during which the source remains the rollback authority."""

    migration: str
    opened_at: datetime
    duration: timedelta
    source_authoritative: bool = True
    reconciliation: tuple[ReconciliationReport, ...] = ()

    @property
    def closes_at(self) -> datetime:
        return self.opened_at + self.duration

    def remaining(self, now: datetime) -> timedelta:
        left = self.closes_at - now
        return left if left > timedelta(0) else timedelta(0)

    @property
    def diverged(self) -> bool:
        return any(not report.agreed for report in self.reconciliation)

    def state(self, now: datetime) -> WindowState:
        """Where the window stands.

        Divergence is reported ahead of elapsing, because an operator who has
        both facts needs the alarming one first. It is still only a report:
        nothing here rolls anything back.
        """
        if self.diverged:
            return WindowState.DIVERGED
        return WindowState.ELAPSED if now >= self.closes_at else WindowState.HOLDING

    def describe(self, now: datetime) -> str:
        state = self.state(now)
        authority = "source" if self.source_authoritative else "target"
        if state is WindowState.DIVERGED:
            names = ", ".join(r.dataset for r in self.reconciliation if not r.agreed)
            return (
                f"{self.migration}: diverged on {names} — "
                f"{authority} is still authoritative; rolling back is your call"
            )
        if state is WindowState.ELAPSED:
            return f"{self.migration}: window closed; ready to finalize"
        return (
            f"{self.migration}: holding, {_short(self.remaining(now))} left, "
            f"{authority} authoritative"
        )


@dataclass(frozen=True)
class RollbackRecord:
    """Authority returned to the source, and why."""

    migration: str
    decided_by: str
    reason: str
    at: datetime
    # The position the cutover recorded. Rolling back means treating the source
    # as authoritative from here, which is only meaningful if that point is
    # known — which is why the cutover records it.
    cutover_position: SourcePosition | None = None

    def describe(self) -> str:
        where = f" from {self.cutover_position.value}" if self.cutover_position else ""
        return f"{self.migration} rolled back{where} by {self.decided_by}: {self.reason}"

    def as_evidence(self) -> dict[str, object]:
        return {
            "decided_by": self.decided_by,
            "reason": self.reason,
            "at": self.at.isoformat(),
            "from_position": (
                None if self.cutover_position is None else self.cutover_position.value
            ),
            # Traffic rollback, not a reverse bulk migration (§7 Phase 9). The
            # source was never stopped being the authority, so there is nothing
            # to move back.
            "method": "traffic",
        }


def drain_step(lag: timedelta | None, *, threshold: timedelta) -> StepResult:
    """Whether the stream is caught up enough to stop writing to the source."""
    if lag is None:
        return StepResult(
            step=CutoverStep.DRAIN,
            completed=True,
            detail="no change stream to drain",
        )
    if lag <= threshold:
        return StepResult(
            step=CutoverStep.DRAIN,
            completed=True,
            detail=f"lag {_short(lag)} within {_short(threshold)}",
        )
    return StepResult(
        step=CutoverStep.DRAIN,
        completed=False,
        detail=f"lag {_short(lag)} still over {_short(threshold)}",
    )


def reconcile_step(reports: Sequence[ReconciliationReport]) -> StepResult:
    """The last comparison before traffic moves.

    Separate from the reconciliation that opened `READY_FOR_CUTOVER`, and run
    after the drain rather than before: the point of draining is that more rows
    arrived, and a reconciliation from before them proves nothing about now.
    """
    if not reports:
        return StepResult(
            step=CutoverStep.FINAL_RECONCILE,
            completed=False,
            detail="nothing reconciled; a cutover on an unchecked target is a guess",
        )
    disagreed = [report.dataset for report in reports if not report.agreed]
    if disagreed:
        return StepResult(
            step=CutoverStep.FINAL_RECONCILE,
            completed=False,
            detail=f"{', '.join(disagreed)} disagree after draining",
        )
    return StepResult(
        step=CutoverStep.FINAL_RECONCILE,
        completed=True,
        detail=f"{len(reports)} dataset(s) agree",
    )


def position_step(position: SourcePosition | None) -> StepResult:
    if position is None:
        return StepResult(
            step=CutoverStep.RECORD_POSITION,
            completed=False,
            detail="no source position recorded; a rollback would have no point to return to",
        )
    return StepResult(
        step=CutoverStep.RECORD_POSITION,
        completed=True,
        detail=f"{position.kind.value}={position.value}",
    )


def build_record(
    migration: str,
    *,
    approved_by: str,
    reason: str,
    gates: GateReport,
    lag: timedelta | None,
    lag_threshold: timedelta,
    reconciliation: Sequence[ReconciliationReport],
    position: SourcePosition | None,
    at: datetime | None = None,
) -> CutoverRecord:
    """Run the cutover steps in order and record what each one found."""
    record = CutoverRecord(
        migration=migration,
        approved_by=approved_by,
        reason=reason,
        at=at or datetime.now(UTC),
        gates=gates,
        position=position,
        reconciliation=tuple(reconciliation),
    )
    record.steps.append(drain_step(lag, threshold=lag_threshold))
    record.steps.append(reconcile_step(reconciliation))
    record.steps.append(position_step(position))
    return record


def _short(value: timedelta) -> str:
    seconds = value.total_seconds()
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 90:
        return f"{seconds:.1f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"
