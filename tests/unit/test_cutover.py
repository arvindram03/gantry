# SPDX-License-Identifier: Apache-2.0
"""Cutover steps, the rollback window, and what neither of them does.

Gantry decides whether you may cut over and holds the window open. It does not
move traffic and it does not roll back on its own — the tests that matter most
here are the ones asserting an *absence*.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from gantry.core.positions import PositionKind, SourcePosition
from gantry.migration.cutover import (
    CutoverStep,
    RollbackRecord,
    RollbackWindow,
    WindowState,
    build_record,
    drain_step,
    position_step,
    reconcile_step,
)
from gantry.migration.gates import GateReport
from gantry.migration.reconcile import (
    LayerOutcome,
    LayerResult,
    ReconciliationLayer,
    ReconciliationReport,
)

AT = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
LSN = SourcePosition(kind=PositionKind.LSN, value="37450805752")


def report(dataset: str = "public.orders", *, agreed: bool = True) -> ReconciliationReport:
    result = ReconciliationReport(dataset=dataset, target=dataset)
    result.layers.append(
        LayerResult(
            layer=ReconciliationLayer.COUNT,
            outcome=LayerOutcome.AGREED if agreed else LayerOutcome.DISAGREED,
            detail="test",
        )
    )
    return result


class TestDrain:
    def test_lag_within_the_threshold_completes(self) -> None:
        assert drain_step(timedelta(seconds=1), threshold=timedelta(seconds=2)).completed

    def test_lag_over_the_threshold_does_not(self) -> None:
        step = drain_step(timedelta(seconds=9), threshold=timedelta(seconds=2))
        assert not step.completed
        assert "9.0s" in step.detail and "2.0s" in step.detail

    def test_a_snapshot_migration_has_nothing_to_drain(self) -> None:
        step = drain_step(None, threshold=timedelta(seconds=2))
        assert step.completed
        assert "no change stream" in step.detail


class TestFinalReconcile:
    def test_agreement_completes(self) -> None:
        assert reconcile_step([report(), report("public.customers")]).completed

    def test_disagreement_names_the_dataset(self) -> None:
        step = reconcile_step([report(agreed=False)])
        assert not step.completed
        assert "public.orders" in step.detail

    def test_reconciling_nothing_is_not_agreement(self) -> None:
        """A cutover on a target nobody checked is a guess, and an empty list
        of reports must not read as a clean bill of health."""
        step = reconcile_step([])
        assert not step.completed
        assert "unchecked" in step.detail


class TestPosition:
    def test_a_recorded_position_completes(self) -> None:
        step = position_step(LSN)
        assert step.completed
        assert "lsn=37450805752" in step.detail

    def test_no_position_blocks_the_cutover(self) -> None:
        """Without it, 'return to the source' names no particular instant."""
        step = position_step(None)
        assert not step.completed
        assert "no point to return to" in step.detail


class TestTheRecord:
    def test_a_clean_cutover_completes_every_step(self) -> None:
        record = build_record(
            "m",
            approved_by="arvind",
            reason="release window",
            gates=GateReport(migration="m"),
            lag=None,
            lag_threshold=timedelta(seconds=2),
            reconciliation=[report()],
            position=LSN,
            at=AT,
        )
        assert record.completed
        assert [step.step for step in record.steps] == list(CutoverStep)

    def test_one_failed_step_fails_the_cutover(self) -> None:
        record = build_record(
            "m",
            approved_by="arvind",
            reason="release window",
            gates=GateReport(migration="m"),
            lag=timedelta(seconds=30),
            lag_threshold=timedelta(seconds=2),
            reconciliation=[report()],
            position=LSN,
            at=AT,
        )
        assert not record.completed

    def test_evidence_carries_the_position_and_its_kind(self) -> None:
        """A position is opaque and comparable only within its kind. Reading
        one back and assuming it was an LSN is the assumption SourcePosition
        exists to prevent."""
        record = build_record(
            "m",
            approved_by="arvind",
            reason="release window",
            gates=GateReport(migration="m"),
            lag=None,
            lag_threshold=timedelta(seconds=2),
            reconciliation=[report()],
            position=LSN,
            at=AT,
        )
        evidence = record.as_evidence()
        assert evidence["position"] == "37450805752"
        assert evidence["position_kind"] == "lsn"


class TestTheWindow:
    def window(self, **overrides: object) -> RollbackWindow:
        base: dict[str, object] = {
            "migration": "m",
            "opened_at": AT,
            "duration": timedelta(hours=24),
        }
        base.update(overrides)
        return RollbackWindow(**base)  # type: ignore[arg-type]

    def test_it_holds_until_the_duration_elapses(self) -> None:
        window = self.window()
        assert window.state(AT + timedelta(hours=1)) is WindowState.HOLDING
        assert window.remaining(AT + timedelta(hours=1)) == timedelta(hours=23)

    def test_it_elapses_on_time(self) -> None:
        assert self.window().state(AT + timedelta(hours=24)) is WindowState.ELAPSED

    def test_remaining_never_goes_negative(self) -> None:
        assert self.window().remaining(AT + timedelta(days=9)) == timedelta(0)

    def test_divergence_is_reported_ahead_of_elapsing(self) -> None:
        """An operator holding both facts needs the alarming one first."""
        window = self.window(reconciliation=(report(agreed=False),))
        assert window.state(AT + timedelta(days=9)) is WindowState.DIVERGED

    def test_divergence_does_not_roll_anything_back(self) -> None:
        """The whole point. It may mean the migration was wrong, or that the
        target is now correct and the source is stale by design; nothing in
        the runtime can tell those apart, so it reports and waits."""
        window = self.window(reconciliation=(report(agreed=False),))
        described = window.describe(AT + timedelta(hours=1))
        assert "rolling back is your call" in described
        assert "source is still authoritative" in described

    def test_the_description_says_who_is_authoritative(self) -> None:
        assert "source authoritative" in self.window().describe(AT + timedelta(hours=1))


def test_a_rollback_record_says_it_moved_traffic_not_data() -> None:
    """Traffic rollback, not a reverse bulk migration (RFC 0 §7 Phase 9). The
    source never stopped being the authority, so there is nothing to move."""
    record = RollbackRecord(
        migration="m",
        decided_by="arvind",
        reason="checkout errors spiked",
        at=AT,
        cutover_position=LSN,
    )
    assert record.as_evidence()["method"] == "traffic"
    assert "37450805752" in record.describe()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (timedelta(milliseconds=250), "250ms"),
        (timedelta(seconds=9), "9.0s"),
        (timedelta(minutes=30), "30m"),
        (timedelta(hours=24), "24.0h"),
    ],
)
def test_durations_read_at_the_scale_a_person_thinks_in(value: timedelta, expected: str) -> None:
    from gantry.migration.cutover import _short

    assert _short(value) == expected
