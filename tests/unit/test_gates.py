# SPDX-License-Identifier: Apache-2.0
"""Cutover gates.

A gate reads a fact the runtime already measured and compares it to a threshold
the spec declared. Nothing here estimates or infers, and these tests exist
mostly to keep it that way — the failure mode for this module is somebody
making it clever.

The distinction the tests hammer hardest: **unmeasured is not passed.** A gate
nobody supplied a reading for blocks, because otherwise forgetting to wire a
probe is indistinguishable from wiring one that always says yes.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from gantry.core.evidence import (
    ScopeKind,
    Severity,
    VerificationResult,
    VerificationScope,
    VerificationStatus,
)
from gantry.core.verification import CheckName
from gantry.migration.gates import GateFacts, GateName, GateOutcome, evaluate
from gantry.migration.model import CutoverGates
from gantry.migration.prepare import (
    PrepareCheck,
    PrepareFailure,
    PrepareReport,
)

AT = datetime(2026, 9, 5, tzinfo=UTC)


def green() -> GateFacts:
    """Everything measured, everything fine."""
    return GateFacts(
        partitions_total=12,
        partitions_verified=12,
        streaming=True,
        cdc_lag=timedelta(milliseconds=800),
        verification=(),
        target_healthy=True,
        prepare=PrepareReport(migration="m"),
        approved_by="arvind",
    )


def failure(severity: Severity = Severity.CRITICAL) -> VerificationResult:
    return VerificationResult(
        check=CheckName.CHUNK_CHECKSUM,
        status=VerificationStatus.FAILED,
        scope=VerificationScope(kind=ScopeKind.DATASET, dataset="public.orders"),
        severity=severity,
        operation="orders-snapshot",
        plan_version=1,
        # The model insists a failed checksum records how the sides differ,
        # which is right: a failure nobody can act on is a log line.
        difference="source 12,000 rows, target 11,999",
        observed_at=AT,
    )


def outcome(facts: GateFacts, gate: GateName, gates: CutoverGates | None = None) -> GateOutcome:
    report = evaluate("m", gates or CutoverGates(), facts)
    result = report.gate(gate)
    assert result is not None
    return result.outcome


class TestUnmeasuredBlocks:
    """The property that makes the rest trustworthy."""

    @pytest.mark.parametrize(
        "gate",
        [
            GateName.ALL_PARTITIONS_VERIFIED,
            GateName.MAX_CDC_LAG,
            GateName.CRITICAL_VERIFICATION_FAILURES,
            GateName.TARGET_HEALTHY,
            GateName.SCHEMA_COMPATIBLE,
        ],
    )
    def test_a_gate_with_no_reading_is_unknown_and_blocks(self, gate: GateName) -> None:
        report = evaluate("m", CutoverGates(), GateFacts())
        result = report.gate(gate)
        assert result is not None
        assert result.outcome is GateOutcome.UNKNOWN
        assert result.blocks

    def test_nothing_measured_means_nothing_passes(self) -> None:
        report = evaluate("m", CutoverGates(), GateFacts())
        assert not report.passed
        assert len(report.blocking) == len(report.results)


class TestPartitions:
    def test_every_partition_verified_passes(self) -> None:
        assert outcome(green(), GateName.ALL_PARTITIONS_VERIFIED) is GateOutcome.PASSED

    def test_one_unverified_partition_blocks(self) -> None:
        """Verified, not merely copied. A partition that arrived without being
        checked is exactly the one a cutover should not trust."""
        facts = GateFacts(**{**green().__dict__, "partitions_verified": 11})
        assert outcome(facts, GateName.ALL_PARTITIONS_VERIFIED) is GateOutcome.FAILED

    def test_the_failure_says_how_many_are_missing(self) -> None:
        facts = GateFacts(**{**green().__dict__, "partitions_verified": 9})
        result = evaluate("m", CutoverGates(), facts).gate(GateName.ALL_PARTITIONS_VERIFIED)
        assert result is not None
        assert "3 partition(s) unverified" in (result.detail or "")


class TestLag:
    def test_within_the_threshold_passes(self) -> None:
        assert outcome(green(), GateName.MAX_CDC_LAG) is GateOutcome.PASSED

    def test_over_the_threshold_blocks(self) -> None:
        facts = GateFacts(**{**green().__dict__, "cdc_lag": timedelta(seconds=9)})
        assert outcome(facts, GateName.MAX_CDC_LAG) is GateOutcome.FAILED

    def test_a_snapshot_migration_has_no_stream_to_be_behind(self) -> None:
        """Disabled, not unknown. Blocking a snapshot cutover on a stream that
        does not exist would make the gate unusable; faking a zero reading
        would be worse, because it reads as a measurement nobody took."""
        facts = GateFacts(**{**green().__dict__, "streaming": False, "cdc_lag": None})
        assert outcome(facts, GateName.MAX_CDC_LAG) is GateOutcome.DISABLED

    def test_a_streaming_migration_with_no_reading_still_blocks(self) -> None:
        """Silence means nobody looked. Only an explicit streaming=False says
        there is no stream."""
        facts = GateFacts(**{**green().__dict__, "streaming": True, "cdc_lag": None})
        assert outcome(facts, GateName.MAX_CDC_LAG) is GateOutcome.UNKNOWN


class TestCriticalFailures:
    def test_no_failures_passes(self) -> None:
        assert outcome(green(), GateName.CRITICAL_VERIFICATION_FAILURES) is GateOutcome.PASSED

    def test_a_critical_failure_blocks(self) -> None:
        facts = GateFacts(**{**green().__dict__, "verification": (failure(),)})
        assert outcome(facts, GateName.CRITICAL_VERIFICATION_FAILURES) is GateOutcome.FAILED

    def test_a_warning_does_not_block(self) -> None:
        """Warnings make a Result worth reading, not untrustworthy."""
        facts = GateFacts(**{**green().__dict__, "verification": (failure(Severity.WARNING),)})
        assert outcome(facts, GateName.CRITICAL_VERIFICATION_FAILURES) is GateOutcome.PASSED

    def test_a_tolerance_can_be_raised_but_has_to_be_written_down(self) -> None:
        facts = GateFacts(**{**green().__dict__, "verification": (failure(),)})
        relaxed = CutoverGates(critical_verification_failures=1)
        assert (
            outcome(facts, GateName.CRITICAL_VERIFICATION_FAILURES, relaxed) is GateOutcome.PASSED
        )


class TestSchema:
    def test_a_compatible_target_passes(self) -> None:
        assert outcome(green(), GateName.SCHEMA_COMPATIBLE) is GateOutcome.PASSED

    def test_an_incompatibility_blocks_and_names_one(self) -> None:
        """Re-checked at cutover, not trusted from planning. A column altered
        during the snapshot is exactly what this catches."""
        report = PrepareReport(migration="m")
        report.failures.append(
            PrepareFailure(
                check=PrepareCheck.SCHEMA_COMPATIBLE,
                subject="public.orders.amount",
                problem="target is narrower and would truncate",
                repair="alter_target",
            )
        )
        facts = GateFacts(**{**green().__dict__, "prepare": report})
        result = evaluate("m", CutoverGates(), facts).gate(GateName.SCHEMA_COMPATIBLE)
        assert result is not None
        assert result.outcome is GateOutcome.FAILED
        assert "amount" in (result.detail or "")


class TestApproval:
    def test_a_named_operator_passes(self) -> None:
        assert outcome(green(), GateName.REQUIRE_APPROVAL) is GateOutcome.PASSED

    def test_nobody_blocks(self) -> None:
        facts = GateFacts(**{**green().__dict__, "approved_by": None})
        assert outcome(facts, GateName.REQUIRE_APPROVAL) is GateOutcome.FAILED

    def test_whitespace_is_not_a_name(self) -> None:
        facts = GateFacts(**{**green().__dict__, "approved_by": "   "})
        assert outcome(facts, GateName.REQUIRE_APPROVAL) is GateOutcome.FAILED

    def test_approval_can_be_turned_off_explicitly(self) -> None:
        facts = GateFacts(**{**green().__dict__, "approved_by": None})
        unattended = CutoverGates(require_approval=False)
        assert outcome(facts, GateName.REQUIRE_APPROVAL, unattended) is GateOutcome.DISABLED


class TestTheReport:
    def test_all_gates_green_passes(self) -> None:
        assert evaluate("m", CutoverGates(), green()).passed

    def test_a_disabled_gate_is_recorded_not_omitted(self) -> None:
        """An operator reading the report afterwards needs to see that a check
        was turned off, not merely fail to see that it ran."""
        gates = CutoverGates(target_healthy=False)
        report = evaluate("m", gates, green())
        result = report.gate(GateName.TARGET_HEALTHY)
        assert result is not None
        assert result.outcome is GateOutcome.DISABLED
        assert not result.blocks

    def test_evidence_keeps_every_gate_including_the_ones_that_passed(self) -> None:
        """The question a post-mortem asks is what we believed when we decided,
        and a record of only the failures cannot answer it."""
        evidence = evaluate("m", CutoverGates(), green()).as_evidence()
        assert evidence["passed"] is True
        gates = evidence["gates"]
        assert isinstance(gates, list)
        assert len(gates) == len(GateName)

    def test_the_summary_names_every_blocking_gate(self) -> None:
        report = evaluate("m", CutoverGates(), GateFacts(approved_by="arvind"))
        summary = report.describe()
        for result in report.blocking:
            assert result.gate.value in summary
