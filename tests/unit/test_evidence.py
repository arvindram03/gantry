"""Verification result semantics."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from gantry.core.evidence import (
    ScopeKind,
    Severity,
    VerificationResult,
    VerificationScope,
    VerificationStatus,
)
from gantry.core.provenance import Provenance
from gantry.core.results import Result, ResultKind, ResultStatus
from gantry.core.verification import CheckName

AT = datetime(2026, 9, 21, tzinfo=UTC)


def finding(**overrides: object) -> VerificationResult:
    base: dict[str, object] = {
        "check": CheckName.ROW_COUNT,
        "status": VerificationStatus.PASSED,
        "scope": VerificationScope(kind=ScopeKind.DATASET, dataset="public.orders"),
        "operation": "orders-snapshot",
        "plan_version": 1,
        "observed_at": AT,
    }
    base.update(overrides)
    return VerificationResult.model_validate(base)


def test_a_failure_must_say_how_the_sides_differ() -> None:
    """A failure without a difference tells an operator nothing actionable."""
    with pytest.raises(ValueError, match="must record how the sides differ"):
        finding(status=VerificationStatus.FAILED)


def test_a_failure_with_a_difference_is_accepted() -> None:
    failed = finding(status=VerificationStatus.FAILED, difference="500 rows missing")
    assert not failed.passed
    assert failed.blocks_cutover


def test_an_error_blocks_cutover_too() -> None:
    """An unanswered question is not a satisfied one."""
    errored = finding(status=VerificationStatus.ERRORED, difference="column not found")
    assert errored.blocks_cutover


def test_a_skip_does_not_block() -> None:
    assert not finding(status=VerificationStatus.SKIPPED).blocks_cutover


def test_a_warning_does_not_block() -> None:
    """Warnings make a Result worth reading, not untrustworthy."""
    warned = finding(
        status=VerificationStatus.FAILED,
        difference="null rate 0.06 exceeds 0.05",
        severity=Severity.WARNING,
    )
    assert not warned.blocks_cutover


def test_scope_renders_without_stuttering() -> None:
    """A partition id already carries its dataset."""
    scope = VerificationScope(
        kind=ScopeKind.PARTITION,
        dataset="public.orders",
        identifier="public.orders/00004",
    )
    assert str(scope) == "partition/public.orders/00004"


def test_scope_renders_a_plain_identifier() -> None:
    scope = VerificationScope(kind=ScopeKind.CHUNK, dataset="orders", identifier="a-f")
    assert str(scope) == "chunk/orders/a-f"


def test_describe_is_readable() -> None:
    described = finding(
        status=VerificationStatus.FAILED, difference="500 rows missing from target"
    ).describe()
    assert described == ("row_count failed at dataset/public.orders: 500 rows missing from target")


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        finding(observed_at=datetime(2026, 9, 21))


# --- evidence on a Result --------------------------------------------------


def result(*findings: VerificationResult) -> Result:
    return Result(
        name="orders-snapshot.movement",
        kind=ResultKind.MOVEMENT,
        status=ResultStatus.OK,
        provenance=Provenance(generated_at=AT),
        created_at=AT,
        verification=findings,
    )


def test_a_result_carries_its_evidence() -> None:
    """A Result that cannot show why it is trustworthy is not evidence."""
    carried = result(finding(), finding(check=CheckName.PRIMARY_KEY_UNIQUE))
    assert len(carried.verification) == 2
    assert carried.blocking_failures == ()


def test_blocking_failures_are_surfaced_separately() -> None:
    failure = finding(status=VerificationStatus.FAILED, difference="500 rows missing")
    carried = result(finding(), failure)
    assert carried.blocking_failures == (failure,)


def test_a_result_with_no_verification_carries_none() -> None:
    assert result().verification == ()
