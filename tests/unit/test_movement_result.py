"""MovementResult: the first concrete Result."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from gantry.core.provenance import DatasetPin, Lineage, Provenance
from gantry.core.results import ResultKind, ResultStatus
from gantry.movement.result import MovementResult

AT = datetime(2026, 9, 18, tzinfo=UTC)


def result(**overrides: object) -> MovementResult:
    base: dict[str, object] = {
        "name": "orders-snapshot.movement",
        "status": ResultStatus.OK,
        "provenance": Provenance(generated_at=AT, operation="orders-snapshot", plan_version=1),
        "created_at": AT,
        "started_at": AT,
        "finished_at": AT + timedelta(seconds=100),
        "rows_inserted": 1_000_000,
        "partitions_total": 10,
        "partitions_complete": 10,
    }
    base.update(overrides)
    return MovementResult.model_validate(base)


def test_it_is_a_movement_result() -> None:
    assert result().kind is ResultKind.MOVEMENT


def test_throughput_is_derived_from_the_run() -> None:
    assert result().rows_per_second == 10_000


def test_an_instant_result_reports_no_rate() -> None:
    assert result(finished_at=AT).rows_per_second is None


def test_completion_and_trustworthiness_are_separate() -> None:
    """Finishing every partition is not the same as having verified them.

    Conflating the two is how a migration gets declared successful because all
    the jobs ran.
    """
    unverified = result(partitions_verified=0)
    assert unverified.is_complete
    assert unverified.is_trustworthy

    rejected = result(status=ResultStatus.VERIFICATION_FAILED)
    assert rejected.is_complete
    assert not rejected.is_trustworthy


def test_an_unfinished_run_is_not_complete() -> None:
    assert not result(partitions_complete=7).is_complete


def test_more_partitions_complete_than_exist_is_rejected() -> None:
    with pytest.raises(ValueError, match="more partitions complete"):
        result(partitions_complete=11)


def test_time_must_move_forwards() -> None:
    with pytest.raises(ValueError, match="must not precede"):
        result(finished_at=AT - timedelta(seconds=1))


def test_provenance_is_required() -> None:
    with pytest.raises(ValueError):
        MovementResult.model_validate(
            {
                "name": "x",
                "status": ResultStatus.OK,
                "created_at": AT,
                "started_at": AT,
                "finished_at": AT,
            }
        )


def test_provenance_pins_the_inputs_it_read() -> None:
    pin = DatasetPin(name="public.orders", version=2, content_hash="sha256:" + "a" * 64)
    pinned = result(
        provenance=Provenance(
            generated_at=AT,
            operation="orders-snapshot",
            plan_version=1,
            lineage=Lineage(inputs=(pin,)),
        )
    )
    assert pinned.provenance.lineage.inputs[0].version == 2
    assert str(pinned.provenance.lineage.inputs[0]) == "public.orders@2"
