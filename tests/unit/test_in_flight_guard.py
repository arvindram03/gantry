# SPDX-License-Identifier: Apache-2.0
"""Refusing a plan version aimed at an Operation that is still running.

The distinction these tests exist to hold is narrow and easy to lose: resuming
the *same* plan version is replay, and the whole design rests on it working.
Starting a *different* version on top of a live run is the thing that
half-finished a Movement and then reported a verification failure — which
reads as a data bug and is not one.

A guard that blocks both would break crash recovery. A guard that blocks
neither is what shipped in v1.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from gantry.core.operation import OperationState, OperationType
from gantry.lifecycle.states import OperationInFlightError
from gantry.movement.service import _refuse_if_in_flight
from gantry.state.operations import OperationRecord

AT = datetime(2026, 9, 5, tzinfo=UTC)


def record(state: OperationState, version: int | None = 1) -> OperationRecord:
    return OperationRecord(
        name="orders-snapshot",
        operation_type=OperationType.MOVEMENT,
        state=state,
        current_plan_version=version,
        row_version=1,
        created_at=AT,
        updated_at=AT,
    )


def test_resuming_the_same_version_is_allowed() -> None:
    """Replay. A crashed worker resumes exactly this way, and blocking it
    would break the guarantee the runtime is built on."""
    _refuse_if_in_flight(record(OperationState.EXECUTING, 1), 1)


def test_a_different_version_on_a_live_run_is_refused() -> None:
    with pytest.raises(OperationInFlightError) as caught:
        _refuse_if_in_flight(record(OperationState.EXECUTING, 1), 2)

    assert caught.value.in_flight == 1
    assert caught.value.requested == 2


def test_the_refusal_names_the_operation_both_versions_and_the_fix() -> None:
    """A denial that does not say what to do invites retrying at random."""
    with pytest.raises(OperationInFlightError) as caught:
        _refuse_if_in_flight(record(OperationState.EXECUTING, 1), 2)

    message = str(caught.value)
    assert "orders-snapshot" in message
    assert "version 1" in message and "version 2" in message
    # `pause`, not `abort`: aborting reaches FAILED, which is terminal, so
    # recommending it would leave an Operation that cannot be restarted.
    assert "gantry pause" in message
    assert "gantry abort" not in message
    assert "gantry status" in message


@pytest.mark.parametrize(
    "state",
    [
        OperationState.DRAFT,
        OperationState.PLANNED,
        OperationState.GENERATED,
        OperationState.VALIDATED,
        OperationState.PAUSED,
        OperationState.COMPLETED,
        OperationState.FAILED,
    ],
)
def test_only_executing_blocks(state: OperationState) -> None:
    """Pausing is how an operator stops to change something, so a paused
    Operation must stay replannable. Nothing else has work in flight."""
    _refuse_if_in_flight(record(state, 1), 2)


def test_an_executing_operation_with_no_recorded_version_still_refuses() -> None:
    """Unknown is not the same as matching. Letting it through would make the
    guard depend on bookkeeping being complete."""
    with pytest.raises(OperationInFlightError, match="an earlier version"):
        _refuse_if_in_flight(record(OperationState.EXECUTING, None), 2)
