# SPDX-License-Identifier: Apache-2.0
"""The Migration workflow state machine.

These states sit *above* the Operation lifecycle rather than replacing it. The
transition table is the place the workflow's two structural promises live, so
it is the place they are tested: cutover is reachable only through the gates,
and rollback is available only while there is something to roll back to.
"""

from __future__ import annotations

from itertools import pairwise

import pytest
from gantry.lifecycle.migration import (
    OPERATOR_ONLY,
    IllegalMigrationTransitionError,
    MigrationState,
    allowed_transitions,
    can_transition,
    check_transition,
    is_terminal,
    requires_operator,
)

M = MigrationState


class TestTheHappyPath:
    def test_the_whole_sequence_is_walkable(self) -> None:
        path = [
            M.DRAFT,
            M.DISCOVERING,
            M.PLANNED,
            M.PREPARING,
            M.SNAPSHOTTING,
            M.CATCHING_UP,
            M.VERIFYING,
            M.READY_FOR_CUTOVER,
            M.CUTTING_OVER,
            M.ROLLBACK_WINDOW,
            M.COMPLETED,
        ]
        for current, following in pairwise(path):
            assert can_transition(current, following), f"{current} -> {following}"


class TestCutoverIsOneWayThroughTheGates:
    """An edge that exists will eventually be taken."""

    @pytest.mark.parametrize(
        "state",
        [M.DRAFT, M.DISCOVERING, M.PLANNED, M.PREPARING, M.SNAPSHOTTING, M.CATCHING_UP],
    )
    def test_nothing_reaches_cutover_without_passing_verification(
        self, state: MigrationState
    ) -> None:
        assert not can_transition(state, M.CUTTING_OVER)
        assert not can_transition(state, M.READY_FOR_CUTOVER)

    def test_ready_for_cutover_comes_only_from_verifying(self) -> None:
        sources = [s for s in MigrationState if can_transition(s, M.READY_FOR_CUTOVER)]
        assert sources == [M.VERIFYING] or set(sources) == {M.VERIFYING, M.PAUSED}

    def test_cutting_over_comes_only_from_ready_for_cutover(self) -> None:
        sources = [s for s in MigrationState if can_transition(s, M.CUTTING_OVER)]
        assert sources == [M.READY_FOR_CUTOVER]

    def test_drift_found_at_verification_returns_to_catching_up(self) -> None:
        """Not a failure. The stream simply has more to apply."""
        assert can_transition(M.VERIFYING, M.CATCHING_UP)

    def test_a_stale_cutover_window_can_return_to_catching_up(self) -> None:
        """Gates passed ten minutes ago are a claim about ten minutes ago."""
        assert can_transition(M.READY_FOR_CUTOVER, M.CATCHING_UP)


class TestRollback:
    def test_a_cutover_that_fails_halfway_can_go_back(self) -> None:
        assert can_transition(M.CUTTING_OVER, M.ROLLING_BACK)

    def test_the_rollback_window_can_go_back(self) -> None:
        assert can_transition(M.ROLLBACK_WINDOW, M.ROLLING_BACK)

    def test_a_completed_migration_cannot(self) -> None:
        """The source has been released; there is nothing to roll back to."""
        assert not can_transition(M.COMPLETED, M.ROLLING_BACK)
        assert is_terminal(M.COMPLETED)

    def test_rolling_back_ends_rolled_back_or_failed_and_nothing_else(self) -> None:
        assert allowed_transitions(M.ROLLING_BACK) == frozenset({M.ROLLED_BACK, M.FAILED})

    def test_rolling_back_cannot_be_paused(self) -> None:
        """A half-completed rollback is the worst state to sit in."""
        assert not can_transition(M.ROLLING_BACK, M.PAUSED)


class TestTerminalStates:
    @pytest.mark.parametrize("state", [M.COMPLETED, M.FAILED, M.ROLLED_BACK])
    def test_terminal_states_go_nowhere(self, state: MigrationState) -> None:
        assert is_terminal(state)
        assert allowed_transitions(state) == frozenset()

    def test_a_terminal_state_says_so_when_refusing(self) -> None:
        with pytest.raises(IllegalMigrationTransitionError, match="terminal"):
            check_transition("m", M.COMPLETED, M.CUTTING_OVER)


class TestPausing:
    @pytest.mark.parametrize(
        "state",
        [
            M.DISCOVERING,
            M.PREPARING,
            M.SNAPSHOTTING,
            M.CATCHING_UP,
            M.VERIFYING,
            M.READY_FOR_CUTOVER,
        ],
    )
    def test_work_bearing_phases_can_pause(self, state: MigrationState) -> None:
        assert can_transition(state, M.PAUSED)

    def test_cutting_over_can_pause_but_not_resume_into_cutting_over(self) -> None:
        """Resuming a half-done cutover is not a resume, it is a new decision
        that has to pass the gates again."""
        assert can_transition(M.CUTTING_OVER, M.PAUSED)
        assert not can_transition(M.PAUSED, M.CUTTING_OVER)


class TestOperatorOnlyTransitions:
    def test_moving_traffic_is_an_operator_decision(self) -> None:
        """Both directions. Cutting over moves production traffic; rolling
        back moves it again. Neither is something a model may decide."""
        assert requires_operator(M.CUTTING_OVER)
        assert requires_operator(M.ROLLING_BACK)
        assert frozenset({M.CUTTING_OVER, M.ROLLING_BACK}) == OPERATOR_ONLY

    @pytest.mark.parametrize("state", [M.SNAPSHOTTING, M.VERIFYING, M.READY_FOR_CUTOVER])
    def test_everything_else_the_runtime_may_do_itself(self, state: MigrationState) -> None:
        assert not requires_operator(state)


def test_a_refusal_lists_what_would_have_been_allowed() -> None:
    with pytest.raises(IllegalMigrationTransitionError) as caught:
        check_transition("orders-to-warehouse", M.SNAPSHOTTING, M.CUTTING_OVER)

    message = str(caught.value)
    assert "orders-to-warehouse" in message
    assert "snapshotting" in message and "cutting_over" in message
    assert "catching_up" in message, "the refusal should name the way forward"


def test_every_state_has_a_row() -> None:
    """A state added without a row would raise KeyError at runtime rather
    than being rejected as an illegal transition."""
    for state in MigrationState:
        assert state in {s for s in MigrationState if allowed_transitions(s) is not None}
