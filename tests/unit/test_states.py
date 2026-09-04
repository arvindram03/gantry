"""The Operation state machine."""

from __future__ import annotations

from datetime import UTC, datetime
from itertools import pairwise

import pytest
from gantry.core.operation import OperationState as S
from gantry.lifecycle.states import (
    ActorKind,
    IllegalTransitionError,
    allowed_transitions,
    can_transition,
    is_terminal,
    stage_for,
    transition,
)

AT = datetime(2026, 9, 10, tzinfo=UTC)


def move(current: S, requested: S, **kwargs: object) -> object:
    return transition(
        "op",
        current,
        requested,
        actor=ActorKind.RUNTIME,
        reason="test",
        occurred_at=AT,
        **kwargs,  # type: ignore[arg-type]
    )


def test_happy_path_follows_the_lifecycle() -> None:
    path = [S.DRAFT, S.PLANNED, S.GENERATED, S.VALIDATED, S.EXECUTING, S.VERIFYING, S.COMPLETED]
    for current, requested in pairwise(path):
        assert can_transition(current, requested), f"{current} -> {requested}"


def test_execution_cannot_skip_verification() -> None:
    """An engine reporting success does not by itself complete an Operation."""
    assert not can_transition(S.EXECUTING, S.COMPLETED)
    assert can_transition(S.EXECUTING, S.VERIFYING)


def test_verification_failure_is_distinct_from_failure() -> None:
    """A technically successful job can still produce an untrustworthy result."""
    assert can_transition(S.VERIFYING, S.VERIFICATION_FAILED)
    assert is_terminal(S.VERIFICATION_FAILED)


def test_validation_failure_returns_to_draft_for_repair() -> None:
    """Validation failures are structured input to a planner, not dead ends."""
    assert can_transition(S.VALIDATED, S.DRAFT)


def test_nothing_may_skip_validation_to_execute() -> None:
    for state in (S.DRAFT, S.PLANNED, S.GENERATED):
        assert not can_transition(state, S.EXECUTING)


def test_terminal_states_have_no_successors() -> None:
    for state in (S.COMPLETED, S.FAILED, S.VERIFICATION_FAILED):
        assert allowed_transitions(state) == frozenset()
        assert is_terminal(state)


def test_paused_resumes_to_where_it_left_off() -> None:
    for state in (S.PLANNED, S.GENERATED, S.VALIDATED, S.EXECUTING, S.VERIFYING):
        assert can_transition(S.PAUSED, state)


def test_completed_cannot_be_revived() -> None:
    with pytest.raises(IllegalTransitionError, match="terminal state"):
        move(S.COMPLETED, S.EXECUTING)


def test_illegal_transition_names_the_allowed_moves() -> None:
    with pytest.raises(IllegalTransitionError, match="allowed: failed, planned"):
        move(S.DRAFT, S.COMPLETED)


def test_transition_records_actor_and_reason() -> None:
    record = transition(
        "op",
        S.DRAFT,
        S.PLANNED,
        actor=ActorKind.OPERATOR,
        reason="operator started the run",
        occurred_at=AT,
        plan_version=3,
    )
    assert record.from_state is S.DRAFT
    assert record.to_state is S.PLANNED
    assert record.actor is ActorKind.OPERATOR
    assert record.reason == "operator started the run"
    assert record.plan_version == 3


def test_every_transition_requires_a_reason() -> None:
    """Audit records with empty reasons are not evidence of anything."""
    with pytest.raises(ValueError, match="requires a reason"):
        transition("op", S.DRAFT, S.PLANNED, actor=ActorKind.RUNTIME, reason="   ", occurred_at=AT)


def test_transition_requires_tz_aware_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        transition(
            "op",
            S.DRAFT,
            S.PLANNED,
            actor=ActorKind.RUNTIME,
            reason="test",
            occurred_at=datetime(2026, 9, 10),
        )


def test_every_state_has_a_transition_entry() -> None:
    """The table is data; every state must appear in it."""
    for state in S:
        assert isinstance(allowed_transitions(state), frozenset)
        stage_for(state)


def test_no_transition_targets_draft_except_repair() -> None:
    origins = {state for state in S if S.DRAFT in allowed_transitions(state)}
    assert origins == {S.VALIDATED}


def test_agents_are_a_recognised_actor_but_not_a_special_case() -> None:
    """Agents propose; the same rules apply to what they cause."""
    with pytest.raises(IllegalTransitionError):
        transition(
            "op",
            S.DRAFT,
            S.COMPLETED,
            actor=ActorKind.AGENT,
            reason="agent tried to skip ahead",
            occurred_at=AT,
        )
