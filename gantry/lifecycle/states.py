# SPDX-License-Identifier: Apache-2.0
"""The Operation state machine.

Transitions are the only way an Operation changes state, and every transition
is persisted with actor and reason. Illegal transitions raise rather than being
tolerated: a state machine that quietly accepts an impossible move cannot be
used as evidence afterwards.

The table is data, not a pile of conditionals, so the legal graph can be
inspected and tested directly.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ResourceName
from gantry.core.operation import TERMINAL_STATES, LifecycleStage, OperationState

S = OperationState

# Legal transitions. The happy path follows the lifecycle stages in order;
# every non-terminal state may fail or pause.
_TRANSITIONS: dict[OperationState, frozenset[OperationState]] = {
    S.DRAFT: frozenset({S.PLANNED, S.FAILED}),
    S.PLANNED: frozenset({S.GENERATED, S.FAILED, S.PAUSED}),
    # DRAFT is reachable from here because a validation failure is repairable
    # input for a planner, not a dead end.
    S.GENERATED: frozenset({S.VALIDATED, S.DRAFT, S.FAILED, S.PAUSED}),
    # Validation failure is a normal outcome that feeds agentic repair, so it
    # returns to DRAFT rather than dead-ending.
    S.VALIDATED: frozenset({S.EXECUTING, S.DRAFT, S.FAILED, S.PAUSED}),
    S.EXECUTING: frozenset({S.VERIFYING, S.FAILED, S.PAUSED}),
    # An engine reporting success only reaches VERIFYING. Verification decides
    # whether the Operation completed or produced an untrustworthy result.
    S.VERIFYING: frozenset({S.COMPLETED, S.VERIFICATION_FAILED, S.FAILED, S.PAUSED}),
    S.PAUSED: frozenset({S.PLANNED, S.GENERATED, S.VALIDATED, S.EXECUTING, S.VERIFYING, S.FAILED}),
    # Terminal states. Repair starts a new attempt rather than reviving one.
    S.COMPLETED: frozenset(),
    S.FAILED: frozenset(),
    S.VERIFICATION_FAILED: frozenset(),
}

# The stage an Operation is in once it has reached a state.
_STATE_STAGE: dict[OperationState, LifecycleStage | None] = {
    S.DRAFT: None,
    S.PLANNED: LifecycleStage.PLAN,
    S.GENERATED: LifecycleStage.GENERATE,
    S.VALIDATED: LifecycleStage.VALIDATE,
    S.EXECUTING: LifecycleStage.EXECUTE,
    S.VERIFYING: LifecycleStage.VERIFY,
    S.COMPLETED: LifecycleStage.RESULT,
    S.VERIFICATION_FAILED: LifecycleStage.VERIFY,
    S.FAILED: None,
    S.PAUSED: None,
}


class ActorKind(StrEnum):
    """Who caused a transition. Agents propose; they do not execute."""

    RUNTIME = "runtime"
    OPERATOR = "operator"
    AGENT = "agent"


class IllegalTransitionError(Exception):
    def __init__(self, operation: str, current: OperationState, requested: OperationState) -> None:
        allowed = sorted(state.value for state in _TRANSITIONS[current])
        super().__init__(
            f"operation {operation!r}: cannot move from {current.value!r} to "
            f"{requested.value!r} (allowed: {', '.join(allowed) or 'none, terminal state'})"
        )
        self.operation = operation
        self.current = current
        self.requested = requested


class StateTransition(BaseModel):
    """A persisted record of one state change."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: ResourceName
    from_state: OperationState
    to_state: OperationState
    actor: ActorKind
    reason: str
    occurred_at: datetime
    plan_version: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _require_tz(self) -> StateTransition:
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return self


def allowed_transitions(state: OperationState) -> frozenset[OperationState]:
    return _TRANSITIONS[state]


def can_transition(current: OperationState, requested: OperationState) -> bool:
    return requested in _TRANSITIONS[current]


def stage_for(state: OperationState) -> LifecycleStage | None:
    """The lifecycle stage an Operation has reached in this state."""
    return _STATE_STAGE[state]


def is_terminal(state: OperationState) -> bool:
    return state in TERMINAL_STATES


def transition(
    operation: str,
    current: OperationState,
    requested: OperationState,
    *,
    actor: ActorKind,
    reason: str,
    occurred_at: datetime,
    plan_version: int | None = None,
) -> StateTransition:
    """Validate a transition and produce its audit record.

    Returns the record rather than writing it: persistence belongs to the
    store, and separating them keeps the legal graph testable without one.
    """
    if not can_transition(current, requested):
        raise IllegalTransitionError(operation, current, requested)
    if not reason.strip():
        raise ValueError("every transition requires a reason for the audit log")
    return StateTransition(
        operation=operation,
        from_state=current,
        to_state=requested,
        actor=actor,
        reason=reason,
        occurred_at=occurred_at,
        plan_version=plan_version,
    )
