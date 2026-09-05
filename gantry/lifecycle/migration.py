# SPDX-License-Identifier: Apache-2.0
"""The Migration workflow state machine (RFC 0 §9.2).

These are **workflow** states, not runtime primitives. They sit above the
Operation lifecycle rather than replacing it: while a Migration is
`SNAPSHOTTING`, the Movements beneath it are running their own
`DRAFT → PLANNED → … → COMPLETED` and keeping their own checkpoints. The
workflow state says which phase of the cutover we are in; the Operation states
say what is actually durable.

Two rules the transition table encodes, both from §9.3:

**Cutover is one-way through gates.** `READY_FOR_CUTOVER` is reachable only
from `VERIFYING`, and `CUTTING_OVER` only from `READY_FOR_CUTOVER`. There is no
edge that skips the gate evaluation, because an edge that exists will
eventually be taken.

**Rollback is available for exactly as long as the window.** `ROLLING_BACK` is
reachable from `ROLLBACK_WINDOW` and from `CUTTING_OVER` — a cutover that fails
halfway must be able to go back — and from nowhere else. Once `COMPLETED`, the
source has been released and there is nothing to roll back to.
"""

from __future__ import annotations

from enum import StrEnum


class MigrationState(StrEnum):
    """Where a Migration has got to."""

    DRAFT = "draft"
    DISCOVERING = "discovering"
    PLANNED = "planned"
    PREPARING = "preparing"
    SNAPSHOTTING = "snapshotting"
    CATCHING_UP = "catching_up"
    VERIFYING = "verifying"
    READY_FOR_CUTOVER = "ready_for_cutover"
    CUTTING_OVER = "cutting_over"
    ROLLBACK_WINDOW = "rollback_window"
    COMPLETED = "completed"

    FAILED = "failed"
    PAUSED = "paused"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"


M = MigrationState

# Every state that is not terminal may fail or be paused; listing those two on
# each row rather than special-casing them keeps the table readable as the
# thing it is — a table.
_FAILABLE = frozenset({M.FAILED, M.PAUSED})

_TRANSITIONS: dict[MigrationState, frozenset[MigrationState]] = {
    M.DRAFT: frozenset({M.DISCOVERING}) | _FAILABLE,
    M.DISCOVERING: frozenset({M.PLANNED}) | _FAILABLE,
    M.PLANNED: frozenset({M.PREPARING}) | _FAILABLE,
    # Preparing can send you back to planning: an incompatible target is
    # repairable input, not a dead end, exactly as a failed Analysis
    # validation returns to DRAFT rather than failing the Operation.
    M.PREPARING: frozenset({M.SNAPSHOTTING, M.PLANNED}) | _FAILABLE,
    M.SNAPSHOTTING: frozenset({M.CATCHING_UP, M.VERIFYING}) | _FAILABLE,
    # Catch-up and verification alternate. Reconciling does not stop the
    # stream, and a verification run that finds drift sends you back to
    # catching up rather than failing the migration.
    M.CATCHING_UP: frozenset({M.VERIFYING}) | _FAILABLE,
    M.VERIFYING: frozenset({M.READY_FOR_CUTOVER, M.CATCHING_UP}) | _FAILABLE,
    # The only door to cutover, and it is opened by the gates.
    M.READY_FOR_CUTOVER: frozenset({M.CUTTING_OVER, M.CATCHING_UP}) | _FAILABLE,
    # A cutover that fails halfway must be able to go back.
    M.CUTTING_OVER: frozenset({M.ROLLBACK_WINDOW, M.ROLLING_BACK}) | _FAILABLE,
    M.ROLLBACK_WINDOW: frozenset({M.COMPLETED, M.ROLLING_BACK}) | _FAILABLE,
    M.ROLLING_BACK: frozenset({M.ROLLED_BACK, M.FAILED}),
    # Paused resumes into any phase that can hold work.
    M.PAUSED: frozenset(
        {
            M.DISCOVERING,
            M.PLANNED,
            M.PREPARING,
            M.SNAPSHOTTING,
            M.CATCHING_UP,
            M.VERIFYING,
            M.READY_FOR_CUTOVER,
            M.ROLLBACK_WINDOW,
            M.FAILED,
        }
    ),
    # Terminal. A migration that finished, failed or rolled back is history;
    # starting again means a new Migration, not reviving this one.
    M.COMPLETED: frozenset(),
    M.FAILED: frozenset(),
    M.ROLLED_BACK: frozenset(),
}

TERMINAL_STATES: frozenset[MigrationState] = frozenset({M.COMPLETED, M.FAILED, M.ROLLED_BACK})

# Transitions no agent may propose, however it phrases the request. Cutting
# over moves production traffic and rolling back moves it again; both are
# operator decisions by construction, not by policy that could be relaxed.
OPERATOR_ONLY: frozenset[MigrationState] = frozenset({M.CUTTING_OVER, M.ROLLING_BACK})


class IllegalMigrationTransitionError(Exception):
    def __init__(self, migration: str, current: MigrationState, requested: MigrationState) -> None:
        allowed = sorted(state.value for state in _TRANSITIONS[current])
        detail = ", ".join(allowed) if allowed else "nothing - this state is terminal"
        super().__init__(
            f"migration {migration!r} cannot move from {current.value} to "
            f"{requested.value}; allowed: {detail}"
        )
        self.migration = migration
        self.current = current
        self.requested = requested


def allowed_transitions(state: MigrationState) -> frozenset[MigrationState]:
    return _TRANSITIONS[state]


def can_transition(current: MigrationState, requested: MigrationState) -> bool:
    return requested in _TRANSITIONS[current]


def is_terminal(state: MigrationState) -> bool:
    return state in TERMINAL_STATES


def requires_operator(state: MigrationState) -> bool:
    """Whether reaching this state is an operator's decision alone."""
    return state in OPERATOR_ONLY


def check_transition(migration: str, current: MigrationState, requested: MigrationState) -> None:
    if not can_transition(current, requested):
        raise IllegalMigrationTransitionError(migration, current, requested)
