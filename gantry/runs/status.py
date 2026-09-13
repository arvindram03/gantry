# SPDX-License-Identifier: Apache-2.0
"""The states a governed run moves through, and what each one means."""

from __future__ import annotations

from enum import StrEnum


class RunStatus(StrEnum):
    """Where a run got to, and why it stopped there.

    The distinctions that matter are between the ways a run can fail. A
    proposal Gantry refused, work the engine could not do, a check that
    could not be evaluated, and a result that was measured and rejected are
    four different events with four different remedies. Collapsing any of
    them into "failed" throws away the only part anyone can act on.
    """

    PENDING = "PENDING"
    """Recorded, not yet admitted. A run exists here before anything external does."""

    POLICY_REJECTED = "POLICY_REJECTED"
    """Refused before execution. Nothing ran."""

    AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"
    """Policy allowed it, and a rule asked that the user be told first.

    Not a refusal and not terminal: the run is parked, nothing external has
    started, and it resumes if the host confirms. A run left here forever is the
    honest record of a question nobody answered.
    """

    CONFIRMATION_DECLINED = "CONFIRMATION_DECLINED"
    """The host said the user declined. Terminal, and nothing ran."""

    VERIFICATION_CONFLICT = "VERIFICATION_CONFLICT"
    """The verification contract contradicted itself, so no outcome could satisfy it."""

    RUNNING = "RUNNING"
    """Submitted to the engine and in flight."""

    EXECUTION_FAILED = "EXECUTION_FAILED"
    """The engine could not carry out work that Gantry had admitted."""

    VERIFYING = "VERIFYING"
    """Execution finished, or a stream reached its required state; checks are running."""

    VERIFICATION_UNSUPPORTED = "VERIFICATION_UNSUPPORTED"
    """A required check could not be evaluated, so acceptance cannot be claimed."""

    REJECTED = "REJECTED"
    """It ran, it was measured, and the result is not acceptable."""

    ACCEPTED = "ACCEPTED"
    """Executed and verified. For a stream, this means it reached a verified healthy
    state — not that it has finished, and not that it will stay healthy."""

    @property
    def awaiting(self) -> bool:
        """Parked, waiting on someone outside Gantry. Not terminal, not running."""
        return self is RunStatus.AWAITING_CONFIRMATION

    @property
    def terminal(self) -> bool:
        return self in _TERMINAL

    @property
    def accepted(self) -> bool:
        return self is RunStatus.ACCEPTED


_TERMINAL = frozenset(
    {
        RunStatus.POLICY_REJECTED,
        RunStatus.CONFIRMATION_DECLINED,
        RunStatus.VERIFICATION_CONFLICT,
        RunStatus.EXECUTION_FAILED,
        RunStatus.VERIFICATION_UNSUPPORTED,
        RunStatus.REJECTED,
        RunStatus.ACCEPTED,
    }
)


__all__ = ["RunStatus"]
