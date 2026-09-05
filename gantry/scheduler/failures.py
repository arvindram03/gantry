"""Failure classification.

A worker that treats every error the same way either gives up on a transient
blip or retries a schema mismatch forever. Neither is acceptable, so failures
are sorted into what the runtime can actually do about them:

- retry the same work
- stop, because retrying cannot help
- stop and replan, because the plan itself no longer fits the world

The default is RETRYABLE. The runtime assumes retries will happen, and an
unrecognised error is more often a transient one than a permanent one - a
bounded attempt count turns a wrong guess into a quarantined task rather than
an infinite loop.
"""

from __future__ import annotations

from enum import StrEnum


class FailureClass(StrEnum):
    RETRYABLE = "retryable"
    FATAL = "fatal"
    NEEDS_REPLAN = "needs_replan"


# Conditions that resolve on their own: contention, timeouts, a target that is
# briefly unavailable.
_RETRYABLE_MARKERS = (
    "deadlock detected",
    "could not serialize access",
    "lock timeout",
    "canceling statement due to statement timeout",
    "connection was closed",
    "connection is closed",
    "connection reset",
    "server closed the connection",
    "too many connections",
    "cannot connect",
    "timeout expired",
    "temporarily unavailable",
)

# The plan describes a world that no longer exists. Retrying reproduces the
# same error; a human or a planner has to change the plan.
_REPLAN_MARKERS = (
    "does not exist",
    "undefinedtable",
    "undefinedcolumn",
    "column mismatch",
    "no key",
    "stable key",
    "discover the source",
    "has no field",
)

# Retrying cannot help and neither can replanning: the request itself is
# refused.
_FATAL_MARKERS = (
    "permission denied",
    "insufficient privilege",
    "authentication failed",
    "unsafe identifier",
    "unsupported column type",
    "unsafe target name",
)


def classify(error: BaseException) -> FailureClass:
    """Sort a failure into what can be done about it."""
    text = f"{type(error).__name__}: {error}".lower()

    for marker in _FATAL_MARKERS:
        if marker in text:
            return FailureClass.FATAL
    for marker in _REPLAN_MARKERS:
        if marker in text:
            return FailureClass.NEEDS_REPLAN
    for marker in _RETRYABLE_MARKERS:
        if marker in text:
            return FailureClass.RETRYABLE

    return FailureClass.RETRYABLE


def is_retryable(error: BaseException) -> bool:
    return classify(error) is FailureClass.RETRYABLE
