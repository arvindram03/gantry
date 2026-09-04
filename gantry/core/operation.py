"""Operation identity and lifecycle states.

Movement and Analysis are both Operations. The runtime enforces one lifecycle
over both; operation types contribute sub-states within `EXECUTING` but do not
define their own top-level lifecycle.
"""

from __future__ import annotations

from enum import StrEnum


class OperationType(StrEnum):
    MOVEMENT = "movement"
    ANALYSIS = "analysis"


class LifecycleStage(StrEnum):
    """The guarantee boundary, in order.

    Generation is a no-op for operations that compile straight to an engine
    API, but the stage still exists so provenance has one shape.
    """

    PLAN = "plan"
    GENERATE = "generate"
    VALIDATE = "validate"
    EXECUTE = "execute"
    VERIFY = "verify"
    RESULT = "result"


class OperationState(StrEnum):
    """Top-level Operation states.

    `VALIDATED` is reachable only through deterministic pre-execution
    validation, and `COMPLETED` only through verification. An engine reporting
    success does not by itself advance an Operation past `VERIFYING`.
    """

    DRAFT = "draft"
    PLANNED = "planned"
    GENERATED = "generated"
    VALIDATED = "validated"
    EXECUTING = "executing"
    VERIFYING = "verifying"
    COMPLETED = "completed"

    FAILED = "failed"
    PAUSED = "paused"
    VERIFICATION_FAILED = "verification_failed"


class MovementSubState(StrEnum):
    """Movement progress within `EXECUTING`."""

    PREPARING = "preparing"
    SNAPSHOTTING = "snapshotting"
    CATCHING_UP = "catching_up"


TERMINAL_STATES: frozenset[OperationState] = frozenset(
    {
        OperationState.COMPLETED,
        OperationState.FAILED,
        OperationState.VERIFICATION_FAILED,
    }
)
