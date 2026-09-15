# SPDX-License-Identifier: Apache-2.0
"""Portable execution failure taxonomy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum


class FailureKind(StrEnum):
    """A portable reason an execution did not produce a trustworthy result.

    Normalized across engines so a caller can branch on the kind rather than
    parse an engine's message. The distinctions that matter most are refusals
    before execution (`POLICY_REJECTED`,
    `UNSUPPORTED_POLICY_REQUIREMENT`), failures during it (`ENGINE_ERROR`,
    `TIMEOUT`), and a run that finished but cannot be believed
    (`VERIFICATION_FAILED`).
    """

    VALIDATION_ERROR = "VALIDATION_ERROR"
    POLICY_REJECTED = "POLICY_REJECTED"
    UNSUPPORTED_POLICY_REQUIREMENT = "UNSUPPORTED_POLICY_REQUIREMENT"
    INPUT_NOT_ALLOWED = "INPUT_NOT_ALLOWED"
    OUTPUT_NOT_ALLOWED = "OUTPUT_NOT_ALLOWED"
    SOURCE_NOT_ALLOWED = "SOURCE_NOT_ALLOWED"
    DESTINATION_NOT_ALLOWED = "DESTINATION_NOT_ALLOWED"
    DESTINATION_EXISTS = "DESTINATION_EXISTS"
    OPERATION_NOT_ALLOWED = "OPERATION_NOT_ALLOWED"
    SUBMISSION_ERROR = "SUBMISSION_ERROR"
    AUTH_ERROR = "AUTH_ERROR"
    OBJECT_NOT_FOUND = "OBJECT_NOT_FOUND"
    SYNTAX_ERROR = "SYNTAX_ERROR"
    CONNECTOR_ERROR = "CONNECTOR_ERROR"
    RESOURCE_ERROR = "RESOURCE_ERROR"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    COST_LIMIT_EXCEEDED = "COST_LIMIT_EXCEEDED"
    TIMEOUT = "TIMEOUT"
    ENGINE_ERROR = "ENGINE_ERROR"
    USER_CODE_ERROR = "USER_CODE_ERROR"
    DATA_ERROR = "DATA_ERROR"
    CANCELLED = "CANCELLED"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    UNSUPPORTED_VERIFICATION = "UNSUPPORTED_VERIFICATION"
    VERIFICATION_UNSUPPORTED = "VERIFICATION_UNSUPPORTED"
    VERIFICATION_CONFLICT = "VERIFICATION_CONFLICT"
    UNKNOWN = "UNKNOWN"

    @property
    def transient(self) -> bool:
        """Whether this kind describes a condition that may pass on its own.

        The rule every adapter should agree with, written once. Before this
        existed each adapter carried its own copy — BigQuery tested
        `kind in {TIMEOUT, RESOURCE_EXHAUSTED}`, Snowflake `kind is TIMEOUT`,
        Flink an if-chain — and nothing made a new adapter agree with the old
        ones.

        The set is deliberately small and conservative, because the cost of the
        two mistakes is not symmetric. Calling a permanent failure transient
        invites a caller to retry something that will never succeed; calling a
        transient one permanent only costs them an attempt they could have made.

        Three kinds are left out on purpose. `CONNECTOR_ERROR` is a broker that
        is down *or* a sink table that does not exist, and the message rarely
        says which. `ENGINE_ERROR` and `UNKNOWN` mean Gantry could not tell what
        went wrong, and advising a retry on that basis is advice with nothing
        behind it.
        """
        return self in _TRANSIENT


_TRANSIENT = frozenset(
    {
        FailureKind.TIMEOUT,
        FailureKind.RESOURCE_ERROR,
        FailureKind.RESOURCE_EXHAUSTED,
    }
)


@dataclass(frozen=True, slots=True)
class Failure:
    """One normalized failure, keeping the engine's own words alongside.

    `kind` and `retryable` are for code to act on; `message` is for a human.
    `native_code`, `native_message` and `native` preserve what the engine
    actually said, so normalizing never loses the detail needed to debug.

    `retryable` says one narrow thing: **the condition that caused this failure
    may pass on its own.** It should equal `kind.transient` unless an adapter
    genuinely knows better about its own engine, and a test enforces that across
    the library so the flag means the same thing on every backend.

    It does not say the work is safe to run again. A materialization that timed
    out halfway may have left its destination behind, and create-only means the
    retry fails with `DESTINATION_EXISTS` rather than succeeding. That question
    belongs to the run, which knows what kind of operation it was and how far it
    got: see `Run.safe_to_retry`.

    Gantry itself never retries. A retried write is a second attempt at
    something policy admitted once, and deciding that belongs to the
    application, not to the layer whose job is to be the record of what ran.
    """

    kind: FailureKind
    retryable: bool
    message: str
    native_code: str | None = None
    native_message: str | None = None
    native: Mapping[str, object] = field(default_factory=dict)
