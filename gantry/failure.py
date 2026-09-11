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
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class Failure:
    """One normalized failure, keeping the engine's own words alongside.

    `kind` and `retryable` are for code to act on; `message` is for a human.
    `native_code`, `native_message` and `native` preserve what the engine
    actually said, so normalizing never loses the detail needed to debug.
    """

    kind: FailureKind
    retryable: bool
    message: str
    native_code: str | None = None
    native_message: str | None = None
    native: Mapping[str, object] = field(default_factory=dict)
