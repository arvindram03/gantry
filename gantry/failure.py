# SPDX-License-Identifier: Apache-2.0
"""Portable execution failure taxonomy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum


class FailureKind(StrEnum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    POLICY_REJECTED = "POLICY_REJECTED"
    UNSUPPORTED_POLICY_REQUIREMENT = "UNSUPPORTED_POLICY_REQUIREMENT"
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
    kind: FailureKind
    retryable: bool
    message: str
    native_code: str | None = None
    native_message: str | None = None
    native: Mapping[str, object] = field(default_factory=dict)
