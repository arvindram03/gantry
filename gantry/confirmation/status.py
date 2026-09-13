# SPDX-License-Identifier: Apache-2.0
"""The four states a confirmation can be in."""

from __future__ import annotations

from enum import StrEnum


class ConfirmationStatus(StrEnum):
    """Whether a run needed confirmation, and what the host said.

    `NOT_REQUIRED` is a real answer rather than an absence: a run that never
    needed asking about reads differently from one still waiting, and both have
    to be distinguishable months later.
    """

    NOT_REQUIRED = "not_required"
    """No rule asked for confirmation. Execution proceeded on policy alone."""

    REQUIRED = "required"
    """Policy allowed the operation and asked that the user be told first."""

    CONFIRMED = "confirmed"
    """The host application said the user confirmed. Not an authenticated approval."""

    DECLINED = "declined"
    """The host said no. Terminal, and nothing external ran."""

    @property
    def pending(self) -> bool:
        return self is ConfirmationStatus.REQUIRED


class ConfirmationReasonCode(StrEnum):
    """Why the host should ask, in machine-readable form.

    Small on purpose. A code is for the host to decide how loudly to ask — a
    production write may warrant a different prompt from an expensive query —
    and a vocabulary nobody can enumerate cannot be used for that.
    """

    SENSITIVE_DESTINATION = "SENSITIVE_DESTINATION"
    PRODUCTION_WRITE = "PRODUCTION_WRITE"
    DESTRUCTIVE_OPERATION = "DESTRUCTIVE_OPERATION"
    HIGH_COST = "HIGH_COST"
    LONG_RUNNING_JOB = "LONG_RUNNING_JOB"
    CUSTOM_POLICY_REQUIREMENT = "CUSTOM_POLICY_REQUIREMENT"


__all__ = ["ConfirmationReasonCode", "ConfirmationStatus"]
