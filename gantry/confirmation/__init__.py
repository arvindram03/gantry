# SPDX-License-Identifier: Apache-2.0
"""Confirmation: this is allowed, but ask the user before it runs.

> **Confirmation is a user-interaction gate, not an authentication guarantee.**

Gantry does not claim that a trusted human approved anything. It claims only
that an operation was marked as requiring confirmation and that the host
application supplied confirmation before execution. Authorization is policy's
job; this is the separate question of whether the host should ask first.
"""

from gantry.confirmation.model import (
    NOT_REQUIRED,
    ConfirmationReason,
    ConfirmationRecord,
    ConfirmationRequirement,
    reasons_from,
)
from gantry.confirmation.status import ConfirmationReasonCode, ConfirmationStatus

__all__ = [
    "NOT_REQUIRED",
    "ConfirmationReason",
    "ConfirmationReasonCode",
    "ConfirmationRecord",
    "ConfirmationRequirement",
    "ConfirmationStatus",
    "reasons_from",
]
