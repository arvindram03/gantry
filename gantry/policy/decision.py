# SPDX-License-Identifier: Apache-2.0
"""The structured answer, and the codes that explain it."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from gantry.confirmation.model import NOT_REQUIRED, ConfirmationRequirement


class PolicyReasonCode(StrEnum):
    """Why a decision came out the way it did, in machine-readable form.

    A denial that says only "permission denied" cannot be acted on by the agent
    that hit it, triaged by the operator who reads it, or counted by anyone
    asking which rule keeps firing.
    """

    NO_MATCHING_ALLOW = "NO_MATCHING_ALLOW"
    EXPLICIT_DENY = "EXPLICIT_DENY"
    ACTOR_DENIED = "ACTOR_DENIED"
    OPERATION_DENIED = "OPERATION_DENIED"
    SOURCE_DENIED = "SOURCE_DENIED"
    DESTINATION_DENIED = "DESTINATION_DENIED"
    ENVIRONMENT_DENIED = "ENVIRONMENT_DENIED"
    CONSTRAINT_EXCEEDED = "CONSTRAINT_EXCEEDED"
    RESOURCE_UNRESOLVED = "RESOURCE_UNRESOLVED"
    POLICY_INVALID = "POLICY_INVALID"


@dataclass(frozen=True, slots=True)
class PolicyReason:
    """One reason, tied to the resource and rule it came from."""

    code: PolicyReasonCode
    resource: str | None = None
    rule: str | None = None
    message: str | None = None

    def __str__(self) -> str:
        if self.message:
            return self.message
        parts = [self.code.value]
        if self.resource:
            parts.append(self.resource)
        if self.rule:
            parts.append(f"rule {self.rule}")
        return ": ".join(parts)

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code.value,
            "resource": self.resource,
            "rule": self.rule,
            "message": str(self),
        }


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Whether the proposal is authorized, under which policy, and why.

    Carried onto the run whether it allowed or refused. An allow that records
    nothing is indistinguishable later from an operation nobody checked.

    `confirmation` is a second, independent answer: the operation is allowed,
    and the host should tell the user before it happens. A denial never carries
    one — there is nothing to confirm about work that will not run.
    """

    allowed: bool
    policy: str
    policy_version: str
    matched_rules: tuple[str, ...] = ()
    reasons: tuple[PolicyReason, ...] = ()
    confirmation: ConfirmationRequirement = NOT_REQUIRED
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(reason.code.value for reason in self.reasons))

    def messages(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(str(reason) for reason in self.reasons))

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed": self.allowed,
            "policy": self.policy,
            "policy_version": self.policy_version,
            "matched_rules": list(self.matched_rules),
            "reasons": [reason.as_dict() for reason in self.reasons],
            "confirmation": self.confirmation.as_dict(),
            "evaluated_at": self.evaluated_at.isoformat(),
        }


def denial(
    policy: str,
    version: str,
    reasons: Sequence[PolicyReason],
    *,
    matched_rules: Sequence[str] = (),
) -> PolicyDecision:
    return PolicyDecision(
        allowed=False,
        policy=policy,
        policy_version=version,
        matched_rules=tuple(matched_rules),
        reasons=tuple(reasons),
    )
