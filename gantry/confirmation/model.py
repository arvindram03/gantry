# SPDX-License-Identifier: Apache-2.0
"""What Gantry asks for, and what the host answered.

Confirmation is an indicative control. Gantry records that it asked and that
the host said the user agreed; it does not and cannot claim that a particular
authenticated person approved anything. Every name here is chosen to keep that
distinction visible — there is no `approved_by`, because there is nobody Gantry
could name.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from gantry.confirmation.status import ConfirmationReasonCode, ConfirmationStatus


@dataclass(frozen=True, slots=True)
class ConfirmationReason:
    """One reason the host should ask before this runs."""

    code: ConfirmationReasonCode
    message: str
    rule: str | None = None
    details: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", ConfirmationReasonCode(self.code))
        if not self.message.strip():
            raise ValueError("a confirmation reason must say something to the user")

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"code": self.code.value, "message": self.message}
        if self.rule is not None:
            payload["rule"] = self.rule
        if self.details:
            payload["details"] = dict(self.details)
        return payload


@dataclass(frozen=True, slots=True)
class ConfirmationRequirement:
    """Whether to ask, and everything to say when asking.

    Several rules can each want confirmation; they collapse into one
    requirement carrying several reasons rather than into several prompts. Being
    asked twice about one operation teaches a user to click through.
    """

    required: bool = False
    reasons: tuple[ConfirmationReason, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "reasons", tuple(self.reasons))
        if self.required and not self.reasons:
            raise ValueError("a required confirmation must carry at least one reason")

    @property
    def message(self) -> str:
        """Every reason in one sentence, for a host that wants a single line."""
        return " ".join(reason.message for reason in self.reasons)

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(reason.code.value for reason in self.reasons))

    def as_dict(self) -> dict[str, object]:
        return {
            "required": self.required,
            "reasons": [reason.as_dict() for reason in self.reasons],
        }


NOT_REQUIRED = ConfirmationRequirement()


@dataclass(frozen=True, slots=True)
class ConfirmationRecord:
    """What was asked, what was answered, and which proposal it was about.

    `proposal_hash` is the binding that makes the record mean anything: a
    confirmation is for one immutable proposal, so confirming one statement can
    never license running a different one. A run whose proposal changes needs a
    new run and a fresh confirmation.
    """

    status: ConfirmationStatus
    run_id: str
    proposal_hash: str | None = None
    reasons: tuple[ConfirmationReason, ...] = ()
    required_at: datetime | None = None
    confirmed_at: datetime | None = None
    declined_at: datetime | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", ConfirmationStatus(self.status))
        object.__setattr__(self, "reasons", tuple(self.reasons))
        oversized = [key for key, value in self.metadata.items() if len(str(value)) > 1024]
        if oversized:
            raise ValueError(
                f"confirmation metadata values must stay small; too long: "
                f"{', '.join(sorted(oversized))}"
            )

    @classmethod
    def required(
        cls,
        run_id: str,
        requirement: ConfirmationRequirement,
        *,
        proposal_hash: str | None = None,
    ) -> ConfirmationRecord:
        return cls(
            status=ConfirmationStatus.REQUIRED,
            run_id=run_id,
            proposal_hash=proposal_hash,
            reasons=requirement.reasons,
            required_at=datetime.now(UTC),
        )

    @property
    def message(self) -> str:
        """What to show the user."""
        return " ".join(reason.message for reason in self.reasons)

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(reason.code.value for reason in self.reasons))

    def confirmed(self, *, metadata: Mapping[str, object] | None = None) -> ConfirmationRecord:
        from dataclasses import replace

        return replace(
            self,
            status=ConfirmationStatus.CONFIRMED,
            confirmed_at=datetime.now(UTC),
            metadata={**self.metadata, **(metadata or {})},
        )

    def declined(self, *, metadata: Mapping[str, object] | None = None) -> ConfirmationRecord:
        from dataclasses import replace

        return replace(
            self,
            status=ConfirmationStatus.DECLINED,
            declined_at=datetime.now(UTC),
            metadata={**self.metadata, **(metadata or {})},
        )

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "status": self.status.value,
            "run_id": self.run_id,
            "proposal_hash": self.proposal_hash,
            "reasons": [reason.as_dict() for reason in self.reasons],
            "required_at": None if self.required_at is None else self.required_at.isoformat(),
            "confirmed_at": None if self.confirmed_at is None else self.confirmed_at.isoformat(),
            "declined_at": None if self.declined_at is None else self.declined_at.isoformat(),
        }
        if self.metadata:
            payload["metadata"] = {str(key): value for key, value in self.metadata.items()}
        return payload


def reasons_from(
    entries: Sequence[tuple[str | None, str | None, str | None]],
) -> tuple[ConfirmationReason, ...]:
    """Build reasons from `(rule, code, message)` triples, dropping duplicates.

    Two rules asking for the same thing is one reason, not two: the user is
    being asked about one operation.
    """
    built: dict[tuple[str, str], ConfirmationReason] = {}
    for rule, code, message in entries:
        resolved = ConfirmationReasonCode(code or ConfirmationReasonCode.CUSTOM_POLICY_REQUIREMENT)
        text = message or _default_message(resolved)
        built.setdefault((resolved.value, text), ConfirmationReason(resolved, text, rule=rule))
    return tuple(built.values())


def _default_message(code: ConfirmationReasonCode) -> str:
    return {
        ConfirmationReasonCode.SENSITIVE_DESTINATION: (
            "This operation writes to a destination marked sensitive."
        ),
        ConfirmationReasonCode.PRODUCTION_WRITE: "This operation writes to production data.",
        ConfirmationReasonCode.DESTRUCTIVE_OPERATION: (
            "This operation replaces or removes existing data."
        ),
        ConfirmationReasonCode.HIGH_COST: "This operation may be expensive to run.",
        ConfirmationReasonCode.LONG_RUNNING_JOB: "This operation submits a long-running job.",
        ConfirmationReasonCode.CUSTOM_POLICY_REQUIREMENT: (
            "Policy asks that this operation be confirmed before it runs."
        ),
    }[code]
