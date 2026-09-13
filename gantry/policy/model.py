# SPDX-License-Identifier: Apache-2.0
"""What a policy is: a named, versioned list of allow and deny rules."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from hashlib import sha256

from gantry.confirmation.status import ConfirmationReasonCode
from gantry.policy.errors import PolicyConfigurationError
from gantry.policy.patterns import normalize_pattern
from gantry.runs.model import OperationKind


class Effect(StrEnum):
    """Allow or deny. There is no third answer and no priority number."""

    ALLOW = "allow"
    DENY = "deny"


#: Constraints a rule may bound, and the unit each is measured in. A rule sets a
#: ceiling; it never raises what the operation already configured, so the
#: effective bound is always the smaller of the two.
CONSTRAINTS: Mapping[str, type] = {
    "max_rows": int,
    "max_documents": int,
    "max_bytes_scanned": int,
    "max_cost_usd": float,
    "timeout_seconds": float,
}


def _names(values: Sequence[str] | None, field_name: str) -> tuple[str, ...] | None:
    if values is None:
        return None
    if isinstance(values, str):
        raise PolicyConfigurationError(f"{field_name} must be a list of names, not a string")
    cleaned = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise PolicyConfigurationError(f"{field_name} must contain non-empty strings")
        cleaned.append(value.strip().lower())
    return tuple(dict.fromkeys(cleaned))


def _patterns(values: Sequence[str] | None, field_name: str) -> tuple[str, ...] | None:
    if values is None:
        return None
    if isinstance(values, str):
        raise PolicyConfigurationError(f"{field_name} must be a list of patterns, not a string")
    return tuple(dict.fromkeys(normalize_pattern(value) for value in values))


@dataclass(frozen=True, slots=True)
class PolicyRule:
    """One rule. Every dimension left as `None` is unconstrained, except one.

    `destinations` is the exception, and the asymmetry is deliberate: an allow
    rule that does not name destinations authorizes no writes at all. Reading
    the wrong table is a leak; writing the wrong table destroys something, so
    write authority is never granted by omission. `deny` rules keep the plain
    reading — a dimension left out simply does not narrow the rule.

    `constraints` are ceilings the request must already be under. A rule cannot
    raise a limit the operation configured, so trusted constraints only ever
    compose toward less authority.

    `require_confirmation` does not change the answer — the rule still allows —
    it asks the host to tell the user before the work happens. Authority and
    "should someone be told" are separate questions, and collapsing them would
    turn every sensitive operation into a refusal.
    """

    effect: Effect
    name: str | None = None
    require_confirmation: bool = False
    confirmation_code: ConfirmationReasonCode | str | None = None
    confirmation_message: str | None = None
    actors: tuple[str, ...] | None = None
    operations: tuple[OperationKind, ...] | None = None
    engines: tuple[str, ...] | None = None
    sources: tuple[str, ...] | None = None
    destinations: tuple[str, ...] | None = None
    environments: tuple[str, ...] | None = None
    constraints: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "effect", _effect(self.effect))
        if self.name is not None and not self.name.strip():
            raise PolicyConfigurationError("rule name must not be empty when given")
        object.__setattr__(self, "actors", _names(self.actors, "actors"))
        object.__setattr__(self, "engines", _names(self.engines, "engines"))
        object.__setattr__(self, "environments", _names(self.environments, "environments"))
        object.__setattr__(self, "sources", _patterns(self.sources, "sources"))
        object.__setattr__(self, "destinations", _patterns(self.destinations, "destinations"))
        object.__setattr__(self, "operations", _operations(self.operations))
        object.__setattr__(self, "constraints", _constraints(self.constraints))
        self._check_confirmation()

    def _check_confirmation(self) -> None:
        """A confirmation prompt nobody will ever see is a mistake, not a preference."""
        if self.confirmation_code is not None:
            try:
                object.__setattr__(
                    self, "confirmation_code", ConfirmationReasonCode(self.confirmation_code)
                )
            except ValueError as error:
                known = ", ".join(code.value for code in ConfirmationReasonCode)
                raise PolicyConfigurationError(
                    f"unknown confirmation code: {self.confirmation_code!r}; v0 has {known}"
                ) from error
        if self.confirmation_message is not None and not self.confirmation_message.strip():
            raise PolicyConfigurationError("confirmation message must not be empty when given")
        if self.require_confirmation and self.effect is Effect.DENY:
            raise PolicyConfigurationError(
                "a deny rule cannot require confirmation: nothing it matches will run"
            )
        if not self.require_confirmation and (
            self.confirmation_code is not None or self.confirmation_message is not None
        ):
            raise PolicyConfigurationError(
                "confirmation_code and confirmation_message need require_confirmation=True, "
                "or nothing will ever show them"
            )

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"effect": self.effect.value}
        if self.name is not None:
            payload["name"] = self.name
        if self.require_confirmation:
            payload["require_confirmation"] = True
        if self.confirmation_code is not None:
            payload["confirmation_code"] = str(self.confirmation_code)
        if self.confirmation_message is not None:
            payload["confirmation_message"] = self.confirmation_message
        for key in ("actors", "engines", "sources", "destinations", "environments"):
            value = getattr(self, key)
            if value is not None:
                payload[key] = list(value)
        if self.operations is not None:
            payload["operations"] = [operation.value for operation in self.operations]
        if self.constraints:
            payload["constraints"] = dict(sorted(self.constraints.items()))
        return payload


def _effect(effect: Effect | str) -> Effect:
    try:
        return Effect(effect)
    except ValueError as error:
        raise PolicyConfigurationError(f"unknown policy effect: {effect!r}") from error


def _operations(values: Sequence[OperationKind | str] | None) -> tuple[OperationKind, ...] | None:
    if values is None:
        return None
    if isinstance(values, str):
        raise PolicyConfigurationError("operations must be a list of names, not a string")
    operations = []
    for value in values:
        try:
            operations.append(OperationKind(value))
        except ValueError as error:
            known = ", ".join(sorted(kind.value for kind in OperationKind))
            raise PolicyConfigurationError(
                f"unknown operation: {value!r}; policy operations are {known}"
            ) from error
    return tuple(dict.fromkeys(operations))


def _constraints(values: Mapping[str, float] | None) -> Mapping[str, float]:
    if not values:
        return {}
    bounded: dict[str, float] = {}
    for key, value in values.items():
        if key not in CONSTRAINTS:
            known = ", ".join(sorted(CONSTRAINTS))
            raise PolicyConfigurationError(f"unknown constraint: {key!r}; policy bounds {known}")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PolicyConfigurationError(f"constraint {key} must be numeric, not {value!r}")
        if value <= 0:
            raise PolicyConfigurationError(f"constraint {key} must be positive, not {value!r}")
        bounded[key] = CONSTRAINTS[key](value)
    return bounded


@dataclass(frozen=True, slots=True)
class Policy:
    """A named set of rules, and the hash of exactly those rules.

    The version is derived from the rules rather than declared beside them, so
    a run that records `sha256:…` records the configuration that actually
    decided it. Two policies that differ anywhere hash differently; the same
    rules written in the same order hash the same in any process.
    """

    name: str
    rules: tuple[PolicyRule, ...] = ()

    def __init__(self, name: str, rules: Sequence[PolicyRule] = ()) -> None:
        if not isinstance(name, str) or not name.strip():
            raise PolicyConfigurationError("policy name must be a non-empty string")
        if isinstance(rules, PolicyRule):
            raise PolicyConfigurationError("rules must be a list of rules, not one rule")
        materialized = tuple(rules)
        for rule in materialized:
            if not isinstance(rule, PolicyRule):
                raise PolicyConfigurationError(f"rules must be PolicyRule objects, got {rule!r}")
        object.__setattr__(self, "name", name.strip())
        object.__setattr__(self, "rules", tuple(_named(materialized)))

    @property
    def version(self) -> str:
        """`sha256:…` over the canonical form of the name and rules."""
        canonical = json.dumps(
            {"name": self.name, "rules": [rule.as_dict() for rule in self.rules]},
            sort_keys=True,
            separators=(",", ":"),
        )
        return f"sha256:{sha256(canonical.encode()).hexdigest()}"

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "version": self.version,
            "rules": [rule.as_dict() for rule in self.rules],
        }


def _named(rules: Sequence[PolicyRule]) -> list[PolicyRule]:
    """Give every rule a name, because a decision has to say which rule it was.

    An explicit name survives reordering and is what an operator recognizes;
    the generated fallback at least says what the rule was and where it sat.
    """
    from dataclasses import replace

    named: list[PolicyRule] = []
    seen: set[str] = set()
    for index, rule in enumerate(rules):
        if rule.name is not None:
            if rule.name in seen:
                raise PolicyConfigurationError(f"duplicate rule name: {rule.name!r}")
            seen.add(rule.name)
            named.append(rule)
            continue
        scope = "any" if rule.operations is None else "-".join(o.value for o in rule.operations)
        named.append(replace(rule, name=f"{rule.effect.value}-{scope}-{index}"))
    return named
