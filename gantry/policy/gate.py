"""Where agent access is decided.

Every path that can put Dataset content in front of an agent goes through
`AccessGate.authorize`, and it either returns a decision or raises. That
placement is the whole design: the constraint is in the deterministic path, so
there is no prompt to talk around, no tool description to reinterpret, and no
model that can decide today is an exception.

The gate returns *what may be returned*, not merely yes or no. A decision that
says "permitted, with these fields masked and at most this many rows" is what
lets the caller comply by construction rather than by remembering to.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from gantry.core.dataset import AgentAccessPolicy, DatasetManifest
from gantry.core.sizes import format_byte_size
from gantry.policy.ladder import AccessRung
from gantry.policy.rules import AccessRules, Decision, PiiMode, most_restrictive


class AccessDeniedError(Exception):
    """An agent asked for something policy does not permit.

    Carries the decision so a caller - or an agent reading the error - can see
    which rule refused and what would have been allowed instead. A denial that
    does not say why teaches nothing and invites retrying at random.
    """

    def __init__(self, decision: AccessDecision) -> None:
        super().__init__(decision.describe())
        self.decision = decision


@dataclass(frozen=True)
class AccessRequest:
    """What an agent is asking to do."""

    rung: AccessRung
    dataset: DatasetManifest
    # The fields the caller will actually receive. Empty means "whatever the
    # rung returns by default", which is treated as possibly everything.
    fields: tuple[str, ...] = ()
    reason: str | None = None
    rows_requested: int | None = None
    estimated_bytes: int | None = None
    # Smallest group the caller's aggregate can return, where it knows.
    group_size: int | None = None


@dataclass(frozen=True)
class AccessDecision:
    """What policy permits for one request."""

    request: AccessRequest
    decision: Decision
    redacted_fields: tuple[str, ...] = ()
    row_limit: int | None = None
    # Every rule that had something to say, in the order it was applied.
    grounds: tuple[str, ...] = field(default_factory=tuple)

    @property
    def permitted(self) -> bool:
        return self.decision is not Decision.DENY

    @property
    def dataset(self) -> str:
        return self.request.dataset.name

    @property
    def rung(self) -> AccessRung:
        return self.request.rung

    def describe(self) -> str:
        head = f"{self.rung.describe()} on {self.dataset}: {self.decision.value}"
        if self.redacted_fields:
            head += f" (masking {', '.join(self.redacted_fields)})"
        # A row limit on a refusal reads as an offer. Only say it when rows
        # are actually coming back.
        if self.permitted and self.row_limit is not None:
            head += f" (at most {self.row_limit:,} rows)"
        return head + (f" - {'; '.join(self.grounds)}" if self.grounds else "")


# What a Dataset's own stance permits at each rung. A Dataset may tighten the
# global policy and never loosen it, so this is a ceiling, not an override.
_CEILINGS: dict[AgentAccessPolicy, dict[bool, Decision]] = {
    AgentAccessPolicy.ALLOW: {False: Decision.ALLOW, True: Decision.ALLOW},
    # Aggregates freely; records only masked, and never in full.
    AgentAccessPolicy.AGGREGATE_OR_MASKED: {False: Decision.ALLOW, True: Decision.REDACT},
    AgentAccessPolicy.DENY: {False: Decision.DENY, True: Decision.DENY},
}


class AccessGate:
    """Applies the policy to one request at a time."""

    def __init__(self, rules: AccessRules | None = None) -> None:
        self._rules = rules or AccessRules()

    @property
    def rules(self) -> AccessRules:
        return self._rules

    def evaluate(self, request: AccessRequest) -> AccessDecision:
        """Decide, without raising. Use `authorize` to enforce."""
        rules = self._rules
        rung = request.rung
        grounds: list[str] = []

        decision = rules.for_class(rows=rung.returns_rows, values=rung.returns_values)
        if decision is Decision.DENY:
            grounds.append(f"policy default denies {_class_of(rung)}")

        stance = request.dataset.access.agent_policy
        ceiling = _CEILINGS[stance][rung.returns_rows]
        if _tightens(ceiling, decision):
            grounds.append(f"dataset policy is {stance.value}")
        decision = most_restrictive(decision, ceiling)

        redacted = _sensitive_in_play(request)
        if redacted and rung.returns_values:
            if rules.pii.mode is PiiMode.DENY:
                grounds.append(f"pii mode is deny and {', '.join(redacted)} is sensitive")
                decision = Decision.DENY
            elif rules.pii.mode is PiiMode.REDACT:
                if decision is Decision.ALLOW:
                    grounds.append(f"masking sensitive {', '.join(redacted)}")
                decision = most_restrictive(decision, Decision.REDACT)
            else:
                redacted = ()

        row_limit = self._row_limit(request, grounds)
        decision = most_restrictive(decision, self._sample_rules(request, grounds))
        decision = most_restrictive(decision, self._query_rules(request, grounds))

        return AccessDecision(
            request=request,
            decision=decision,
            redacted_fields=redacted if decision is not Decision.ALLOW else (),
            row_limit=row_limit,
            grounds=tuple(grounds),
        )

    def authorize(self, request: AccessRequest) -> AccessDecision:
        """Decide, and raise if the answer is no."""
        decision = self.evaluate(request)
        if not decision.permitted:
            raise AccessDeniedError(decision)
        return decision

    def _row_limit(self, request: AccessRequest, grounds: list[str]) -> int | None:
        if request.rung is not AccessRung.SAMPLE:
            return None
        allowed = self._rules.samples.max_rows
        asked = request.rows_requested
        if asked is not None and asked > allowed:
            grounds.append(f"sample capped at {allowed} rows (asked for {asked:,})")
        return allowed if asked is None else min(asked, allowed)

    def _sample_rules(self, request: AccessRequest, grounds: list[str]) -> Decision:
        if request.rung is not AccessRung.SAMPLE:
            return Decision.ALLOW
        rules = self._rules.samples
        if rules.max_rows == 0:
            grounds.append("sample policy permits no rows")
            return Decision.DENY
        if rules.require_reason and not (request.reason or "").strip():
            grounds.append("sample policy requires a stated reason")
            return Decision.DENY
        return Decision.ALLOW

    def _query_rules(self, request: AccessRequest, grounds: list[str]) -> Decision:
        if request.rung not in (AccessRung.QUERY, AccessRung.PARTITION):
            return Decision.ALLOW

        rules = self._rules.queries
        budget = rules.max_bytes_scanned
        estimate = request.estimated_bytes
        if budget is not None and estimate is not None and estimate > budget:
            grounds.append(
                f"estimated {format_byte_size(estimate)} scanned, "
                f"over the {format_byte_size(budget)} budget"
            )
            return Decision.DENY

        smallest = request.group_size
        if rules.min_group_size > 1 and smallest is not None and smallest < rules.min_group_size:
            grounds.append(
                f"smallest group is {smallest}, under the minimum of {rules.min_group_size}"
            )
            return Decision.DENY
        return Decision.ALLOW


def _class_of(rung: AccessRung) -> str:
    if rung.returns_rows:
        return "row access"
    return "aggregates" if rung.returns_values else "metadata"


def _tightens(ceiling: Decision, current: Decision) -> bool:
    return most_restrictive(ceiling, current) is ceiling and ceiling is not current


def _sensitive_in_play(request: AccessRequest) -> tuple[str, ...]:
    """Which declared-sensitive fields this request would return.

    With no field list the request gets whatever the rung returns, so every
    sensitive field is in play. Assuming the narrower thing would let an
    unspecified request slip past redaction.
    """
    sensitive = set(request.dataset.sensitive_fields)
    if not sensitive:
        return ()
    if not request.fields:
        return tuple(sorted(sensitive))
    return tuple(sorted(sensitive & set(request.fields)))
