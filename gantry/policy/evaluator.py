# SPDX-License-Identifier: Apache-2.0
"""Deciding a normalized request against a policy.

Three properties matter more than any cleverness here: an explicit deny always
wins, an explicit policy denies what it does not mention, and every resource the
proposal touches has to be authorized on its own. Anything the evaluator cannot
work out is a denial, not a pass.
"""

from __future__ import annotations

from collections.abc import Sequence

from gantry.policy.decision import PolicyDecision, PolicyReason, PolicyReasonCode, denial
from gantry.policy.model import Effect, Policy, PolicyRule
from gantry.policy.patterns import matches_any
from gantry.policy.request import PolicyRequest
from gantry.runs.model import ResourceRef

_DIMENSION_CODES = {
    "actors": PolicyReasonCode.ACTOR_DENIED,
    "operations": PolicyReasonCode.OPERATION_DENIED,
    "environments": PolicyReasonCode.ENVIRONMENT_DENIED,
    "constraints": PolicyReasonCode.CONSTRAINT_EXCEEDED,
}


def evaluate(policy: Policy, request: PolicyRequest) -> PolicyDecision:
    """Decide one request. Never raises: a broken rule denies rather than escapes.

    Evaluation runs before anything external happens, so an exception here would
    surface as a crash on a path whose whole job is to be the gate. A rule that
    cannot be evaluated is treated as a policy that cannot be trusted, which is
    a denial.
    """
    version = policy.version
    try:
        return _evaluate(policy, request, version)
    except Exception as error:  # pragma: no cover - defensive, see the docstring
        return denial(
            policy.name,
            version,
            (
                PolicyReason(
                    PolicyReasonCode.POLICY_INVALID,
                    message=f"policy could not be evaluated: {error}",
                ),
            ),
        )


def _evaluate(policy: Policy, request: PolicyRequest, version: str) -> PolicyDecision:
    if request.unresolved:
        return denial(
            policy.name,
            version,
            [
                PolicyReason(
                    PolicyReasonCode.RESOURCE_UNRESOLVED,
                    message=f"policy-relevant effect could not be determined: {detail}",
                )
                for detail in request.unresolved
            ],
        )

    allows = [rule for rule in policy.rules if rule.effect is Effect.ALLOW]
    denies = [rule for rule in policy.rules if rule.effect is Effect.DENY]

    refusals = _explicit_denials(denies, request)
    if refusals:
        return denial(
            policy.name,
            version,
            [reason for reason, _ in refusals],
            matched_rules=tuple(dict.fromkeys(rule for _, rule in refusals)),
        )

    applicable = [rule for rule in allows if not _blocking_dimension(rule, request)]
    if not applicable:
        return denial(policy.name, version, _no_allow_reasons(allows, request))

    matched: list[str] = []
    reasons: list[PolicyReason] = []
    writers = [rule for rule in applicable if _accounts_for_reads(rule, request)]
    for ref in request.inputs:
        rule = _covers(applicable, ref, write=False)
        if rule is None:
            reasons.append(
                PolicyReason(
                    PolicyReasonCode.NO_MATCHING_ALLOW,
                    resource=ref.resource,
                    message=f"no rule allows reading {ref.resource}",
                )
            )
        else:
            matched.append(str(rule.name))
    for ref in request.outputs:
        rule = _covers(writers, ref, write=True)
        if rule is None:
            reasons.append(
                PolicyReason(
                    PolicyReasonCode.NO_MATCHING_ALLOW,
                    resource=ref.resource,
                    message=f"no rule allows writing {ref.resource}",
                )
            )
        else:
            matched.append(str(rule.name))

    if reasons:
        return denial(policy.name, version, reasons, matched_rules=tuple(dict.fromkeys(matched)))
    if not request.inputs and not request.outputs:
        # Nothing to authorize per-resource, so authority rests on the rule
        # applying at all — an operation with no resolved resources still has
        # an actor, an operation kind and an environment to match.
        matched.extend(str(rule.name) for rule in applicable)
    return PolicyDecision(
        allowed=True,
        policy=policy.name,
        policy_version=version,
        matched_rules=tuple(dict.fromkeys(matched)),
    )


def _accounts_for_reads(rule: PolicyRule, request: PolicyRequest) -> bool:
    """May this rule grant a write, given everything the proposal reads?

    Reads compose across rules: two rules each naming one schema together
    authorize a join over both. Writes do not. A rule grants write authority only
    if its own sources cover every input, so two rules that separately allow
    `raw.* -> scratch_a.*` and `other.* -> scratch_b.*` never combine into
    permission to move `raw` data into `scratch_b`. Composition has to narrow
    authority; a cross-product would widen it.
    """
    if rule.sources is None:
        return True
    return all(matches_any(rule.sources, ref.resource) for ref in request.inputs)


def _covers(rules: Sequence[PolicyRule], ref: ResourceRef, *, write: bool) -> PolicyRule | None:
    """The first applicable rule that authorizes this one resource.

    A rule that does not name `sources` reads anything. A rule that does not
    name `destinations` writes nothing: write authority is granted explicitly
    or not at all.
    """
    for rule in rules:
        patterns = rule.destinations if write else rule.sources
        if patterns is None:
            if write:
                continue
            return rule
        if matches_any(patterns, ref.resource):
            return rule
    return None


def _explicit_denials(
    rules: Sequence[PolicyRule], request: PolicyRequest
) -> list[tuple[PolicyReason, str]]:
    """Every deny rule that fires, and what it fired on.

    A deny rule narrows on each dimension it names: it fires when the actor,
    operation, engine and environment all match and at least one touched
    resource falls inside each resource dimension it named. Naming a source and
    a destination therefore denies that pairing rather than either half of it.
    """
    refusals: list[tuple[PolicyReason, str]] = []
    for rule in rules:
        if _blocking_dimension(rule, request) is not None:
            continue
        sources = _touched(rule.sources, request.inputs)
        destinations = _touched(rule.destinations, request.outputs)
        if rule.sources is not None and not sources:
            continue
        if rule.destinations is not None and not destinations:
            continue
        name = str(rule.name)
        for ref in destinations:
            refusals.append(
                (
                    PolicyReason(
                        PolicyReasonCode.DESTINATION_DENIED,
                        resource=ref,
                        rule=name,
                        message=f"writing {ref} is denied by rule {name}",
                    ),
                    name,
                )
            )
        for ref in sources:
            refusals.append(
                (
                    PolicyReason(
                        PolicyReasonCode.SOURCE_DENIED,
                        resource=ref,
                        rule=name,
                        message=f"reading {ref} is denied by rule {name}",
                    ),
                    name,
                )
            )
        if not sources and not destinations:
            refusals.append((_identity_denial(rule, request, name), name))
    return refusals


def _identity_denial(rule: PolicyRule, request: PolicyRequest, name: str) -> PolicyReason:
    """A deny rule that named no resources still denied something specific."""
    if rule.actors is not None:
        return PolicyReason(
            PolicyReasonCode.ACTOR_DENIED,
            resource=request.actor.label,
            rule=name,
            message=f"{request.actor.label} is denied by rule {name}",
        )
    if rule.environments is not None:
        return PolicyReason(
            PolicyReasonCode.ENVIRONMENT_DENIED,
            resource=request.environment,
            rule=name,
            message=f"environment {request.environment} is denied by rule {name}",
        )
    if rule.operations is not None:
        return PolicyReason(
            PolicyReasonCode.OPERATION_DENIED,
            resource=request.operation.value,
            rule=name,
            message=f"{request.operation.value} is denied by rule {name}",
        )
    return PolicyReason(
        PolicyReasonCode.EXPLICIT_DENY,
        rule=name,
        message=f"denied by rule {name}",
    )


def _touched(patterns: tuple[str, ...] | None, refs: Sequence[ResourceRef]) -> tuple[str, ...]:
    if patterns is None:
        return ()
    return tuple(ref.resource for ref in refs if matches_any(patterns, ref.resource))


def _blocking_dimension(rule: PolicyRule, request: PolicyRequest) -> str | None:
    """The first non-resource dimension that stops this rule applying.

    Shared by both effects: a rule that does not apply cannot allow, and cannot
    deny either.
    """
    if rule.actors is not None and not _actor_matches(rule.actors, request):
        return "actors"
    if rule.operations is not None and request.operation not in rule.operations:
        return "operations"
    if rule.engines is not None and request.engine.strip().lower() not in rule.engines:
        return "engines"
    if rule.environments is not None and (
        request.environment is None or request.environment not in rule.environments
    ):
        return "environments"
    if rule.constraints and _exceeded(rule, request):
        return "constraints"
    return None


def _actor_matches(actors: tuple[str, ...], request: PolicyRequest) -> bool:
    """Match the actor's id, or its full `type:id` label.

    Both spellings are accepted because both are how people name actors:
    `research-agent` in a small deployment, `agent:research-agent` where a user
    and an agent could share an id.
    """
    identity = request.actor.id
    candidates = {request.actor.label.lower()}
    if identity is not None:
        candidates.add(identity.strip().lower())
    return bool(candidates & set(actors))


def _exceeded(rule: PolicyRule, request: PolicyRequest) -> bool:
    """Is the request outside a ceiling this rule sets?

    An unconfigured limit counts as exceeding a rule that sets one: a rule that
    bounds rows cannot apply to an operation that never bounded them, because
    nothing would hold the bound.
    """
    for key, ceiling in rule.constraints.items():
        configured = request.constraints.get(key)
        if configured is None or configured > ceiling:
            return True
    return False


def _no_allow_reasons(rules: Sequence[PolicyRule], request: PolicyRequest) -> list[PolicyReason]:
    """Why nothing could allow this, named as precisely as it can be.

    When every allow rule fell at the same hurdle, that hurdle is the reason —
    an operator reading `ACTOR_DENIED` learns more than one reading that no rule
    matched. When they fell at different ones, the honest answer is that none
    matched.
    """
    blocking = {_blocking_dimension(rule, request) for rule in rules}
    if len(blocking) == 1 and rules:
        dimension = blocking.pop()
        code = _DIMENSION_CODES.get(str(dimension))
        if code is not None:
            return [PolicyReason(code, message=_dimension_message(str(dimension), request))]
    return [
        PolicyReason(
            PolicyReasonCode.NO_MATCHING_ALLOW,
            message=f"no rule allows {request.operation.value} for {request.actor.label}",
        )
    ]


def _dimension_message(dimension: str, request: PolicyRequest) -> str:
    if dimension == "actors":
        return f"no rule allows {request.actor.label}"
    if dimension == "operations":
        return f"no rule allows {request.operation.value}"
    if dimension == "environments":
        return f"no rule allows environment {request.environment}"
    return "the operation's configured limits exceed every rule's ceiling"
