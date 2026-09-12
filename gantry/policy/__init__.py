# SPDX-License-Identifier: Apache-2.0
"""Policy: may this actor perform this proposed operation on these resources?

Policy controls authority, and it runs before anything external happens. It is
trusted configuration written by the application — never by the agent, and never
through a tool argument.

Two layers live here. `PolicyRequirements` is the capability contract an adapter
must be able to honour for work to be admitted at all. `Policy` is the reusable
rule set: `allow` and `deny` rules over actors, operations, engines, sources,
destinations and environments, evaluated against a normalized `PolicyRequest`
that provider inspection builds from the proposal.
"""

from gantry.policy import allow, deny
from gantry.policy.decision import PolicyDecision, PolicyReason, PolicyReasonCode
from gantry.policy.errors import PolicyConfigurationError
from gantry.policy.evaluator import evaluate
from gantry.policy.model import CONSTRAINTS, Effect, Policy, PolicyRule
from gantry.policy.patterns import matches, normalize_pattern
from gantry.policy.request import PolicyRequest
from gantry.policy.requirements import PolicyRequirements

__all__ = [
    "CONSTRAINTS",
    "Effect",
    "Policy",
    "PolicyConfigurationError",
    "PolicyDecision",
    "PolicyReason",
    "PolicyReasonCode",
    "PolicyRequest",
    "PolicyRequirements",
    "PolicyRule",
    "allow",
    "deny",
    "evaluate",
    "matches",
    "normalize_pattern",
]
