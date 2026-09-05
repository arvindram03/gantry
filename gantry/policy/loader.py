# SPDX-License-Identifier: Apache-2.0
"""Loading an agent access policy from YAML.

The RFC writes the policy as a standalone block rather than a resource:

    agentAccess:
      default: {rows: deny, aggregates: allow, metadata: allow}
      samples: {maxRows: 50, requireReason: true}

So this loads a policy *file*, not a `kind:` document. A policy is operational
configuration - it changes when an operator's appetite changes, not when a
Dataset does - and giving it a resource kind would imply it versions alongside
the things it governs.

The one rule worth stating: a file that fails to parse does not fall back to
the defaults. A permissive policy silently substituted for an unreadable strict
one is the worst possible failure here.
"""

from __future__ import annotations

from pathlib import Path

from gantry.policy.rules import AccessRules
from gantry.spec.yaml_compat import safe_load

POLICY_KEY = "agentAccess"


class PolicyError(ValueError):
    """A policy file that could not be read as one."""


def parse_access_rules(document: object, *, source: str = "<policy>") -> AccessRules:
    if not isinstance(document, dict):
        raise PolicyError(f"{source}: a policy file must be a mapping")

    block = document.get(POLICY_KEY, document)
    if not isinstance(block, dict):
        raise PolicyError(f"{source}: {POLICY_KEY} must be a mapping")

    try:
        return AccessRules.model_validate(block)
    except ValueError as error:
        raise PolicyError(f"{source}: {error}") from error


def load_access_rules(path: str | Path) -> AccessRules:
    """Read a policy file. Never falls back to the defaults on failure."""
    location = Path(path)
    try:
        text = location.read_text()
    except OSError as error:
        raise PolicyError(f"{location}: {error}") from error
    return parse_access_rules(safe_load(text), source=str(location))
