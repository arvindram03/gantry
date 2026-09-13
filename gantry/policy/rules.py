# SPDX-License-Identifier: Apache-2.0
"""Rule constructors. Small functions, not a language.

`gantry.allow.query(...)` and `gantry.deny.materialize(...)` build `PolicyRule`
objects. They exist so the common shapes read like what they mean and so a
mistake — an unknown operation, a malformed pattern, an allow that could never
grant anything — is raised where the policy is written.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from gantry.confirmation.status import ConfirmationReasonCode
from gantry.policy.errors import PolicyConfigurationError
from gantry.policy.model import Effect, PolicyRule
from gantry.runs.model import OperationKind

#: Operations that write somewhere. An allow rule for one of these has to say
#: where, or it grants an authority nobody wrote down.
WRITING = (OperationKind.MATERIALIZE, OperationKind.BATCH, OperationKind.STREAM)


def allow(
    operation: OperationKind,
    *,
    name: str | None = None,
    require_confirmation: bool = False,
    confirmation_code: ConfirmationReasonCode | str | None = None,
    confirmation_message: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    if operation in WRITING and not destinations:
        raise PolicyConfigurationError(
            f"allow.{operation.value}(...) must name destinations; a rule that allows a write "
            "without saying where it may land can never authorize anything"
        )
    return PolicyRule(
        effect=Effect.ALLOW,
        name=name,
        require_confirmation=require_confirmation,
        confirmation_code=confirmation_code,
        confirmation_message=confirmation_message,
        actors=None if actors is None else tuple(actors),
        operations=(operation,),
        engines=None if engines is None else tuple(engines),
        sources=None if sources is None else tuple(sources),
        destinations=None if destinations is None else tuple(destinations),
        environments=None if environments is None else tuple(environments),
        constraints=dict(constraints or {}),
    )


def deny(
    operation: OperationKind | None = None,
    *,
    name: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    return PolicyRule(
        effect=Effect.DENY,
        name=name,
        actors=None if actors is None else tuple(actors),
        operations=None if operation is None else (operation,),
        engines=None if engines is None else tuple(engines),
        sources=None if sources is None else tuple(sources),
        destinations=None if destinations is None else tuple(destinations),
        environments=None if environments is None else tuple(environments),
        constraints=dict(constraints or {}),
    )
