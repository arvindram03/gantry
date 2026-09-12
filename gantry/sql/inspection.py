# SPDX-License-Identifier: Apache-2.0
"""Turning SQL into the normalized request policy decides on.

The agent does not get to say what its SQL touches. The dialect classifier does,
from the statement itself, and anything it cannot settle — an unclassifiable
statement, a batch whose write targets are not separable — is reported as
unresolved so that policy fails closed instead of authorizing an effect nobody
named.
"""

from __future__ import annotations

from collections.abc import Sequence

from gantry.actor import current_actor, current_environment
from gantry.policy.request import PolicyRequest
from gantry.runs.model import OperationKind, ResourceRef
from gantry.sql.classification import SQLClassification, SQLObjectRef, SQLOperation
from gantry.sql.dialect import SQLDialect
from gantry.sql.policy import SQLPolicy


def constraints(policy: SQLPolicy) -> dict[str, float]:
    """What the operation already bounds, for rules that require a ceiling."""
    values: dict[str, float] = {
        "max_rows": policy.max_rows,
        "timeout_seconds": policy.timeout_seconds,
    }
    if policy.max_bytes_scanned is not None:
        values["max_bytes_scanned"] = policy.max_bytes_scanned
    if policy.max_cost_usd is not None:
        values["max_cost_usd"] = policy.max_cost_usd
    return values


def _refs(provider: str, names: Sequence[str]) -> tuple[ResourceRef, ...]:
    return tuple(ResourceRef(system=provider, resource=name) for name in dict.fromkeys(names))


def _objects(provider: str, objects: Sequence[SQLObjectRef]) -> tuple[ResourceRef, ...]:
    return _refs(provider, [reference.qualified_name for reference in objects])


def query_request(
    sql: str,
    *,
    dialect: SQLDialect,
    provider: str,
    policy: SQLPolicy,
) -> PolicyRequest:
    """The policy request for one submitted query.

    A statement that writes is still a query operation — the operation is how
    the caller configured it, not what the SQL turned out to do — so its write
    targets travel as outputs and have to be authorized as writes.
    """
    try:
        classification = dialect.classify(sql)
    except Exception as error:  # pragma: no cover - a dialect that raises is a dialect bug
        return _unresolved(
            provider, OperationKind.QUERY, policy, f"SQL could not be parsed: {error}"
        )

    unresolved = _unresolvable(classification)
    writes = _objects(provider, classification.write_targets)
    written = {ref.resource for ref in writes}
    reads = tuple(
        ref for ref in _objects(provider, classification.tables) if ref.resource not in written
    )
    return PolicyRequest(
        actor=current_actor(),
        operation=OperationKind.QUERY,
        engine=provider,
        inputs=reads,
        outputs=writes,
        environment=current_environment(),
        constraints=constraints(policy),
        unresolved=unresolved,
    )


def materialize_request(
    *,
    provider: str,
    sources: Sequence[str],
    destination: str | None,
    policy: SQLPolicy,
) -> PolicyRequest:
    """The policy request for one materialization, from its parsed plan.

    Takes already-qualified names rather than parsed objects: the plan's own
    reference type differs from the classifier's, and policy cares about the
    name a provider normalizes to, not about which parser produced it.
    """
    return PolicyRequest(
        actor=current_actor(),
        operation=OperationKind.MATERIALIZE,
        engine=provider,
        inputs=_refs(provider, sources),
        outputs=() if destination is None else _refs(provider, (destination,)),
        environment=current_environment(),
        constraints=constraints(policy),
        unresolved=() if destination is not None else ("no destination could be determined",),
    )


def _unresolvable(classification: SQLClassification) -> tuple[str, ...]:
    if classification.operation is SQLOperation.UNKNOWN:
        return ("the statement could not be classified",)
    if classification.operation is SQLOperation.MULTI_STATEMENT:
        # Several statements share one classification, so which tables are read
        # and which are written is no longer separable. Authorizing the union as
        # reads would authorize the writes among them.
        return ("a multi-statement submission has no separable read and write targets",)
    return ()


def _unresolved(
    provider: str, operation: OperationKind, policy: SQLPolicy, detail: str
) -> PolicyRequest:
    return PolicyRequest(
        actor=current_actor(),
        operation=operation,
        engine=provider,
        environment=current_environment(),
        constraints=constraints(policy),
        unresolved=(detail,),
    )
