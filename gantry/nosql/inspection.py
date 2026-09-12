# SPDX-License-Identifier: Apache-2.0
"""Turning a MongoDB pipeline into the normalized request policy decides on.

The same rule as everywhere else: what the proposal touches is derived, not
declared. `$lookup` pulls in collections the pipeline never names at the top
level, and `$out`/`$merge` write to one — both are policy-relevant, so both are
resolved here rather than inferred from the caller's intent.
"""

from __future__ import annotations

from collections.abc import Sequence

from gantry.actor import current_actor, current_environment
from gantry.nosql.pipeline import CollectionRef, Pipeline, classify_pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.policy.request import PolicyRequest
from gantry.runs.model import OperationKind, ResourceRef


def constraints(policy: NoSQLPolicy) -> dict[str, float]:
    values: dict[str, float] = {
        "max_documents": policy.max_documents,
        "timeout_seconds": policy.timeout_seconds,
    }
    if policy.max_bytes_scanned is not None:
        values["max_bytes_scanned"] = policy.max_bytes_scanned
    if policy.max_cost_usd is not None:
        values["max_cost_usd"] = policy.max_cost_usd
    return values


def qualified(database: str | None, name: str) -> str:
    """`database.collection`, the name a MongoDB resource is known by.

    A name that already carries a database keeps it. Cross-database writes are
    refused before this point, so the only qualified names reaching here are
    ones the caller wrote that way.
    """
    if database is None or "." in name:
        return name
    return f"{database}.{name}"


def _refs(provider: str, database: str | None, names: Sequence[str]) -> tuple[ResourceRef, ...]:
    return tuple(
        ResourceRef(system=provider, resource=qualified(database, name))
        for name in dict.fromkeys(names)
    )


def query_request(
    collection: str,
    pipeline: Pipeline,
    *,
    provider: str,
    database: str | None,
    policy: NoSQLPolicy,
) -> PolicyRequest:
    """The policy request for one pipeline run as a query."""
    try:
        classification = classify_pipeline(collection, pipeline, database=database)
    except (TypeError, ValueError) as error:
        return PolicyRequest(
            actor=current_actor(),
            operation=OperationKind.QUERY,
            engine=provider,
            environment=current_environment(),
            constraints=constraints(policy),
            unresolved=(f"the pipeline could not be classified: {error}",),
        )
    destination = classification.write_destination
    reads = [
        reference.name
        for reference in classification.collections
        if destination is None or reference.name != destination.name
    ]
    return PolicyRequest(
        actor=current_actor(),
        operation=OperationKind.QUERY,
        engine=provider,
        inputs=_refs(provider, database, reads),
        outputs=() if destination is None else _refs(provider, database, (destination.name,)),
        environment=current_environment(),
        constraints=constraints(policy),
    )


def materialize_request(
    *,
    provider: str,
    database: str | None,
    sources: Sequence[CollectionRef],
    destination: CollectionRef | None,
    policy: NoSQLPolicy,
) -> PolicyRequest:
    """The policy request for one `$out`/`$merge` materialization."""
    return PolicyRequest(
        actor=current_actor(),
        operation=OperationKind.MATERIALIZE,
        engine=provider,
        inputs=_refs(provider, database, [reference.name for reference in sources]),
        outputs=() if destination is None else _refs(provider, database, (destination.name,)),
        environment=current_environment(),
        constraints=constraints(policy),
        unresolved=() if destination is not None else ("no destination could be determined",),
    )
