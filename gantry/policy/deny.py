# SPDX-License-Identifier: Apache-2.0
"""Rules that refuse: `gantry.deny.materialize(destinations=["prod.*"])`.

A deny rule wins over every allow. It narrows on each dimension it names, so
naming a source and a destination denies that pairing rather than either half of
it, and naming nothing at all denies the operation outright.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from gantry.policy.model import PolicyRule
from gantry.policy.rules import deny as _deny
from gantry.runs.model import OperationKind


def query(
    *,
    name: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Deny queries matching this rule."""
    return _deny(
        OperationKind.QUERY,
        name=name,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


def materialize(
    *,
    destinations: Sequence[str] | None = None,
    name: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Deny materializations matching this rule."""
    return _deny(
        OperationKind.MATERIALIZE,
        name=name,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


def batch(
    *,
    destinations: Sequence[str] | None = None,
    name: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Deny batch jobs matching this rule."""
    return _deny(
        OperationKind.BATCH,
        name=name,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


def stream(
    *,
    destinations: Sequence[str] | None = None,
    name: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Deny streaming submissions matching this rule."""
    return _deny(
        OperationKind.STREAM,
        name=name,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


def anything(
    *,
    name: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Deny across every operation — `gantry.deny.anything(environments=["prod"])`."""
    return _deny(
        None,
        name=name,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


__all__ = ["anything", "batch", "materialize", "query", "stream"]
