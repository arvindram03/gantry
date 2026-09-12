# SPDX-License-Identifier: Apache-2.0
"""Resource patterns: exact names and one trailing wildcard, nothing more.

Policy patterns are matched against names providers normalize — `analytics.customers`,
`project.dataset.table`, `database.collection`, `catalog.database.table`.

The grammar is three rules and no regex, because a pattern language is a place to
be wrong quietly — a `.*` read as a regex matches everything, and nobody notices
until an audit:

    events              exactly that resource
    analytics.*         anything inside the `analytics` namespace
    scratch_*           any resource in this namespace whose name starts so

The last of those exists for engines with no namespace level to lean on:
MongoDB has `database.collection` and nothing between, so `derived_*` is the only
way to say "the derived collections". It matches within one segment, never across
a dot.
"""

from __future__ import annotations

from gantry.policy.errors import PolicyConfigurationError

_WILDCARD = "*"


def normalize_pattern(pattern: str) -> str:
    """Validate one pattern and fold it to its matching form.

    Accepted: `events`, `analytics.*`, `project.dataset.*`, `scratch_*` and a
    bare `*`. Everything else — a star inside a name, an empty segment, a
    non-final star — is a configuration error rather than a pattern that
    silently matches nothing.
    """
    if not isinstance(pattern, str):
        raise PolicyConfigurationError(f"resource pattern must be a string, not {type(pattern)}")
    cleaned = pattern.strip()
    if not cleaned:
        raise PolicyConfigurationError("resource pattern must not be empty")
    segments = cleaned.split(".")
    for index, segment in enumerate(segments):
        if not segment:
            raise PolicyConfigurationError(f"resource pattern has an empty segment: {pattern!r}")
        last = index == len(segments) - 1
        if _WILDCARD not in segment:
            continue
        if not last:
            raise PolicyConfigurationError(f"'*' is only allowed in the last segment: {pattern!r}")
        if segment.count(_WILDCARD) > 1 or not segment.endswith(_WILDCARD):
            raise PolicyConfigurationError(f"'*' is only allowed at the end of a name: {pattern!r}")
    return cleaned.lower()


def matches(pattern: str, resource: str) -> bool:
    """Does one normalized pattern cover this resource name?

    A whole `*` segment covers one or more remaining segments, so `analytics.*`
    covers `analytics.customers` and `analytics.public.customers` but not
    `analytics` itself — the pattern names things *in* a namespace, and a
    namespace is not one of its own members.

    A `prefix*` segment covers that one segment only: `scratch_*` matches
    `scratch_orders` and not `scratch_orders.v2`, because a prefix is a name, not
    a namespace.
    """
    name = resource.strip().lower()
    if not name:
        return False
    if pattern == _WILDCARD:
        return True
    expected = pattern.split(".")
    actual = name.split(".")
    tail = expected[-1]
    if tail == _WILDCARD:
        prefix = expected[:-1]
        return len(actual) > len(prefix) and actual[: len(prefix)] == prefix
    if not tail.endswith(_WILDCARD):
        return expected == actual
    if len(actual) != len(expected) or actual[:-1] != expected[:-1]:
        return False
    return actual[-1].startswith(tail[:-1])


def matches_any(patterns: tuple[str, ...], resource: str) -> bool:
    return any(matches(pattern, resource) for pattern in patterns)
