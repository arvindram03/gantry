# SPDX-License-Identifier: Apache-2.0
"""The signal vocabulary.

Signals are named and defined here rather than written as expressions in a
spec. That is the scope fence: a spec that accepted arbitrary SQL would make
this a query language, and Gantry compiles onto engines that already have one.
A signal the compiler does not know is a compile error, not something passed
through and discovered at runtime.

Adding a signal is a deliberate act - a definition here, and a note in the
docs. That is the intended friction.
"""

from __future__ import annotations

from gantry.analysis.artifact import SignalDefinition

_DEFINITIONS: tuple[SignalDefinition, ...] = (
    SignalDefinition(
        name="row_count",
        expression="count(*)",
        description="Rows contributing to the group.",
    ),
    SignalDefinition(
        name="p95_latency",
        expression="percentile_cont(0.95) WITHIN GROUP (ORDER BY {latency_ms})",
        requires=("latency_ms",),
        description="95th percentile latency, in milliseconds.",
    ),
    SignalDefinition(
        name="p50_latency",
        expression="percentile_cont(0.5) WITHIN GROUP (ORDER BY {latency_ms})",
        requires=("latency_ms",),
        description="Median latency, in milliseconds.",
    ),
    SignalDefinition(
        name="error_rate",
        expression=("coalesce(avg(CASE WHEN {status} >= 500 THEN 1.0 ELSE 0.0 END), 0)"),
        requires=("status",),
        description="Fraction of requests returning a server error.",
    ),
    SignalDefinition(
        name="timeout_count",
        expression="count(*) FILTER (WHERE {status} = 504)",
        requires=("status",),
        description="Requests that timed out.",
    ),
    SignalDefinition(
        name="database_calls_per_request",
        expression="coalesce(avg({db_calls}), 0)",
        requires=("db_calls",),
        description="Mean database calls made per request.",
    ),
    SignalDefinition(
        name="database_wait_time",
        expression="coalesce(avg({db_wait_ms}), 0)",
        requires=("db_wait_ms",),
        description="Mean time spent waiting on the database, in milliseconds.",
    ),
)

SIGNALS: dict[str, SignalDefinition] = {definition.name: definition for definition in _DEFINITIONS}


def known_signals() -> tuple[str, ...]:
    return tuple(sorted(SIGNALS))


def signal(name: str) -> SignalDefinition | None:
    return SIGNALS.get(name)
