# SPDX-License-Identifier: Apache-2.0
"""Turning a Flink SQL job into the normalized request policy decides on.

Policy governs submission authority for Flink: which tables a job may read and
which sink it may write. Runtime health — restarts, watermark lag — is
verification's job, not policy's, because it cannot be known before the job
runs.
"""

from __future__ import annotations

from collections.abc import Sequence

from gantry.actor import current_actor, current_environment
from gantry.policy.request import PolicyRequest
from gantry.runs.model import OperationKind, ResourceRef


def qualified(name: str, *, catalog: str | None, database: str | None) -> str:
    """Fill in the parts the connection knows, and guess at nothing else.

    A job that writes `orders` and one that writes `gantry.orders` name the same
    table when the connection's default database is `gantry`. Leaving both as
    written would let the spelling decide whether a rule matched, so a bare name
    is qualified from the connection's own defaults.

    A name that already contains a dot is used exactly as written. Flink allows
    one identifier to contain dots — a JDBC catalog exposes a PostgreSQL
    `analytics.orders` as the single identifier `analytics.orders` — so counting
    dots cannot tell a qualified name from a name with a dot in it. Guessing
    would rename tables, so policy patterns for those name them as the SQL does.
    """
    cleaned = name.strip().strip("`").strip()
    if not cleaned or "." in cleaned:
        return cleaned
    parts = [part for part in (catalog, database) if part]
    return ".".join([*parts, cleaned])


def job_request(
    *,
    kind: OperationKind,
    inputs: Sequence[str],
    output: str | None,
    catalog: str | None = None,
    database: str | None = None,
    constraints: dict[str, float] | None = None,
    unresolved: Sequence[str] = (),
) -> PolicyRequest:
    """The policy request for one Flink batch or streaming job."""

    def ref(name: str) -> ResourceRef:
        return ResourceRef(
            system="flink", resource=qualified(name, catalog=catalog, database=database)
        )

    return PolicyRequest(
        actor=current_actor(),
        operation=kind,
        engine="flink",
        inputs=tuple(ref(name) for name in dict.fromkeys(inputs)),
        outputs=() if output is None else (ref(output),),
        environment=current_environment(),
        constraints=dict(constraints or {}),
        unresolved=tuple(unresolved) or (() if output else ("no sink could be determined",)),
    )
