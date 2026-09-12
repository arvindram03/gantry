# SPDX-License-Identifier: Apache-2.0
"""Durable run records: what happened, and why it was accepted.

A result lives as long as the process that asked for it. The question this
answers is the one that comes later and from someone else — why did last
night's rollup get accepted, and what was checked — at a point when the agent
conversation that produced it is gone.

    gantry.runs.record(result.evidence)
    run = gantry.runs.get("run_123")
    print(run.render())

SQLite by default, which is enough for v0 and needs nothing running. The store
is swappable for anything implementing `RunStore`.
"""

from __future__ import annotations

from gantry.runs.model import RunRecord
from gantry.runs.store import (
    MemoryRunStore,
    RunStore,
    SQLiteRunStore,
    configure,
    get,
    recent,
    record,
)

__all__ = [
    "MemoryRunStore",
    "RunRecord",
    "RunStore",
    "SQLiteRunStore",
    "configure",
    "get",
    "recent",
    "record",
]
