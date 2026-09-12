# SPDX-License-Identifier: Apache-2.0
"""Durable run records: what happened, and why it was accepted.

A result lives as long as the process that asked for it. The question this
answers is the one that comes later and from someone else — why did last
night's rollup get accepted, and what was checked — at a point when the agent
conversation that produced it is gone.

    run = gantry.runs.get("run_123")
    print(run.render())

Governed operations record automatically. Storage is in-memory until the
application configures SQLite (or another `RunStore`) for durable evidence.
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
