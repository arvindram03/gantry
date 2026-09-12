# SPDX-License-Identifier: Apache-2.0
"""Durable records of governed data work.

Every governed operation produces a run, recorded before anything external
happens and updated as it progresses. A run outlives the agent and the process
that started it, which is what makes it possible to ask later why a piece of
work was accepted.

    gantry.runs.configure(gantry.runs.SQLiteRunStore(".gantry/runs.db"))

    run = await query("SELECT ...")
    run.id, run.status

    # …in another process, holding only the id
    print(gantry.runs.get(run_id).render())
"""

from __future__ import annotations

from gantry.runs.model import (
    AdmissionRecord,
    ExecutionRecord,
    OperationKind,
    OperationRef,
    ProposalRecord,
    ProposalStorage,
    QueryResultRef,
    ResourceRef,
    Run,
    new_run_id,
)
from gantry.runs.sqlite import SQLiteRunStore
from gantry.runs.status import RunStatus
from gantry.runs.store import (
    MemoryRunStore,
    RunPersistenceError,
    RunStore,
    configure,
    create,
    get,
    recent,
    store,
    update,
)

__all__ = [
    "AdmissionRecord",
    "ExecutionRecord",
    "MemoryRunStore",
    "OperationKind",
    "OperationRef",
    "ProposalRecord",
    "ProposalStorage",
    "QueryResultRef",
    "ResourceRef",
    "Run",
    "RunPersistenceError",
    "RunStatus",
    "RunStore",
    "SQLiteRunStore",
    "configure",
    "create",
    "get",
    "new_run_id",
    "recent",
    "store",
    "update",
]
