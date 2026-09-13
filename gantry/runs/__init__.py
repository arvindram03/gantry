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

A run parked by a confirmation rule waits here until the host answers:

    if run.status is gantry.RunStatus.AWAITING_CONFIRMATION:
        run = await gantry.runs.confirm(run.id)   # or gantry.runs.decline(run.id)
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
from gantry.runs.service import ConfirmationError, awaiting, confirm, decline
from gantry.runs.sqlite import SQLiteRunStore
from gantry.runs.status import RunStatus
from gantry.runs.store import (
    MemoryRunStore,
    RunPersistenceError,
    RunStore,
    compare_and_set,
    configure,
    create,
    get,
    recent,
    store,
    update,
)

__all__ = [
    "AdmissionRecord",
    "ConfirmationError",
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
    "awaiting",
    "compare_and_set",
    "configure",
    "confirm",
    "create",
    "decline",
    "get",
    "new_run_id",
    "recent",
    "store",
    "update",
]
