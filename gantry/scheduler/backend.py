"""The workflow backend interface.

Durable task dispatch sits behind this Protocol so the durability substrate can
change without touching the runtime. v1 ships a Postgres leased queue; Temporal
becomes an alternative implementation rather than a rewrite.

Dispatch is at-least-once by design. A worker can die after committing its side
effect and before recording progress, so the runtime assumes retries and
requires effects to be idempotent - see the design document's section 8.7.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from gantry.core.names import ResourceName
from gantry.lifecycle.plan import PlanVersion


class TaskState(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    DONE = "done"
    # Retries exhausted. Quarantined rather than retried forever, so one poison
    # task cannot consume the whole worker pool.
    QUARANTINED = "quarantined"


class Task(BaseModel):
    """One leasable unit of work: a plan node for an operation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: ResourceName
    plan_version: int = Field(ge=1)
    node_id: str
    depends_on: tuple[str, ...] = ()
    state: TaskState = TaskState.PENDING
    attempts: int = Field(default=0, ge=0)
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    last_error: str | None = None


class WorkflowBackend(Protocol):
    """Durable dispatch of plan nodes."""

    async def submit(self, plan: PlanVersion) -> int:
        """Enqueue a plan's nodes. Idempotent: resubmitting adds nothing.

        Resubmission happens on every restart, so it must not reset progress
        already made.
        """
        ...

    async def lease(self, worker: str, ttl: timedelta, *, now: datetime) -> Task | None:
        """Claim one runnable task, or None if nothing is ready.

        A task is runnable only when every node it depends on is done.
        """
        ...

    async def complete(self, task: Task) -> None:
        """Mark a leased task done."""
        ...

    async def release(self, task: Task, reason: str, *, max_attempts: int) -> TaskState:
        """Return a failed task to the queue, or quarantine it.

        Returns the state the task ended in so the caller can distinguish a
        retry from exhaustion.
        """
        ...

    async def reclaim_expired(self, *, now: datetime) -> int:
        """Return tasks whose lease expired to PENDING.

        This is what makes worker death survivable: nobody has to notice the
        crash, the lease simply runs out.
        """
        ...

    async def tasks(self, operation: str) -> Sequence[Task]:
        """Every task for an operation, for inspection and tests."""
        ...
