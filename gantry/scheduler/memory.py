# SPDX-License-Identifier: Apache-2.0
"""In-memory workflow backend.

The reference implementation of the leasing rules, and the fake the simulator
runs against. Behaviour here is the contract the Postgres backend must match.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta

from gantry.lifecycle.plan import PlanVersion
from gantry.scheduler.backend import Task, TaskState


class InMemoryWorkflowBackend:
    """A leased task queue held in process memory."""

    def __init__(self) -> None:
        self._tasks: dict[tuple[str, str], Task] = {}

    async def submit(self, plan: PlanVersion) -> int:
        added = 0
        for node in plan.nodes:
            key = (plan.operation, node.id)
            if key in self._tasks:
                continue
            self._tasks[key] = Task(
                operation=plan.operation,
                plan_version=plan.version,
                node_id=node.id,
                depends_on=node.depends_on,
            )
            added += 1
        return added

    async def lease(self, worker: str, ttl: timedelta, *, now: datetime) -> Task | None:
        for key in sorted(self._tasks):
            task = self._tasks[key]
            if task.state is not TaskState.PENDING:
                continue
            if not self._dependencies_met(task):
                continue
            leased = task.model_copy(
                update={
                    "state": TaskState.LEASED,
                    "attempts": task.attempts + 1,
                    "lease_owner": worker,
                    "lease_expires_at": now + ttl,
                }
            )
            self._tasks[key] = leased
            return leased
        return None

    async def complete(self, task: Task) -> None:
        key = (task.operation, task.node_id)
        current = self._tasks[key]
        self._tasks[key] = current.model_copy(
            update={"state": TaskState.DONE, "lease_owner": None, "lease_expires_at": None}
        )

    async def release(self, task: Task, reason: str, *, max_attempts: int) -> TaskState:
        key = (task.operation, task.node_id)
        current = self._tasks[key]
        state = TaskState.QUARANTINED if current.attempts >= max_attempts else TaskState.PENDING
        self._tasks[key] = current.model_copy(
            update={
                "state": state,
                "lease_owner": None,
                "lease_expires_at": None,
                "last_error": reason,
            }
        )
        return state

    async def reclaim_expired(self, *, now: datetime) -> int:
        reclaimed = 0
        for key, task in self._tasks.items():
            if task.state is not TaskState.LEASED:
                continue
            if task.lease_expires_at is not None and task.lease_expires_at > now:
                continue
            self._tasks[key] = task.model_copy(
                update={
                    "state": TaskState.PENDING,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error": "lease expired",
                }
            )
            reclaimed += 1
        return reclaimed

    async def tasks(self, operation: str) -> Sequence[Task]:
        return tuple(
            task for key, task in sorted(self._tasks.items()) if task.operation == operation
        )

    def _dependencies_met(self, task: Task) -> bool:
        return all(
            self._tasks[(task.operation, dependency)].state is TaskState.DONE
            for dependency in task.depends_on
            if (task.operation, dependency) in self._tasks
        )
