"""The durable leased task queue.

Leasing uses `FOR UPDATE SKIP LOCKED`, so many workers can claim work
concurrently without blocking each other and without ever handing the same task
to two workers. Dependency readiness is evaluated in the same statement as the
claim: checking first and claiming second would leave a window where a
dependency regresses between the two.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta

from sqlalchemy import Row, and_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.lifecycle.plan import PlanVersion
from gantry.scheduler.backend import Task, TaskState
from gantry.state.database import transaction
from gantry.state.tables import tasks

# One statement: find a runnable task, lock it, and claim it. Splitting the
# read from the write would let two workers see the same PENDING row.
_LEASE_SQL = text(
    """
    UPDATE tasks AS t
       SET state = 'leased',
           attempts = t.attempts + 1,
           lease_owner = :worker,
           lease_expires_at = :expires_at
     WHERE (t.operation, t.node_id) = (
           SELECT c.operation, c.node_id
             FROM tasks AS c
            WHERE c.operation = :operation
              AND c.state = 'pending'
              AND EXISTS (
                    SELECT 1
                      FROM operations AS o
                     WHERE o.name = c.operation
                       AND o.state NOT IN ('paused', 'failed', 'completed')
                  )
              AND NOT EXISTS (
                    SELECT 1
                      FROM tasks AS dep
                     WHERE dep.operation = c.operation
                       AND dep.node_id = ANY(c.depends_on)
                       AND dep.state <> 'done'
                  )
            ORDER BY c.node_id
              FOR UPDATE SKIP LOCKED
            LIMIT 1
           )
 RETURNING t.operation, t.node_id, t.plan_version, t.depends_on, t.state,
           t.attempts, t.lease_owner, t.lease_expires_at, t.last_error
    """
)


class PostgresWorkflowBackend:
    """A leased task queue backed by the metadata store."""

    def __init__(self, engine: AsyncEngine, operation: str) -> None:
        self._engine = engine
        self._operation = operation

    async def submit(self, plan: PlanVersion) -> int:
        rows = [
            {
                "operation": plan.operation,
                "node_id": node.id,
                "plan_version": plan.version,
                "depends_on": list(node.depends_on),
                "state": TaskState.PENDING.value,
                "attempts": 0,
            }
            for node in plan.nodes
        ]
        async with transaction(self._engine) as connection:
            # Resubmission happens on every restart and must not reset progress.
            # RETURNING rather than rowcount: a multi-row INSERT ... ON CONFLICT
            # DO NOTHING reports -1 through the async driver.
            result = await connection.execute(
                insert(tasks)
                .on_conflict_do_nothing(index_elements=["operation", "node_id"])
                .returning(tasks.c.node_id),
                rows,
            )
            return len(result.all())

    async def lease(self, worker: str, ttl: timedelta, *, now: datetime) -> Task | None:
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(
                    _LEASE_SQL,
                    {"worker": worker, "expires_at": now + ttl, "operation": self._operation},
                )
            ).one_or_none()
        return None if row is None else _to_task(row)

    async def complete(self, task: Task) -> None:
        async with transaction(self._engine) as connection:
            await connection.execute(
                update(tasks)
                .where(
                    and_(
                        tasks.c.operation == task.operation,
                        tasks.c.node_id == task.node_id,
                    )
                )
                .values(state=TaskState.DONE.value, lease_owner=None, lease_expires_at=None)
            )

    async def release(self, task: Task, reason: str, *, max_attempts: int) -> TaskState:
        state = TaskState.QUARANTINED if task.attempts >= max_attempts else TaskState.PENDING
        async with transaction(self._engine) as connection:
            await connection.execute(
                update(tasks)
                .where(
                    and_(
                        tasks.c.operation == task.operation,
                        tasks.c.node_id == task.node_id,
                    )
                )
                .values(
                    state=state.value,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_error=reason,
                )
            )
        return state

    async def reclaim_expired(self, *, now: datetime) -> int:
        async with transaction(self._engine) as connection:
            result = await connection.execute(
                update(tasks)
                .where(
                    and_(
                        tasks.c.operation == self._operation,
                        tasks.c.state == TaskState.LEASED.value,
                        tasks.c.lease_expires_at <= now,
                    )
                )
                .values(
                    state=TaskState.PENDING.value,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_error="lease expired",
                )
            )
        return result.rowcount

    async def tasks(self, operation: str) -> Sequence[Task]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(tasks).where(tasks.c.operation == operation).order_by(tasks.c.node_id)
                )
            ).all()
        return tuple(_to_task(row) for row in rows)


def _to_task(row: Row[tuple[object, ...]]) -> Task:
    mapping = row._mapping
    return Task(
        operation=mapping["operation"],
        plan_version=mapping["plan_version"],
        node_id=mapping["node_id"],
        depends_on=tuple(mapping["depends_on"]),
        state=TaskState(mapping["state"]),
        attempts=mapping["attempts"],
        lease_owner=mapping["lease_owner"],
        lease_expires_at=mapping["lease_expires_at"],
        last_error=mapping["last_error"],
    )
