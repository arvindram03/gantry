# SPDX-License-Identifier: Apache-2.0
"""The dead-letter queue.

An event the runtime cannot apply is kept, not dropped. A queue that records
the fact and discards the payload is a counter, and a counter cannot be
replayed once the cause is fixed.

Depth is a first-class signal: a growing queue means the stream is producing
changes the target will not take, which is a different problem from being slow
and needs a different response.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import Row, func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.changes import ChangeEvent
from gantry.movement.cdc_apply import event_payload
from gantry.state.database import transaction
from gantry.state.tables import dead_letters


@dataclass(frozen=True)
class DeadLetter:
    """One event the runtime declined to apply, and why."""

    id: int
    operation: str
    dataset: str
    key_text: str | None
    source_lsn: int | None
    reason: str
    payload: dict[str, object]
    occurred_at: datetime
    replayed_at: datetime | None = None

    @property
    def pending(self) -> bool:
        return self.replayed_at is None

    def to_event(self) -> ChangeEvent:
        """Rebuild the event, so a fixed cause can be retried."""
        return ChangeEvent.model_validate(self.payload)


class DeadLetterStore(Protocol):
    async def record(self, operation: str, failures: Sequence[tuple[ChangeEvent, str]]) -> int: ...

    async def pending(self, operation: str, limit: int = 100) -> Sequence[DeadLetter]: ...

    async def depth(self, operation: str) -> int: ...

    async def mark_replayed(self, ids: Sequence[int]) -> int: ...


class PostgresDeadLetterStore:
    """Dead letters in the metadata store."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(self, operation: str, failures: Sequence[tuple[ChangeEvent, str]]) -> int:
        if not failures:
            return 0
        now = datetime.now(UTC)
        rows = [
            {
                "operation": operation,
                "dataset": event.dataset,
                "key_text": event.key_text or None,
                "source_lsn": event.source_lsn,
                "reason": reason,
                "payload": event_payload(event),
                "occurred_at": now,
            }
            for event, reason in failures
        ]
        async with transaction(self._engine) as connection:
            await connection.execute(dead_letters.insert(), rows)
        return len(rows)

    async def pending(self, operation: str, limit: int = 100) -> Sequence[DeadLetter]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(dead_letters)
                    .where(
                        dead_letters.c.operation == operation,
                        dead_letters.c.replayed_at.is_(None),
                    )
                    .order_by(dead_letters.c.id)
                    .limit(limit)
                )
            ).all()
        return tuple(_to_dead_letter(row) for row in rows)

    async def depth(self, operation: str) -> int:
        """How many events are waiting. The metric an operator watches."""
        async with transaction(self._engine) as connection:
            return int(
                (
                    await connection.execute(
                        select(func.count())
                        .select_from(dead_letters)
                        .where(
                            dead_letters.c.operation == operation,
                            dead_letters.c.replayed_at.is_(None),
                        )
                    )
                ).scalar_one()
            )

    async def mark_replayed(self, ids: Sequence[int]) -> int:
        if not ids:
            return 0
        async with transaction(self._engine) as connection:
            result = await connection.execute(
                update(dead_letters)
                .where(dead_letters.c.id.in_(list(ids)))
                .values(replayed_at=datetime.now(UTC))
            )
        return int(result.rowcount)


def _to_dead_letter(row: Row[tuple[object, ...]]) -> DeadLetter:
    mapping = row._mapping
    return DeadLetter(
        id=mapping["id"],
        operation=mapping["operation"],
        dataset=mapping["dataset"],
        key_text=mapping["key_text"],
        source_lsn=mapping["source_lsn"],
        reason=mapping["reason"],
        payload=dict(mapping["payload"]),
        occurred_at=mapping["occurred_at"],
        replayed_at=mapping["replayed_at"],
    )
