"""The record of what agents asked for.

`evidence.persist: true` in the RFC's policy block. What makes this worth
having is that it records refusals as carefully as grants: a log holding only
what was permitted can show a clean history while an agent probes every rung on
every Dataset and is turned away each time.

The rows are evidence, not conversation. Each one says who asked, for what, at
which rung, what policy answered, and on what grounds.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.policy.gate import AccessDecision
from gantry.state.database import transaction
from gantry.state.tables import access_log


@dataclass(frozen=True)
class AccessEvent:
    """One recorded decision."""

    dataset: str
    rung: str
    decision: str
    principal: str
    grounds: tuple[str, ...] = ()
    redacted_fields: tuple[str, ...] = ()
    reason: str | None = None
    row_limit: int | None = None
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def permitted(self) -> bool:
        return self.decision != "deny"

    def describe(self) -> str:
        head = f"{self.principal} {self.rung} {self.dataset}: {self.decision}"
        return head + (f" - {'; '.join(self.grounds)}" if self.grounds else "")


def to_event(decision: AccessDecision, *, principal: str) -> AccessEvent:
    return AccessEvent(
        dataset=decision.dataset,
        rung=decision.rung.describe(),
        decision=decision.decision.value,
        principal=principal,
        grounds=decision.grounds,
        redacted_fields=decision.redacted_fields,
        reason=decision.request.reason,
        row_limit=decision.row_limit,
    )


class AccessLog(Protocol):
    async def record(self, decision: AccessDecision) -> None: ...

    async def events(self, *, dataset: str | None = None) -> Sequence[AccessEvent]: ...


class NullAccessLog:
    """Records nothing. For `evidence.persist: false`, and for tests."""

    async def record(self, decision: AccessDecision) -> None:
        return None

    async def events(self, *, dataset: str | None = None) -> Sequence[AccessEvent]:
        return ()


class InMemoryAccessLog:
    """Holds events in process. The reference implementation."""

    def __init__(self, *, principal: str = "agent") -> None:
        self._principal = principal
        self._events: list[AccessEvent] = []

    async def record(self, decision: AccessDecision) -> None:
        self._events.append(to_event(decision, principal=self._principal))

    async def events(self, *, dataset: str | None = None) -> Sequence[AccessEvent]:
        return tuple(e for e in self._events if dataset is None or e.dataset == dataset)


class PostgresAccessLog:
    """The durable trail, in the metadata store."""

    def __init__(self, engine: AsyncEngine, *, principal: str = "agent") -> None:
        self._engine = engine
        self._principal = principal

    async def record(self, decision: AccessDecision) -> None:
        event = to_event(decision, principal=self._principal)
        async with transaction(self._engine) as connection:
            await connection.execute(
                access_log.insert().values(
                    dataset=event.dataset,
                    rung=event.rung,
                    decision=event.decision,
                    principal=event.principal,
                    grounds=list(event.grounds),
                    redacted_fields=list(event.redacted_fields),
                    reason=event.reason,
                    row_limit=event.row_limit,
                    observed_at=event.observed_at,
                )
            )

    async def events(self, *, dataset: str | None = None) -> Sequence[AccessEvent]:
        statement = select(access_log).order_by(access_log.c.observed_at, access_log.c.id)
        if dataset is not None:
            statement = statement.where(access_log.c.dataset == dataset)
        async with transaction(self._engine) as connection:
            rows = (await connection.execute(statement)).all()
        return tuple(
            AccessEvent(
                dataset=row._mapping["dataset"],
                rung=row._mapping["rung"],
                decision=row._mapping["decision"],
                principal=row._mapping["principal"],
                grounds=tuple(row._mapping["grounds"] or ()),
                redacted_fields=tuple(row._mapping["redacted_fields"] or ()),
                reason=row._mapping["reason"],
                row_limit=row._mapping["row_limit"],
                observed_at=row._mapping["observed_at"],
            )
            for row in rows
        )


def access_log_for(
    engine: AsyncEngine | None, *, persist: bool, principal: str = "agent"
) -> AccessLog:
    """The log the policy asked for."""
    if not persist or engine is None:
        return NullAccessLog()
    return PostgresAccessLog(engine, principal=principal)
