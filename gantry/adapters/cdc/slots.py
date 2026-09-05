"""Replication slot monitoring.

A replication slot is the mechanism that makes CDC reliable and the mechanism
that can take a source database down. The slot holds WAL until the consumer
confirms it, so a consumer that stops - crashed, paused, or merely slower than
the write rate - makes the source accumulate WAL indefinitely. Disks fill, and
the database stops accepting writes.

That failure is silent right up until it is total, which is why the runtime
measures it rather than waiting to be told.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.state.database import transaction

# A slot this far behind is a problem worth acting on well before the disk is.
DEFAULT_WARNING_BYTES = 1_073_741_824  # 1 GiB
DEFAULT_CRITICAL_BYTES = 10_737_418_240  # 10 GiB


@dataclass(frozen=True)
class SlotStatus:
    """What a replication slot is holding."""

    name: str
    exists: bool
    active: bool = False
    retained_bytes: int = 0
    confirmed_flush_lsn: str | None = None
    current_lsn: str | None = None

    def describe(self) -> str:
        if not self.exists:
            return f"slot {self.name!r} does not exist"
        state = "active" if self.active else "inactive"
        return f"slot {self.name!r} {state}, retaining {self.retained_bytes:,} bytes of WAL"


class SlotBusyError(Exception):
    """Raised when a slot could not be dropped because it is still in use."""

    def __init__(self, slot_name: str, timeout: float) -> None:
        super().__init__(
            f"replication slot {slot_name!r} was still active after {timeout:.0f}s; "
            f"stop its consumer before dropping it"
        )
        self.slot_name = slot_name


class SlotNotReadyError(Exception):
    """Raised when a replication slot never appeared."""

    def __init__(self, slot_name: str, timeout: float, status: SlotStatus) -> None:
        super().__init__(
            f"replication slot {slot_name!r} was not ready after {timeout:.0f}s "
            f"({status.describe()}); the connector may have started without "
            f"connecting to the source"
        )
        self.slot_name = slot_name
        self.status = status


class WalRetentionError(Exception):
    """Raised when a slot has retained more WAL than the policy permits."""

    def __init__(self, status: SlotStatus, limit: int) -> None:
        super().__init__(
            f"{status.describe()}, over the {limit:,} byte limit; "
            f"an unconsumed slot will eventually stop the source accepting writes"
        )
        self.status = status


_SLOT_QUERY = text(
    """
    SELECT slot_name,
           active,
           confirmed_flush_lsn::text AS confirmed,
           pg_current_wal_lsn()::text AS current,
           COALESCE(
               pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0
           )::bigint AS retained
      FROM pg_replication_slots
     WHERE slot_name = :slot_name
    """
)


async def slot_status(engine: AsyncEngine, slot_name: str) -> SlotStatus:
    """Read what a slot is currently holding."""
    async with transaction(engine) as connection:
        row = (await connection.execute(_SLOT_QUERY, {"slot_name": slot_name})).one_or_none()

    if row is None:
        return SlotStatus(name=slot_name, exists=False)
    return SlotStatus(
        name=slot_name,
        exists=True,
        active=bool(row.active),
        retained_bytes=int(row.retained),
        confirmed_flush_lsn=row.confirmed,
        current_lsn=row.current,
    )


async def wait_for_slot(
    engine: AsyncEngine, slot_name: str, *, timeout_seconds: float = 60.0, interval: float = 0.5
) -> SlotStatus:
    """Wait until a slot exists and is active.

    A Debezium connector reports RUNNING before its replication slot exists:
    Connect's health says the task started, not that it has connected to the
    source and created the slot. Treating those as the same thing means Prepare
    can complete while nothing is actually capturing changes, and the gap is
    invisible until the snapshot finishes and CDC has nothing to catch up from.
    """
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    status = await slot_status(engine, slot_name)
    while asyncio.get_running_loop().time() < deadline:
        if status.exists and status.active:
            return status
        await asyncio.sleep(interval)
        status = await slot_status(engine, slot_name)
    raise SlotNotReadyError(slot_name, timeout_seconds, status)


async def check_wal_retention(
    engine: AsyncEngine,
    slot_name: str,
    *,
    critical_bytes: int = DEFAULT_CRITICAL_BYTES,
) -> SlotStatus:
    """Read a slot and refuse to continue if it is retaining too much WAL.

    Raising is deliberate. Continuing to stream while the source fills its disk
    trades a stalled migration for a stopped database, which is a much worse
    outcome than the one being avoided.
    """
    status = await slot_status(engine, slot_name)
    if status.exists and status.retained_bytes > critical_bytes:
        raise WalRetentionError(status, critical_bytes)
    return status


async def drop_slot(
    engine: AsyncEngine, slot_name: str, *, timeout_seconds: float = 30.0, interval: float = 0.5
) -> bool:
    """Drop a slot, releasing the WAL it holds.

    Waits for the slot to become inactive first. PostgreSQL refuses to drop an
    active slot, and a connector does not release one the instant it is
    deleted - so dropping immediately after teardown fails, leaves the slot
    behind, and pins WAL forever. That is the exact failure this module exists
    to prevent, and it is easy to write it into the cleanup path by accident.
    """
    deadline = asyncio.get_running_loop().time() + timeout_seconds

    while True:
        status = await slot_status(engine, slot_name)
        if not status.exists:
            return False
        if not status.active:
            break
        if asyncio.get_running_loop().time() >= deadline:
            raise SlotBusyError(slot_name, timeout_seconds)
        await asyncio.sleep(interval)

    async with transaction(engine) as connection:
        await connection.execute(
            text("SELECT pg_drop_replication_slot(:name)"), {"name": slot_name}
        )
    return True


async def drop_orphaned_slots(engine: AsyncEngine, prefix: str) -> tuple[str, ...]:
    """Drop every inactive slot whose name starts with `prefix`.

    A migration that crashed before teardown leaves a slot behind. Cleaning up
    by prefix lets an operator reclaim WAL without knowing which run abandoned
    what; active slots are left alone, because something is still using them.
    """
    async with transaction(engine) as connection:
        rows = (
            await connection.execute(
                text(
                    "SELECT slot_name FROM pg_replication_slots "
                    "WHERE slot_name LIKE :pattern AND NOT active"
                ),
                {"pattern": f"{prefix}%"},
            )
        ).all()

    dropped: list[str] = []
    for row in rows:
        name = str(row.slot_name)
        try:
            if await drop_slot(engine, name, timeout_seconds=1.0):
                dropped.append(name)
        except SlotBusyError:
            # Something took it between the listing and the drop.
            continue
    return tuple(dropped)
