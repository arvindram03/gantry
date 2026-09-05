"""Coordinating a snapshot with a change stream.

The ordering here is the whole of it, and getting it wrong produces a target
that looks correct and is not.

    1. Create the replication slot, and wait for it to exist.
    2. Capture the source position P, after the slot exists.
    3. Snapshot, stamping every row with P.
    4. Apply changes from the stream; anything at or before P is refused.

Each step exists because of a specific way the alternatives fail.

**The slot comes first.** A slot created after the snapshot begins does not
capture the changes made during it, and those changes are lost with nothing to
indicate they ever happened. Creating it first means the stream covers
everything from before the snapshot started.

**The position is captured after the slot exists.** A position captured first
would name a point the stream cannot replay from.

**The snapshot is stamped with P rather than with the source's own column.** A
snapshot represents the source as of one position. Stamping it says so, and
lets the ordinary stale-write guard decide every subsequent conflict: a change
after P wins, a change at or before P is already in the snapshot.

**Overlap is expected, not avoided.** Changes between the slot's creation and P
appear in both the snapshot and the stream. They are applied twice and the
second application is refused, which is exactly what idempotency is for. The
alternative - trying to make the two phases disjoint - requires locking the
source, which is what this design exists to avoid.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.adapters.cdc.debezium import DebeziumConfig, DebeziumConnectClient
from gantry.adapters.cdc.slots import check_wal_retention, wait_for_slot
from gantry.adapters.source.postgres import PostgresSourceAdapter

# How close the stream has to get before the runtime calls it caught up.
DEFAULT_LAG_THRESHOLD = timedelta(seconds=2)


class HandoffError(Exception):
    """The snapshot and the stream could not be coordinated."""


@dataclass(frozen=True)
class Handoff:
    """The position a snapshot represents, and the stream that continues it."""

    snapshot_lsn: int
    slot_name: str
    topics: tuple[str, ...]
    established_at: datetime

    def supersedes(self, event_lsn: int) -> bool:
        """Whether the snapshot already contains a change at this position."""
        return event_lsn <= self.snapshot_lsn


async def establish(
    source_engine: AsyncEngine,
    *,
    config: DebeziumConfig,
    connect_url: str,
    slot_timeout_seconds: float = 60.0,
) -> Handoff:
    """Start capturing, then fix the position the snapshot will represent.

    Returns once the stream is guaranteed to cover everything after the
    returned position.
    """
    client = DebeziumConnectClient(connect_url)
    await client.ensure(config)

    # RUNNING is not capturing: the slot has to exist before the position is
    # meaningful, or the stream cannot replay from it.
    await wait_for_slot(source_engine, config.slot_name, timeout_seconds=slot_timeout_seconds)

    position = await PostgresSourceAdapter(source_engine).current_position()
    return Handoff(
        snapshot_lsn=int(position.value),
        slot_name=config.slot_name,
        topics=config.topics,
        established_at=datetime.now(UTC),
    )


@dataclass
class CatchUpReport:
    """How the stream got on catching up with the source."""

    lag: timedelta
    applied: int = 0
    rejected_stale: int = 0
    polls: int = 0
    caught_up: bool = False
    retained_wal_bytes: int = 0

    def describe(self) -> str:
        state = "caught up" if self.caught_up else "still behind"
        return (
            f"{state}: lag {self.lag.total_seconds():.2f}s, "
            f"{self.applied} applied, {self.rejected_stale} stale rejected"
        )


async def wait_for_lag(
    source_engine: AsyncEngine,
    handoff: Handoff,
    *,
    measure_lag: Callable[[], Awaitable[timedelta]],
    threshold: timedelta = DEFAULT_LAG_THRESHOLD,
    give_up_after: timedelta = timedelta(minutes=10),
    interval: float = 0.5,
) -> CatchUpReport:
    """Wait until the stream is within `threshold` of the source.

    WAL retention is checked while waiting rather than after. A stream that is
    not catching up is exactly the condition that makes a slot accumulate WAL,
    so the check that matters most is the one performed while the problem is
    happening.
    """
    deadline = asyncio.get_running_loop().time() + give_up_after.total_seconds()
    report = CatchUpReport(lag=timedelta.max)

    while asyncio.get_running_loop().time() < deadline:
        report.polls += 1
        report.lag = await measure_lag()

        status = await check_wal_retention(source_engine, handoff.slot_name)
        report.retained_wal_bytes = status.retained_bytes

        if report.lag <= threshold:
            report.caught_up = True
            return report
        await asyncio.sleep(interval)

    return report
