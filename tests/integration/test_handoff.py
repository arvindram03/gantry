"""Snapshot and change stream, running at the same time.

Requires the full stack: make dev-up && uv run alembic upgrade head

The exit criterion for M3: with writes continuing throughout the snapshot, the
target converges on the source, verified by checksum rather than by row count -
a count cannot see a row that is present on both sides and stale.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from gantry.adapters.cdc.debezium import DebeziumConfig, DebeziumConnectClient
from gantry.adapters.cdc.kafka import KafkaCDCAdapter
from gantry.adapters.cdc.slots import SlotBusyError, drop_slot
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.adapters.target.postgres import PostgresTargetAdapter
from gantry.core.changes import ChangeEvent, ChangeOperation, StreamPosition
from gantry.core.dataset import DatasetManifest
from gantry.movement.cdc_apply import CDCApplier
from gantry.movement.handoff import Handoff, establish, wait_for_lag
from gantry.movement.partitioning import plan_partitions
from gantry.state.database import create_engine, transaction
from gantry.verification.checksum import compute_checksum
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support.jobs import move_partition

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)
CONNECT_URL = os.environ.get("GANTRY_CONNECT_URL", "http://localhost:18083")
BOOTSTRAP = os.environ.get("GANTRY_KAFKA_BOOTSTRAP", "localhost:19092")

TABLE = "public.handoff_probe"
ROWS = 20_000
OPERATION = "handoff-test"


@pytest.fixture
async def source() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(SOURCE_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await connection.execute(
                text(
                    f"CREATE TABLE {TABLE} ("
                    f"  id bigint PRIMARY KEY,"
                    f"  label text NOT NULL,"
                    f"  source_lsn bigint NOT NULL DEFAULT 0)"
                )
            )
            await connection.execute(
                text(
                    f"INSERT INTO {TABLE} (id, label) "
                    f"SELECT g, 'seed-' || g FROM generate_series(1, {ROWS}) AS g"
                )
            )
            await connection.execute(text(f"ANALYZE {TABLE}"))
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def target() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(TARGET_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await connection.execute(text("DROP TABLE IF EXISTS public.gantry_tombstones"))
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def config() -> DebeziumConfig:
    suffix = uuid.uuid4().hex[:8]
    return DebeziumConfig(
        name=f"gantry-handoff-{suffix}",
        database_host="pg-source",
        database_port=5432,
        database_user="gantry",
        database_password="gantry",
        database_name="gantry",
        tables=(TABLE,),
        topic_prefix=f"ghandoff{suffix}",
        slot_name=f"gantry_handoff_{suffix}",
        publication_name=f"gantry_handoff_pub_{suffix}",
    )


@pytest.fixture
async def handoff(source: AsyncEngine, config: DebeziumConfig) -> AsyncIterator[Handoff]:
    established = await establish(source, config=config, connect_url=CONNECT_URL)
    try:
        yield established
    finally:
        await DebeziumConnectClient(CONNECT_URL).delete(config.name)
        with contextlib.suppress(SlotBusyError):
            await drop_slot(source, config.slot_name)


async def manifest_of(source: AsyncEngine) -> DatasetManifest:
    adapter = PostgresSourceAdapter(source)
    return await adapter.profile({m.name: m for m in await adapter.discover()}[TABLE])


async def write_continuously(engine: AsyncEngine, stop: asyncio.Event, updated: list[int]) -> None:
    """Keep changing the source until told to stop.

    Updates rather than inserts, so every change contends with a row the
    snapshot is also copying - which is the case that actually goes wrong.
    """
    index = 1
    while not stop.is_set():
        async with transaction(engine) as connection:
            await connection.execute(
                text(f"UPDATE {TABLE} SET label = 'live-' || id WHERE id = :id"),
                {"id": index},
            )
        updated.append(index)
        index = index % ROWS + 1
        await asyncio.sleep(0.005)


async def checksums(
    source: AsyncEngine, target: AsyncEngine, manifest: DatasetManifest
) -> tuple[str, str]:
    """Compare the two sides on everything but the position column.

    `source_lsn` differs by design - the target records where each row came
    from - so comparing it would report a mismatch on data that agrees.
    """
    comparable = manifest.model_copy(
        update={
            "dataset_schema": manifest.dataset_schema.model_copy(
                update={
                    "fields": tuple(
                        field
                        for field in manifest.dataset_schema.fields
                        if field.name != "source_lsn"
                    )
                }
            )
        }
    )
    left = await compute_checksum(source, comparable, TABLE, predicate="TRUE", params={})
    right = await compute_checksum(target, comparable, TABLE, predicate="TRUE", params={})
    return left.checksum, right.checksum


# --- the handoff itself ----------------------------------------------------


async def test_the_slot_exists_before_the_position_is_taken(
    source: AsyncEngine, handoff: Handoff
) -> None:
    """A position captured before the slot names a point nothing can replay from."""
    from gantry.adapters.cdc.slots import slot_status

    status = await slot_status(source, handoff.slot_name)
    assert status.exists
    assert handoff.snapshot_lsn > 0


async def test_the_snapshot_position_supersedes_earlier_changes(handoff: Handoff) -> None:
    assert handoff.supersedes(handoff.snapshot_lsn)
    assert handoff.supersedes(handoff.snapshot_lsn - 1)
    assert not handoff.supersedes(handoff.snapshot_lsn + 1)


# --- the exit criterion ----------------------------------------------------


async def test_snapshot_and_stream_converge_under_continuous_writes(
    source: AsyncEngine, target: AsyncEngine, config: DebeziumConfig, handoff: Handoff
) -> None:
    """Writes never stop; the target still ends up matching the source."""
    manifest = await manifest_of(source)
    target_adapter = PostgresTargetAdapter(target)
    PostgresSourceAdapter(source)
    await target_adapter.prepare(manifest, target=TABLE)

    consumer = KafkaCDCAdapter(
        topics=list(handoff.topics),
        bootstrap_servers=BOOTSTRAP,
        group_id=f"handoff-{uuid.uuid4().hex[:6]}",
    )
    await consumer.start()

    stop = asyncio.Event()
    updated: list[int] = []
    writer = asyncio.create_task(write_continuously(source, stop, updated))

    try:
        # Snapshot while the writer is running, stamping every row with the
        # position the snapshot represents.
        for partition in plan_partitions(manifest, target_partitions=4).partitions:
            await move_partition(
                manifest, partition, target=TABLE, snapshot_lsn=handoff.snapshot_lsn
            )

        assert updated, "the writer should have changed rows during the snapshot"

        # Let a little more change accumulate, then stop writing and catch up.
        await asyncio.sleep(1.0)
        stop.set()
        await writer

        applier = CDCApplier(target, operation=OPERATION, manifest=manifest, target=TABLE)
        await applier.ensure_tombstones()

        applied = rejected = 0
        for _ in range(40):
            events = await consumer.poll(timeout_ms=1000, max_records=1000)
            if not events:
                left, right = await checksums(source, target, manifest)
                if left == right:
                    break
                continue
            report = await applier.apply(events)
            applied += report.applied
            rejected += report.rejected_stale

        left, right = await checksums(source, target, manifest)
        assert left == right, (
            f"target did not converge: {applied} applied, {rejected} stale rejected"
        )
        assert applied > 0, "the stream should have carried the live writes"
        # Rejections are not asserted here. Every write in this test happens
        # after the snapshot position, so there is genuinely no overlap to
        # refuse - the property is exercised deterministically below rather
        # than left to depend on winning a race.
    finally:
        stop.set()
        writer.cancel()
        await consumer.stop()


async def test_lag_falls_below_the_threshold(
    source: AsyncEngine, config: DebeziumConfig, handoff: Handoff
) -> None:
    """A quiet stream is caught up, not lagging."""
    consumer = KafkaCDCAdapter(
        topics=list(handoff.topics),
        bootstrap_servers=BOOTSTRAP,
        group_id=f"lag-{uuid.uuid4().hex[:6]}",
    )
    await consumer.start()
    try:
        async with transaction(source) as connection:
            await connection.execute(text(f"UPDATE {TABLE} SET label = 'lagcheck' WHERE id = 1"))
        for _ in range(20):
            if await consumer.poll(timeout_ms=1000):
                break

        report = await wait_for_lag(
            source,
            handoff,
            measure_lag=consumer.lag,
            threshold=timedelta(seconds=30),
            give_up_after=timedelta(seconds=30),
        )
        assert report.caught_up, report.describe()
        assert report.retained_wal_bytes >= 0
    finally:
        await consumer.stop()


async def test_a_snapshot_does_not_overwrite_newer_changes(
    source: AsyncEngine, target: AsyncEngine, handoff: Handoff
) -> None:
    """The failure the stamping exists to prevent.

    A change applied by CDC, then a snapshot partition copied afterwards, must
    leave the CDC value in place - otherwise a slow partition silently undoes
    work the stream already did, and the row still looks consistent.
    """
    manifest = await manifest_of(source)
    target_adapter = PostgresTargetAdapter(target)
    PostgresSourceAdapter(source)
    await target_adapter.prepare(manifest, target=TABLE)

    # A change from after the snapshot position lands first.
    async with transaction(target) as connection:
        await connection.execute(
            text(f"INSERT INTO {TABLE} (id, label, source_lsn) VALUES (1, 'from-cdc', :lsn)"),
            {"lsn": handoff.snapshot_lsn + 1000},
        )

    partition = plan_partitions(manifest, target_partitions=1).partitions[0]
    await move_partition(manifest, partition, target=TABLE, snapshot_lsn=handoff.snapshot_lsn)

    async with transaction(target) as connection:
        label = (
            await connection.execute(text(f"SELECT label FROM {TABLE} WHERE id = 1"))
        ).scalar_one()
    assert label == "from-cdc", "the snapshot overwrote a newer change"


async def test_a_change_already_in_the_snapshot_is_refused(
    source: AsyncEngine, target: AsyncEngine, handoff: Handoff
) -> None:
    """Overlap between the snapshot and the stream is absorbed, not avoided.

    Changes between the slot's creation and the snapshot position appear in
    both phases. Applying such a change again must leave the target where the
    snapshot put it - which is what lets the two overlap instead of requiring a
    lock on the source.
    """
    manifest = await manifest_of(source)
    await _snapshot_once(source, target, manifest, handoff)
    applier = await _applier(target, manifest)

    stale = _change(handoff, lsn=handoff.snapshot_lsn - 1, label="from-before-the-snapshot")
    report = await applier.apply([stale])

    assert report.applied == 0
    assert report.rejected_stale == 1
    assert await _label(target, 1) != "from-before-the-snapshot"


async def test_a_change_after_the_snapshot_is_applied(
    source: AsyncEngine, target: AsyncEngine, handoff: Handoff
) -> None:
    """The guard must not refuse everything; newer changes still have to win."""
    manifest = await manifest_of(source)
    await _snapshot_once(source, target, manifest, handoff)
    applier = await _applier(target, manifest)

    fresh = _change(handoff, lsn=handoff.snapshot_lsn + 1, label="from-after-the-snapshot")
    report = await applier.apply([fresh])

    assert report.applied == 1
    assert await _label(target, 1) == "from-after-the-snapshot"


async def _snapshot_once(
    source: AsyncEngine, target: AsyncEngine, manifest: DatasetManifest, handoff: Handoff
) -> None:
    target_adapter = PostgresTargetAdapter(target)
    PostgresSourceAdapter(source)
    await target_adapter.prepare(manifest, target=TABLE)
    partition = plan_partitions(manifest, target_partitions=1).partitions[0]
    await move_partition(manifest, partition, target=TABLE, snapshot_lsn=handoff.snapshot_lsn)


async def _applier(target: AsyncEngine, manifest: DatasetManifest) -> CDCApplier:
    applier = CDCApplier(target, operation=OPERATION, manifest=manifest, target=TABLE)
    await applier.ensure_tombstones()
    return applier


def _change(handoff: Handoff, *, lsn: int, label: str) -> ChangeEvent:
    return ChangeEvent(
        dataset=TABLE,
        operation=ChangeOperation.UPDATE,
        key={"id": "1"},
        after={"id": 1, "label": label, "source_lsn": 1},
        source_lsn=lsn,
        source_timestamp=datetime.now(UTC),
        stream_position=StreamPosition(topic=handoff.topics[0], partition=0, offset=0),
    )


async def _label(target: AsyncEngine, key: int) -> str:
    async with transaction(target) as connection:
        return str(
            (
                await connection.execute(
                    text(f"SELECT label FROM {TABLE} WHERE id = :id"),
                    {"id": key},
                )
            ).scalar_one()
        )
