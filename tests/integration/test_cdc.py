"""CDC against real Debezium and real Kafka.

Requires the full stack: make dev-up && uv run alembic upgrade head

Nothing here is mocked. The failures CDC actually has - slot lifecycle, LSN
handling, resumption, WAL retention - are precisely the ones a fake papers
over.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from gantry.adapters.cdc.debezium import (
    ConnectorError,
    ConnectorState,
    DebeziumConfig,
    DebeziumConnectClient,
)
from gantry.adapters.cdc.kafka import KafkaCDCAdapter
from gantry.adapters.cdc.progress import CDCProgress
from gantry.adapters.cdc.slots import (
    SlotBusyError,
    WalRetentionError,
    check_wal_retention,
    drop_slot,
    slot_status,
    wait_for_slot,
)
from gantry.core.changes import ChangeOperation
from gantry.state.checkpoints import PostgresCheckpointStore
from gantry.state.database import create_engine, transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
CONNECT_URL = os.environ.get("GANTRY_CONNECT_URL", "http://localhost:18083")
BOOTSTRAP = os.environ.get("GANTRY_KAFKA_BOOTSTRAP", "localhost:19092")

TABLE = "public.cdc_probe"
OPERATION = "cdc-integration"


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
                    f"  label text,"
                    f"  amount numeric(12,2)"
                    f")"
                )
            )
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def connector(source: AsyncEngine) -> AsyncIterator[DebeziumConfig]:
    """A real Debezium connector, torn down afterwards.

    Teardown is not tidiness: a slot left behind pins WAL on the source until
    someone notices, which is usually when the disk fills.
    """
    suffix = uuid.uuid4().hex[:8]
    config = DebeziumConfig(
        name=f"gantry-test-{suffix}",
        database_host="pg-source",
        database_port=5432,
        database_user="gantry",
        database_password="gantry",
        database_name="gantry",
        tables=(TABLE,),
        topic_prefix=f"gtest{suffix}",
        slot_name=f"gantry_test_{suffix}",
        publication_name=f"gantry_test_pub_{suffix}",
    )
    client = DebeziumConnectClient(CONNECT_URL)
    await client.ensure(config)
    # RUNNING is not the same as capturing: wait for the slot to actually exist.
    await wait_for_slot(source, config.slot_name)
    try:
        yield config
    finally:
        await client.delete(config.name)
        # drop_slot waits for the connector to let go; dropping immediately
        # fails and leaves the slot pinning WAL.
        with contextlib.suppress(SlotBusyError):
            await drop_slot(source, config.slot_name)


async def write(engine: AsyncEngine, statement: str) -> None:
    async with transaction(engine) as connection:
        await connection.execute(text(statement))


async def drain(adapter: KafkaCDCAdapter, *, expected: int, attempts: int = 12) -> list[object]:
    events: list[object] = []
    for _ in range(attempts):
        events.extend(await adapter.poll(timeout_ms=1500))
        if len(events) >= expected:
            break
    return events


# --- connector lifecycle ---------------------------------------------------


async def test_provisioning_creates_a_running_connector(connector: DebeziumConfig) -> None:
    status = await DebeziumConnectClient(CONNECT_URL).status(connector.name)
    assert status.state is ConnectorState.RUNNING
    assert status.healthy


async def test_provisioning_creates_a_replication_slot(
    source: AsyncEngine, connector: DebeziumConfig
) -> None:
    status = await slot_status(source, connector.slot_name)
    assert status.exists
    assert status.active


async def test_provisioning_is_idempotent(connector: DebeziumConfig) -> None:
    """Prepare runs again on every restart and must converge, not fail."""
    client = DebeziumConnectClient(CONNECT_URL)
    again = await client.ensure(connector)
    assert again.healthy


async def test_an_absent_connector_reports_absent_rather_than_raising() -> None:
    status = await DebeziumConnectClient(CONNECT_URL).status("no-such-connector")
    assert status.state is ConnectorState.ABSENT


async def test_deleting_releases_the_slot(source: AsyncEngine, connector: DebeziumConfig) -> None:
    """An orphaned slot pins WAL forever, so teardown is a correctness concern."""
    client = DebeziumConnectClient(CONNECT_URL)
    assert await client.delete(connector.name)
    # The slot is not free the instant the connector is deleted; drop_slot
    # waits for it rather than failing and leaving it behind.
    assert await drop_slot(source, connector.slot_name)
    assert not (await slot_status(source, connector.slot_name)).exists


async def test_deleting_something_absent_is_not_an_error() -> None:
    assert not await DebeziumConnectClient(CONNECT_URL).delete("no-such-connector")


async def test_a_connector_that_cannot_start_reports_why() -> None:
    """A failure has to carry its trace; retrying a bad config never helps."""
    client = DebeziumConnectClient(CONNECT_URL)
    broken = DebeziumConfig(
        name=f"gantry-broken-{uuid.uuid4().hex[:8]}",
        database_host="nonexistent-host",
        database_port=5432,
        database_user="gantry",
        database_password="gantry",
        database_name="gantry",
        tables=(TABLE,),
        topic_prefix="broken",
        slot_name="broken_slot",
        publication_name="broken_pub",
    )
    try:
        with pytest.raises(ConnectorError):
            await client.wait_until_running(
                (await _provision_ignoring_result(client, broken)), timeout_seconds=25
            )
    finally:
        await client.delete(broken.name)


async def _provision_ignoring_result(client: DebeziumConnectClient, config: DebeziumConfig) -> str:
    with contextlib.suppress(ConnectorError):
        await client.ensure(config)
    return config.name


# --- events ----------------------------------------------------------------


async def test_changes_flow_from_source_to_adapter(
    source: AsyncEngine, connector: DebeziumConfig
) -> None:
    adapter = KafkaCDCAdapter(
        topics=[connector.topic_for(TABLE)],
        bootstrap_servers=BOOTSTRAP,
        group_id=f"gantry-test-{uuid.uuid4().hex[:8]}",
    )
    await adapter.start()
    try:
        await write(source, f"INSERT INTO {TABLE} VALUES (1, 'one', 1.50)")
        await write(source, f"UPDATE {TABLE} SET label = 'uno' WHERE id = 1")
        await write(source, f"DELETE FROM {TABLE} WHERE id = 1")

        events = await drain(adapter, expected=3)
        operations = [event.operation for event in events]  # type: ignore[attr-defined]
        assert ChangeOperation.INSERT in operations
        assert ChangeOperation.UPDATE in operations
        assert ChangeOperation.DELETE in operations
    finally:
        await adapter.stop()


async def test_events_carry_a_monotonic_source_lsn(
    source: AsyncEngine, connector: DebeziumConfig
) -> None:
    """Stale-write rejection has nothing to compare without this."""
    adapter = KafkaCDCAdapter(
        topics=[connector.topic_for(TABLE)],
        bootstrap_servers=BOOTSTRAP,
        group_id=f"gantry-test-{uuid.uuid4().hex[:8]}",
    )
    await adapter.start()
    try:
        for index in range(3):
            await write(source, f"INSERT INTO {TABLE} VALUES ({index + 10}, 'x', 1)")
        events = await drain(adapter, expected=3)
        lsns = [event.source_lsn for event in events]  # type: ignore[attr-defined]
        assert len(lsns) >= 3
        assert lsns == sorted(lsns), "LSNs must not go backwards within a stream"
        assert len(set(lsns)) == len(lsns), "each change has its own position"
    finally:
        await adapter.stop()


async def test_lag_is_measured_against_the_source_clock(
    source: AsyncEngine, connector: DebeziumConfig
) -> None:
    """Against the source's timestamp, so a quiet source is not read as lag."""
    adapter = KafkaCDCAdapter(
        topics=[connector.topic_for(TABLE)],
        bootstrap_servers=BOOTSTRAP,
        group_id=f"gantry-test-{uuid.uuid4().hex[:8]}",
    )
    await adapter.start()
    try:
        assert (await adapter.lag()).total_seconds() == 0, "no events, no lag"
        await write(source, f"INSERT INTO {TABLE} VALUES (99, 'lag', 1)")
        await drain(adapter, expected=1)
        assert (await adapter.lag()).total_seconds() < 30
    finally:
        await adapter.stop()


# --- the exit criterion ----------------------------------------------------


async def test_a_position_survives_a_consumer_restart(
    source: AsyncEngine, connector: DebeziumConfig
) -> None:
    """Resumption uses Gantry's recorded position, not a consumer group's.

    A consumer group commit is a separate durability domain from the target
    write; trusting it would mean believing progress the runtime had not made.
    """
    meta = create_engine(META_URL)
    try:
        async with transaction(meta) as connection:
            await connection.execute(
                text("DELETE FROM checkpoints WHERE operation = :name"), {"name": OPERATION}
            )
            await connection.execute(
                text("DELETE FROM operations WHERE name = :name"), {"name": OPERATION}
            )
            await connection.execute(
                text(
                    "INSERT INTO operations (name, operation_type, state, created_at, updated_at)"
                    " VALUES (:name, 'movement', 'executing', now(), now())"
                ),
                {"name": OPERATION},
            )
        progress = CDCProgress(PostgresCheckpointStore(meta), OPERATION)
        topic = connector.topic_for(TABLE)

        for index in range(4):
            await write(source, f"INSERT INTO {TABLE} VALUES ({index + 100}, 'r', 1)")

        first = KafkaCDCAdapter(
            topics=[topic], bootstrap_servers=BOOTSTRAP, group_id=f"g1-{uuid.uuid4().hex[:6]}"
        )
        await first.start()
        applied = []
        for event in await drain(first, expected=2):
            applied.append(event)
            await progress.record(
                event.stream_position,  # type: ignore[attr-defined]
                source_lsn=event.source_lsn,  # type: ignore[attr-defined]
                committed_at=datetime.now(UTC),
            )
            if len(applied) == 2:
                break
        await first.stop()
        assert len(applied) == 2

        resume = await progress.resume_position(topic)
        assert resume is not None
        assert resume.offset == applied[-1].stream_position.offset  # type: ignore[attr-defined]

        # A brand new consumer group: nothing but the recorded position tells
        # it where to start.
        second = KafkaCDCAdapter(
            topics=[topic], bootstrap_servers=BOOTSTRAP, group_id=f"g2-{uuid.uuid4().hex[:6]}"
        )
        await second.start(resume_from=resume)
        remaining = await drain(second, expected=1)
        await second.stop()

        assert remaining, "the rest of the stream should still be there"
        assert remaining[0].stream_position.offset == resume.offset + 1  # type: ignore[attr-defined]
        assert await progress.applied_lsn(topic) == applied[-1].source_lsn  # type: ignore[attr-defined]
    finally:
        await meta.dispose()


# --- WAL retention ---------------------------------------------------------


async def test_slot_retention_is_measured(source: AsyncEngine, connector: DebeziumConfig) -> None:
    status = await slot_status(source, connector.slot_name)
    assert status.exists
    assert status.retained_bytes >= 0
    assert status.current_lsn is not None


async def test_excess_retention_stops_the_stream(
    source: AsyncEngine, connector: DebeziumConfig
) -> None:
    """Continuing while the source fills its disk trades a stalled migration
    for a stopped database."""
    await write(source, f"INSERT INTO {TABLE} VALUES (500, 'wal', 1)")
    with pytest.raises(WalRetentionError, match="stop the source accepting writes"):
        await check_wal_retention(source, connector.slot_name, critical_bytes=0)


async def test_retention_within_policy_passes(
    source: AsyncEngine, connector: DebeziumConfig
) -> None:
    status = await check_wal_retention(source, connector.slot_name)
    assert status.exists
