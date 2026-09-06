"""The Postgres target adapter against a real database.

Requires the local stack and a seeded source:
    make dev-up && uv run gantry seed --rows 1000000

The claim under test is the one every retry depends on: writing the same rows
twice leaves the target exactly as it was after the first write.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.adapters.target.postgres import PostgresTargetAdapter, UnpreparedTargetError
from gantry.core.dataset import DatasetManifest
from gantry.movement.partitioning import Partition, plan_partitions
from gantry.state.database import create_engine, transaction
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
TARGET_TABLE = "public.orders_day8"

# Small enough to keep the suite quick, large enough to be a real batch.
PARTITIONS = 2000


@pytest.fixture
async def source() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(SOURCE_URL)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def target(source: AsyncEngine) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(TARGET_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TARGET_TABLE}"))
        yield engine
    finally:
        await engine.dispose()


async def fixture_manifest(source: AsyncEngine) -> DatasetManifest:
    adapter = PostgresSourceAdapter(source)
    manifests = {m.name: m for m in await adapter.discover()}
    assert "public.orders" in manifests, "seed the source first"
    return await adapter.profile(manifests["public.orders"])


async def one_partition(source: AsyncEngine, manifest: DatasetManifest) -> Partition:
    return plan_partitions(manifest, target_partitions=PARTITIONS).partitions[7]


async def count(engine: AsyncEngine) -> int:
    async with transaction(engine) as connection:
        return int(
            (await connection.execute(text(f"SELECT count(*) FROM {TARGET_TABLE}"))).scalar_one()
        )


# --- prepare ---------------------------------------------------------------


async def test_prepare_creates_the_target(source: AsyncEngine, target: AsyncEngine) -> None:
    manifest = await fixture_manifest(source)
    await PostgresTargetAdapter(target).prepare(manifest, target=TARGET_TABLE)
    assert await count(target) == 0


async def test_prepare_is_idempotent(source: AsyncEngine, target: AsyncEngine) -> None:
    """Prepare runs again on every restart."""
    manifest = await fixture_manifest(source)
    adapter = PostgresTargetAdapter(target)
    await adapter.prepare(manifest, target=TARGET_TABLE)
    await adapter.prepare(manifest, target=TARGET_TABLE)


async def test_prepare_creates_the_primary_key(source: AsyncEngine, target: AsyncEngine) -> None:
    """Idempotent upserts need a key to detect a conflict against."""
    manifest = await fixture_manifest(source)
    await PostgresTargetAdapter(target).prepare(manifest, target=TARGET_TABLE)
    async with transaction(target) as connection:
        indexes = (
            (
                await connection.execute(
                    text(
                        "SELECT indexname FROM pg_indexes "
                        "WHERE schemaname = 'public' AND tablename = 'orders_day8'"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert any("pkey" in name for name in indexes)


async def test_prepare_defers_secondary_indexes(source: AsyncEngine, target: AsyncEngine) -> None:
    """Every index is maintained on each insert; only the key is worth that.

    The source carries ix_orders_created_at, and the target must not.
    """
    manifest = await fixture_manifest(source)
    assert any(index.name == "ix_orders_created_at" for index in manifest.dataset_schema.indexes), (
        "source fixture should have a secondary index"
    )

    await PostgresTargetAdapter(target).prepare(manifest, target=TARGET_TABLE)
    async with transaction(target) as connection:
        indexes = (
            (
                await connection.execute(
                    text(
                        "SELECT indexname FROM pg_indexes "
                        "WHERE schemaname = 'public' AND tablename = 'orders_day8'"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(indexes) == 1, f"only the primary key should exist, found {indexes}"


async def test_prepare_refuses_a_dataset_without_a_key(
    source: AsyncEngine, target: AsyncEngine
) -> None:
    manifest = await fixture_manifest(source)
    keyless = manifest.model_copy(
        update={"dataset_schema": manifest.dataset_schema.model_copy(update={"keys": ()})}
    )
    with pytest.raises(UnpreparedTargetError, match="stable key"):
        await PostgresTargetAdapter(target).prepare(keyless, target=TARGET_TABLE)


# --- idempotent writes -----------------------------------------------------


async def test_replaying_a_batch_changes_nothing(source: AsyncEngine, target: AsyncEngine) -> None:
    """The property every retry in the runtime depends on."""
    manifest = await fixture_manifest(source)
    source_adapter = PostgresSourceAdapter(source)
    adapter = PostgresTargetAdapter(target)
    await adapter.prepare(manifest, target=TARGET_TABLE)

    partition = await one_partition(source, manifest)
    rows = [
        row async for batch in source_adapter.read_partition(manifest, partition) for row in batch
    ]
    assert rows, "partition should not be empty"

    first = await adapter.write_batch(manifest, target=TARGET_TABLE, rows=rows)
    assert first.rows_inserted == len(rows)
    assert not first.is_noop

    after_first = await count(target)
    replay = await adapter.write_batch(manifest, target=TARGET_TABLE, rows=rows)

    assert replay.is_noop
    assert replay.rows_changed == 0
    assert replay.rows_unchanged == len(rows)
    assert await count(target) == after_first


async def test_a_changed_row_is_updated(source: AsyncEngine, target: AsyncEngine) -> None:
    """Idempotency must not mean ignoring genuine changes."""
    manifest = await fixture_manifest(source)
    source_adapter = PostgresSourceAdapter(source)
    adapter = PostgresTargetAdapter(target)
    await adapter.prepare(manifest, target=TARGET_TABLE)

    partition = await one_partition(source, manifest)
    rows = [
        row async for batch in source_adapter.read_partition(manifest, partition) for row in batch
    ][:100]
    await adapter.write_batch(manifest, target=TARGET_TABLE, rows=rows)

    status_index = [f.name for f in manifest.dataset_schema.fields].index("status")
    mutated = [
        tuple("CHANGED" if i == status_index else value for i, value in enumerate(row))
        for row in rows
    ]
    result = await adapter.write_batch(manifest, target=TARGET_TABLE, rows=mutated)

    assert result.rows_updated == len(rows)
    assert result.rows_inserted == 0


async def test_writing_no_rows_is_a_noop(source: AsyncEngine, target: AsyncEngine) -> None:
    manifest = await fixture_manifest(source)
    adapter = PostgresTargetAdapter(target)
    await adapter.prepare(manifest, target=TARGET_TABLE)
    assert (await adapter.write_batch(manifest, target=TARGET_TABLE, rows=[])).is_noop


# --- the bulk path ---------------------------------------------------------


async def test_the_bulk_path_moves_rows_without_materialising_them(
    source: AsyncEngine, target: AsyncEngine
) -> None:
    """The rows never become Python objects: they go source COPY to target COPY
    inside a container this process only starts and watches."""
    manifest = await fixture_manifest(source)
    adapter = PostgresTargetAdapter(target)
    await adapter.prepare(manifest, target=TARGET_TABLE)

    partition = await one_partition(source, manifest)
    result = await move_partition(manifest, partition, target=TARGET_TABLE)

    assert result.rows_inserted > 0
    assert await count(target) == result.rows_inserted


async def test_the_bulk_path_is_idempotent(source: AsyncEngine, target: AsyncEngine) -> None:
    manifest = await fixture_manifest(source)
    adapter = PostgresTargetAdapter(target)
    await adapter.prepare(manifest, target=TARGET_TABLE)

    partition = await one_partition(source, manifest)
    first = await move_partition(manifest, partition, target=TARGET_TABLE)
    replay = await move_partition(manifest, partition, target=TARGET_TABLE)

    assert replay.rows_inserted == 0, "a replay must not report new rows"
    assert replay.rows_updated == first.rows_inserted
    assert await count(target) == first.rows_inserted


async def test_both_write_paths_agree(source: AsyncEngine, target: AsyncEngine) -> None:
    """The bulk path and the correctness path must produce the same target."""
    manifest = await fixture_manifest(source)
    source_adapter = PostgresSourceAdapter(source)
    adapter = PostgresTargetAdapter(target)
    await adapter.prepare(manifest, target=TARGET_TABLE)

    partition = await one_partition(source, manifest)
    await move_partition(manifest, partition, target=TARGET_TABLE)

    rows = [
        row async for batch in source_adapter.read_partition(manifest, partition) for row in batch
    ]
    # Writing the same rows through the other path must change nothing.
    result = await adapter.write_batch(manifest, target=TARGET_TABLE, rows=rows)
    assert result.is_noop, "the two paths disagree about what was written"
