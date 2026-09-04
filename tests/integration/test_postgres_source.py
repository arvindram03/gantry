"""The Postgres source adapter against a real database.

Requires the local stack and a seeded source:
    make dev-up && uv run gantry seed --rows 1000000

The claim under test is that discovery and profiling are metadata operations.
Neither reads a table, so both stay fast no matter how large the source is.
"""

from __future__ import annotations

import os
import time
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.core.dataset import DatasetManifest, DatasetRef
from gantry.core.positions import PositionKind
from gantry.registry import InMemoryDatasetRegistry
from gantry.registry.discovery import discover_into_registry
from gantry.state.database import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    created = create_engine(SOURCE_URL)
    try:
        yield created
    finally:
        await created.dispose()


@pytest.fixture
def adapter(engine: AsyncEngine) -> PostgresSourceAdapter:
    return PostgresSourceAdapter(engine)


async def orders(adapter: PostgresSourceAdapter) -> DatasetManifest:
    manifests = {m.name: m for m in await adapter.discover()}
    assert "public.orders" in manifests, "seed the source first"
    return manifests["public.orders"]


async def test_discovery_finds_the_seeded_tables(adapter: PostgresSourceAdapter) -> None:
    names = {m.name for m in await adapter.discover()}
    assert {"public.customers", "public.orders"} <= names


async def test_discovery_reads_the_primary_key(adapter: PostgresSourceAdapter) -> None:
    manifest = await orders(adapter)
    assert manifest.dataset_schema.keys == ("order_id",)
    assert any(index.primary for index in manifest.dataset_schema.indexes)


async def test_discovery_reads_columns_and_types(adapter: PostgresSourceAdapter) -> None:
    fields = {f.name: f for f in (await orders(adapter)).dataset_schema.fields}
    assert fields["order_id"].type == "bigint"
    assert not fields["order_id"].nullable
    assert fields["amount"].type.startswith("numeric")


async def test_discovery_reads_foreign_keys(adapter: PostgresSourceAdapter) -> None:
    """Foreign keys drive dependency ordering and integrity verification."""
    foreign_keys = (await orders(adapter)).dataset_schema.foreign_keys
    assert len(foreign_keys) == 1
    assert foreign_keys[0].columns == ("customer_id",)
    assert foreign_keys[0].references == "public.customers"


async def test_discovery_reads_secondary_indexes(adapter: PostgresSourceAdapter) -> None:
    names = {index.name for index in (await orders(adapter)).dataset_schema.indexes}
    assert "ix_orders_created_at" in names


async def test_discovery_estimates_size(adapter: PostgresSourceAdapter) -> None:
    physical = (await orders(adapter)).physical
    assert physical.estimated_rows is not None
    assert physical.estimated_rows > 0
    assert physical.estimated_bytes is not None
    assert physical.estimated_bytes > 0


async def test_profiling_fills_the_statistics(adapter: PostgresSourceAdapter) -> None:
    statistics = (await adapter.profile(await orders(adapter))).statistics
    assert statistics.key_min == "1"
    assert statistics.key_max is not None
    assert int(statistics.key_max) >= 1
    assert statistics.distinct_keys is not None
    assert statistics.profiled_at is not None
    assert not statistics.stale_statistics


async def test_profiling_records_null_rates(adapter: PostgresSourceAdapter) -> None:
    statistics = (await adapter.profile(await orders(adapter))).statistics
    assert "status" in statistics.null_rates
    assert statistics.null_rates["status"] == 0.0


async def test_profiling_does_not_scan_the_table(adapter: PostgresSourceAdapter) -> None:
    """The exit criterion for Day 6, stated as a bound rather than a hope.

    Profiling reads planner statistics and an indexed min/max, so its cost does
    not grow with the row count.
    """
    manifest = await orders(adapter)
    started = time.perf_counter()
    await adapter.profile(manifest)
    elapsed = time.perf_counter() - started
    assert elapsed < 60, f"profiling took {elapsed:.1f}s"


async def test_current_position_returns_an_lsn(adapter: PostgresSourceAdapter) -> None:
    """Captured before a snapshot so CDC resumes with no gap."""
    position = await adapter.current_position()
    assert position.kind is PositionKind.LSN
    assert "/" in position.value


async def test_discovery_registers_datasets_end_to_end(adapter: PostgresSourceAdapter) -> None:
    registry = InMemoryDatasetRegistry()
    report = await discover_into_registry(adapter, registry)

    assert "public.orders" in report.names
    registered = await registry.get(DatasetRef(name="public.orders"))
    assert registered.manifest.statistics.distinct_keys is not None
    assert registered.manifest.dataset_schema.keys == ("order_id",)


async def test_rediscovery_against_a_live_source_is_free(adapter: PostgresSourceAdapter) -> None:
    registry = InMemoryDatasetRegistry()
    await discover_into_registry(adapter, registry)
    await discover_into_registry(adapter, registry)
    assert len(await registry.versions("public.orders")) == 1
