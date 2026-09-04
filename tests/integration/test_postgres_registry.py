"""The durable registry against a real database.

Requires the local stack: make dev-up && alembic upgrade head.

These run the same contract as the in-memory and JSON stores, plus the
concurrency guarantee only a database can make.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from gantry.core import DatasetManifest, DatasetRef, DatasetSchema, PhysicalRef
from gantry.registry import DatasetNotFoundError, DatasetRegistry, DatasetVersionNotFoundError
from gantry.state.database import create_engine, transaction
from gantry.state.registry import PostgresDatasetRegistry
from gantry.state.tables import dataset_versions
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    created = create_engine()
    try:
        async with transaction(created) as connection:
            await connection.execute(delete(dataset_versions))
        yield created
    finally:
        await created.dispose()


@pytest.fixture
def registry(engine: AsyncEngine) -> DatasetRegistry:
    return PostgresDatasetRegistry(engine)


def manifest(name: str = "orders", rows: int | None = None) -> DatasetManifest:
    return DatasetManifest(
        name=name,
        physical=PhysicalRef(adapter="postgres", reference="public.orders", estimated_rows=rows),
        dataset_schema=DatasetSchema(keys=("order_id",)),
    )


async def test_first_registration_is_version_one(registry: DatasetRegistry) -> None:
    registered = await registry.register(manifest())
    assert registered.version == 1
    assert registered.content_hash == manifest().content_hash


async def test_registering_unchanged_content_is_idempotent(registry: DatasetRegistry) -> None:
    first = await registry.register(manifest())
    again = await registry.register(manifest())
    assert again.version == first.version
    assert again.registered_at == first.registered_at
    assert len(await registry.versions("orders")) == 1


async def test_changed_content_creates_the_next_version(registry: DatasetRegistry) -> None:
    await registry.register(manifest())
    second = await registry.register(manifest(rows=100))
    assert second.version == 2


async def test_manifest_round_trips_through_the_database(registry: DatasetRegistry) -> None:
    source = manifest(rows=42)
    await registry.register(source)
    loaded = (await registry.get(DatasetRef(name="orders"))).manifest
    assert loaded == source
    assert loaded.content_hash == source.content_hash


async def test_unpinned_ref_resolves_to_latest(registry: DatasetRegistry) -> None:
    await registry.register(manifest())
    await registry.register(manifest(rows=100))
    assert (await registry.get(DatasetRef(name="orders"))).version == 2


async def test_pinned_version_is_immutable_once_superseded(registry: DatasetRegistry) -> None:
    original = await registry.register(manifest())
    await registry.register(manifest(rows=100))
    assert await registry.get(DatasetRef(name="orders", version=1)) == original


async def test_unknown_dataset_raises(registry: DatasetRegistry) -> None:
    with pytest.raises(DatasetNotFoundError, match="ghost"):
        await registry.get(DatasetRef(name="ghost"))


async def test_unknown_version_reports_the_latest(registry: DatasetRegistry) -> None:
    await registry.register(manifest())
    with pytest.raises(DatasetVersionNotFoundError, match="latest is 1"):
        await registry.get(DatasetRef(name="orders", version=7))


async def test_list_returns_latest_of_each(registry: DatasetRegistry) -> None:
    await registry.register(manifest(name="orders"))
    await registry.register(manifest(name="orders", rows=5))
    await registry.register(manifest(name="customers"))
    listed = await registry.list()
    assert [(e.name, e.version) for e in listed] == [("customers", 1), ("orders", 2)]


async def test_concurrent_identical_registration_yields_one_version(engine: AsyncEngine) -> None:
    """The race is settled in the database, not in application code.

    Ten processes registering the same manifest must produce one version, or
    provenance pins referencing it would be split across duplicates.
    """
    registries = [PostgresDatasetRegistry(engine) for _ in range(10)]
    results = await asyncio.gather(*(store.register(manifest()) for store in registries))

    assert {version.version for version in results} == {1}
    assert len(await registries[0].versions("orders")) == 1
