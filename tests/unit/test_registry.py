"""Registry semantics, exercised against every implementation of the Protocol."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from gantry.core import DatasetManifest, DatasetRef, DatasetSchema, PhysicalRef
from gantry.registry import (
    DatasetNotFoundError,
    DatasetRegistry,
    DatasetVersionNotFoundError,
    InMemoryDatasetRegistry,
    JsonFileDatasetRegistry,
)


def ticking_clock() -> Callable[[], datetime]:
    moments = iter(datetime(2026, 9, 8, tzinfo=UTC) + timedelta(minutes=i) for i in range(1000))
    return lambda: next(moments)


@pytest.fixture(params=["memory", "jsonfile"])
def registry(request: pytest.FixtureRequest, tmp_path: Path) -> DatasetRegistry:
    """Both implementations must satisfy the same contract."""
    if request.param == "memory":
        return InMemoryDatasetRegistry(clock=ticking_clock())
    return JsonFileDatasetRegistry(tmp_path / "registry.json", clock=ticking_clock())


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
    assert len(await registry.versions("orders")) == 2


async def test_unpinned_ref_resolves_to_latest(registry: DatasetRegistry) -> None:
    await registry.register(manifest())
    await registry.register(manifest(rows=100))
    assert (await registry.get(DatasetRef(name="orders"))).version == 2


async def test_pinned_ref_resolves_to_that_version(registry: DatasetRegistry) -> None:
    await registry.register(manifest())
    await registry.register(manifest(rows=100))
    pinned = await registry.get(DatasetRef(name="orders", version=1))
    assert pinned.version == 1
    assert pinned.manifest.physical.estimated_rows is None


async def test_pinned_versions_are_immutable_once_superseded(registry: DatasetRegistry) -> None:
    """Provenance pins a version; a later registration must not change it."""
    first = await registry.register(manifest())
    original = await registry.get(DatasetRef(name="orders", version=first.version))
    await registry.register(manifest(rows=100))
    assert await registry.get(DatasetRef(name="orders", version=1)) == original


async def test_unknown_dataset_raises(registry: DatasetRegistry) -> None:
    with pytest.raises(DatasetNotFoundError, match="ghost"):
        await registry.get(DatasetRef(name="ghost"))
    with pytest.raises(DatasetNotFoundError):
        await registry.versions("ghost")


async def test_unknown_version_reports_the_latest(registry: DatasetRegistry) -> None:
    await registry.register(manifest())
    with pytest.raises(DatasetVersionNotFoundError, match="latest is 1"):
        await registry.get(DatasetRef(name="orders", version=7))


async def test_list_returns_latest_of_each_sorted_by_name(registry: DatasetRegistry) -> None:
    await registry.register(manifest(name="orders"))
    await registry.register(manifest(name="orders", rows=5))
    await registry.register(manifest(name="customers"))
    listed = await registry.list()
    assert [(e.name, e.version) for e in listed] == [("customers", 1), ("orders", 2)]


async def test_empty_registry_lists_nothing(registry: DatasetRegistry) -> None:
    assert await registry.list() == ()


async def test_versions_are_ordered_oldest_first(registry: DatasetRegistry) -> None:
    await registry.register(manifest())
    await registry.register(manifest(rows=1))
    await registry.register(manifest(rows=2))
    assert [v.version for v in await registry.versions("orders")] == [1, 2, 3]


async def test_jsonfile_registry_persists_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    await JsonFileDatasetRegistry(path, clock=ticking_clock()).register(manifest())
    reopened = JsonFileDatasetRegistry(path, clock=ticking_clock())
    assert (await reopened.get(DatasetRef(name="orders"))).version == 1


async def test_jsonfile_registry_round_trips_the_manifest(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    source = manifest(rows=42)
    await JsonFileDatasetRegistry(path, clock=ticking_clock()).register(source)
    loaded = (await JsonFileDatasetRegistry(path).get(DatasetRef(name="orders"))).manifest
    assert loaded == source
    assert loaded.content_hash == source.content_hash


async def test_jsonfile_registry_leaves_no_temp_files(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    store = JsonFileDatasetRegistry(path, clock=ticking_clock())
    await store.register(manifest())
    await store.register(manifest(rows=1))
    listing = await asyncio.to_thread(lambda: sorted(p.name for p in tmp_path.iterdir()))
    assert listing == ["registry.json"]


async def test_missing_registry_file_reads_as_empty(tmp_path: Path) -> None:
    assert await JsonFileDatasetRegistry(tmp_path / "absent.json").list() == ()


async def test_implementations_satisfy_the_protocol(tmp_path: Path) -> None:
    """Structural conformance is checked by mypy at these annotations."""
    in_memory: DatasetRegistry = InMemoryDatasetRegistry()
    on_disk: DatasetRegistry = JsonFileDatasetRegistry(tmp_path / "registry.json")
    assert await in_memory.list() == ()
    assert await on_disk.list() == ()
