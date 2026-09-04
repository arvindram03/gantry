"""Registry semantics, exercised against every implementation of the Protocol."""

from __future__ import annotations

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


def test_first_registration_is_version_one(registry: DatasetRegistry) -> None:
    registered = registry.register(manifest())
    assert registered.version == 1
    assert registered.content_hash == manifest().content_hash


def test_registering_unchanged_content_is_idempotent(registry: DatasetRegistry) -> None:
    first = registry.register(manifest())
    again = registry.register(manifest())
    assert again.version == first.version
    assert again.registered_at == first.registered_at
    assert len(registry.versions("orders")) == 1


def test_changed_content_creates_the_next_version(registry: DatasetRegistry) -> None:
    registry.register(manifest())
    second = registry.register(manifest(rows=100))
    assert second.version == 2
    assert len(registry.versions("orders")) == 2


def test_unpinned_ref_resolves_to_latest(registry: DatasetRegistry) -> None:
    registry.register(manifest())
    registry.register(manifest(rows=100))
    assert registry.get(DatasetRef(name="orders")).version == 2


def test_pinned_ref_resolves_to_that_version(registry: DatasetRegistry) -> None:
    registry.register(manifest())
    registry.register(manifest(rows=100))
    pinned = registry.get(DatasetRef(name="orders", version=1))
    assert pinned.version == 1
    assert pinned.manifest.physical.estimated_rows is None


def test_pinned_versions_are_immutable_once_superseded(registry: DatasetRegistry) -> None:
    """Provenance pins a version; a later registration must not change it."""
    first = registry.register(manifest())
    original = registry.get(DatasetRef(name="orders", version=first.version))
    registry.register(manifest(rows=100))
    assert registry.get(DatasetRef(name="orders", version=1)) == original


def test_unknown_dataset_raises(registry: DatasetRegistry) -> None:
    with pytest.raises(DatasetNotFoundError, match="ghost"):
        registry.get(DatasetRef(name="ghost"))
    with pytest.raises(DatasetNotFoundError):
        registry.versions("ghost")


def test_unknown_version_reports_the_latest(registry: DatasetRegistry) -> None:
    registry.register(manifest())
    with pytest.raises(DatasetVersionNotFoundError, match="latest is 1"):
        registry.get(DatasetRef(name="orders", version=7))


def test_list_returns_latest_of_each_sorted_by_name(registry: DatasetRegistry) -> None:
    registry.register(manifest(name="orders"))
    registry.register(manifest(name="orders", rows=5))
    registry.register(manifest(name="customers"))
    listed = registry.list()
    assert [(e.name, e.version) for e in listed] == [("customers", 1), ("orders", 2)]


def test_empty_registry_lists_nothing(registry: DatasetRegistry) -> None:
    assert registry.list() == ()


def test_versions_are_ordered_oldest_first(registry: DatasetRegistry) -> None:
    registry.register(manifest())
    registry.register(manifest(rows=1))
    registry.register(manifest(rows=2))
    assert [v.version for v in registry.versions("orders")] == [1, 2, 3]


def test_jsonfile_registry_persists_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    JsonFileDatasetRegistry(path, clock=ticking_clock()).register(manifest())
    reopened = JsonFileDatasetRegistry(path, clock=ticking_clock())
    assert reopened.get(DatasetRef(name="orders")).version == 1


def test_jsonfile_registry_round_trips_the_manifest(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    source = manifest(rows=42)
    JsonFileDatasetRegistry(path, clock=ticking_clock()).register(source)
    loaded = JsonFileDatasetRegistry(path).get(DatasetRef(name="orders")).manifest
    assert loaded == source
    assert loaded.content_hash == source.content_hash


def test_jsonfile_registry_leaves_no_temp_files(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    store = JsonFileDatasetRegistry(path, clock=ticking_clock())
    store.register(manifest())
    store.register(manifest(rows=1))
    assert sorted(p.name for p in tmp_path.iterdir()) == ["registry.json"]


def test_missing_registry_file_reads_as_empty(tmp_path: Path) -> None:
    assert JsonFileDatasetRegistry(tmp_path / "absent.json").list() == ()


def test_implementations_satisfy_the_protocol(tmp_path: Path) -> None:
    """Structural conformance is checked by mypy at these annotations."""
    in_memory: DatasetRegistry = InMemoryDatasetRegistry()
    on_disk: DatasetRegistry = JsonFileDatasetRegistry(tmp_path / "registry.json")
    assert in_memory.list() == ()
    assert on_disk.list() == ()
