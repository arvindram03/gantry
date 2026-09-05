"""Discovery registers Datasets.

The point of these tests is the direction of the dependency: discovery produces
registered Dataset resources, not a snapshot private to the Movement planner.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime

import pytest
from gantry.core.dataset import DatasetManifest, DatasetRef, DatasetStatistics, PhysicalRef
from gantry.core.positions import PositionKind, SourcePosition
from gantry.core.schema import DatasetSchema, FieldSchema, ForeignKey, Index
from gantry.movement.partitioning import Partition
from gantry.registry import InMemoryDatasetRegistry
from gantry.registry.discovery import discover_into_registry


def orders_manifest(profiled: bool = False) -> DatasetManifest:
    manifest = DatasetManifest(
        name="public.orders",
        physical=PhysicalRef(
            adapter="postgres",
            reference="public.orders",
            estimated_rows=1_000_000,
            estimated_bytes=111_900_000,
        ),
        dataset_schema=DatasetSchema(
            keys=("order_id",),
            fields=(
                FieldSchema(name="order_id", type="bigint", nullable=False),
                FieldSchema(name="customer_id", type="bigint", nullable=False),
                FieldSchema(name="status", type="text", nullable=True),
            ),
            foreign_keys=(
                ForeignKey(
                    columns=("customer_id",),
                    references="public.customers",
                    referenced_columns=("customer_id",),
                ),
            ),
            indexes=(Index(name="orders_pkey", columns=("order_id",), unique=True, primary=True),),
        ),
    )
    if not profiled:
        return manifest
    return manifest.model_copy(
        update={
            "statistics": DatasetStatistics(
                row_count=1_000_000,
                profiled_at=datetime(2026, 9, 14, tzinfo=UTC),
                key_min="1",
                key_max="1000000",
                distinct_keys=1_000_000,
                null_rates={"status": 0.0},
            )
        }
    )


class FakeSourceAdapter:
    """A source that returns fixed manifests, recording what was asked of it."""

    def __init__(self) -> None:
        self.discover_calls = 0
        self.profile_calls = 0

    async def discover(self, *, schemas: Sequence[str] = ("public",)) -> Sequence[DatasetManifest]:
        self.discover_calls += 1
        return (orders_manifest(),)

    async def profile(self, manifest: DatasetManifest) -> DatasetManifest:
        self.profile_calls += 1
        return orders_manifest(profiled=True)

    async def current_position(self) -> SourcePosition:
        return SourcePosition(kind=PositionKind.LSN, value="0/16B3748")

    async def read_partition(
        self, manifest: DatasetManifest, partition: Partition, *, batch_size: int = 10_000
    ) -> AsyncIterator[Sequence[tuple[object, ...]]]:
        """Discovery never reads rows; this exists to satisfy the contract."""
        yield ()


async def test_discovery_registers_datasets() -> None:
    registry = InMemoryDatasetRegistry()
    report = await discover_into_registry(FakeSourceAdapter(), registry, profile=False)

    assert report.names == ("public.orders",)
    assert (await registry.get(DatasetRef(name="public.orders"))).version == 1


async def test_profiling_enriches_the_registered_manifest() -> None:
    """One registration per run, carrying catalog and statistics together."""
    registry = InMemoryDatasetRegistry()
    report = await discover_into_registry(FakeSourceAdapter(), registry)

    assert report.registered[0].version == 1
    assert len(await registry.versions("public.orders")) == 1
    latest = await registry.get(DatasetRef(name="public.orders"))
    assert latest.manifest.statistics.distinct_keys == 1_000_000


async def test_rediscovering_an_unchanged_source_registers_nothing_new() -> None:
    """Content addressing makes rediscovery free, however often it runs.

    Registering a catalog-only manifest and then a profiled one would alternate
    between two manifests for the same table and mint a version on every run.
    """
    registry = InMemoryDatasetRegistry()
    adapter = FakeSourceAdapter()
    for _ in range(5):
        await discover_into_registry(adapter, registry)

    assert len(await registry.versions("public.orders")) == 1
    assert adapter.discover_calls == 5


async def test_profiling_time_does_not_mint_a_version() -> None:
    """When we looked is not what we saw."""
    registry = InMemoryDatasetRegistry()
    early = orders_manifest(profiled=True)
    late = early.model_copy(
        update={
            "statistics": early.statistics.model_copy(
                update={"profiled_at": datetime(2027, 1, 1, tzinfo=UTC)}
            )
        }
    )
    assert early.content_hash == late.content_hash

    await registry.register(early)
    assert (await registry.register(late)).version == 1


async def test_adding_statistics_does_mint_a_version() -> None:
    """Statistics are content, even though the time of observing them is not."""
    registry = InMemoryDatasetRegistry()
    await registry.register(orders_manifest())
    assert (await registry.register(orders_manifest(profiled=True))).version == 2


async def test_skipping_profiling_leaves_statistics_empty() -> None:
    registry = InMemoryDatasetRegistry()
    adapter = FakeSourceAdapter()
    report = await discover_into_registry(adapter, registry, profile=False)

    assert adapter.profile_calls == 0
    assert not report.profiled
    assert report.registered[0].manifest.statistics.row_count is None


async def test_discovered_manifest_carries_relations() -> None:
    """Foreign keys drive both dependency ordering and integrity verification."""
    registry = InMemoryDatasetRegistry()
    await discover_into_registry(FakeSourceAdapter(), registry, profile=False)

    schema = (await registry.get(DatasetRef(name="public.orders"))).manifest.dataset_schema
    assert schema.foreign_keys[0].references == "public.customers"
    assert schema.keys == ("order_id",)
    assert any(index.primary for index in schema.indexes)


@pytest.mark.parametrize("profile", [True, False])
async def test_discovery_is_idempotent_under_repetition(profile: bool) -> None:
    registry = InMemoryDatasetRegistry()
    adapter = FakeSourceAdapter()
    first = await discover_into_registry(adapter, registry, profile=profile)
    second = await discover_into_registry(adapter, registry, profile=profile)

    assert first.registered[0].content_hash == second.registered[0].content_hash
    assert first.registered[0].version == second.registered[0].version
