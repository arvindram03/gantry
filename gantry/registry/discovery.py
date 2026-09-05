# SPDX-License-Identifier: Apache-2.0
"""Discovery as a registry operation.

Discovering a source registers Dataset resources. This is where making Dataset
first-class pays off: the output is addressable by name, versioned, and equally
available to a Movement that will move it and an Analysis that will read it -
rather than a snapshot only the Movement planner understands.

Profiling then registers a second version of the same Datasets. Because
manifests are content-addressed, re-running discovery against an unchanged
source registers nothing new.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from gantry.adapters.source.base import SourceAdapter
from gantry.core.dataset import DatasetManifest, DatasetRef, DatasetVersion
from gantry.registry.base import DatasetRegistry
from gantry.registry.errors import RegistryError


@dataclass(frozen=True)
class DiscoveryReport:
    """What one discovery run registered."""

    registered: tuple[DatasetVersion, ...]
    profiled: bool = False

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(version.name for version in self.registered)


async def discover_into_registry(
    adapter: SourceAdapter,
    registry: DatasetRegistry,
    *,
    schemas: Sequence[str] = ("public",),
    profile: bool = True,
) -> DiscoveryReport:
    """Discover a source and register what it holds.

    Profiling enriches each manifest before it is registered, rather than
    registering a catalog-only version and then a profiled one. Registering
    both would alternate between two different manifests for the same table, so
    every re-run would mint another version - and content addressing exists
    precisely so that re-running an unchanged discovery is free.
    """
    registered: list[DatasetVersion] = []
    for manifest in await adapter.discover(schemas=schemas):
        complete = await adapter.profile(manifest) if profile else manifest
        registered.append(await registry.register(await _carry_forward(registry, complete)))

    return DiscoveryReport(registered=tuple(registered), profiled=profile)


async def _carry_forward(registry: DatasetRegistry, discovered: DatasetManifest) -> DatasetManifest:
    """Preserve declarations a source cannot supply.

    Discovery reads the catalog, which knows the columns and their types and
    nothing about what they mean. Which column carries time, which fields are
    sensitive, what an agent may see - those are declarations someone made, and
    rediscovering a table must not erase them. Without this, registering a
    Dataset spec and then rediscovering would alternate between two manifests
    for one table, which is the version churn content addressing exists to
    prevent.
    """
    try:
        existing = (await registry.get(DatasetRef(name=discovered.name))).manifest
    except RegistryError:
        return discovered

    schema = discovered.dataset_schema
    if existing.dataset_schema.time_field is not None and schema.time_field is None:
        schema = schema.model_copy(update={"time_field": existing.dataset_schema.time_field})

    return discovered.model_copy(
        update={
            "dataset_schema": schema,
            "access": existing.access,
            "sensitive_fields": existing.sensitive_fields or discovered.sensitive_fields,
        }
    )
