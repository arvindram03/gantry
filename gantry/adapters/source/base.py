"""The source adapter interface.

Discovery returns Dataset manifests rather than an adapter-private snapshot.
That is the point of making Dataset a first-class resource: what a source knows
about its data becomes a registered, versioned resource that Movement and
Analysis both address by name, instead of a structure only the Movement planner
can read.

Profiling enriches a manifest rather than returning a separate report, so
discovery produces version 1 and profiling produces version 2 of the same
Dataset - and re-profiling an unchanged table registers nothing new.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from gantry.core.dataset import DatasetManifest
from gantry.core.positions import SourcePosition


class SourceAdapter(Protocol):
    """Read-side adapter for one source system."""

    async def discover(self, *, schemas: Sequence[str] = ("public",)) -> Sequence[DatasetManifest]:
        """Describe the datasets a source holds.

        Metadata only: no table is read. Row and byte figures are the planner's
        own estimates, which is why they are called estimates.
        """
        ...

    async def profile(self, manifest: DatasetManifest) -> DatasetManifest:
        """Enrich a manifest with sampled statistics.

        Must not scan the table. Partition planning needs the key range,
        distinct-key count and null rates, all of which the database already
        estimates from its own sample.
        """
        ...

    async def current_position(self) -> SourcePosition:
        """The source's current change-stream position.

        Captured before a snapshot so CDC can resume from exactly the point the
        snapshot was consistent at, leaving no gap.
        """
        ...
