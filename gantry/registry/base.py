"""The Dataset registry interface.

Registration is content-addressed and idempotent: re-registering an unchanged
manifest returns the existing version rather than creating a new one. That
matters for replay - a re-run that re-registers its inputs must not churn
versions and invalidate the pins in existing provenance records.

Day 4 adds a Postgres-backed implementation behind this same Protocol.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from gantry.core.dataset import DatasetManifest, DatasetRef, DatasetVersion


class DatasetRegistry(Protocol):
    """Register, version and resolve Datasets."""

    def register(self, manifest: DatasetManifest) -> DatasetVersion:
        """Register a manifest, returning the resulting version.

        Creates version 1 for an unknown name, returns the existing latest
        version when the content is unchanged, and otherwise creates the next
        version.
        """
        ...

    def get(self, ref: DatasetRef) -> DatasetVersion:
        """Resolve a reference. An unpinned ref resolves to the latest version.

        Raises `DatasetNotFoundError` or `DatasetVersionNotFoundError`.
        """
        ...

    def versions(self, name: str) -> Sequence[DatasetVersion]:
        """All versions of one Dataset, oldest first."""
        ...

    def list(self) -> Sequence[DatasetVersion]:
        """Latest version of every registered Dataset, by name."""
        ...
