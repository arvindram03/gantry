# SPDX-License-Identifier: Apache-2.0
"""In-memory Dataset registry.

The test fake for the registry Protocol, and the reference implementation of
the versioning rules the durable stores must reproduce.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from gantry.core.dataset import DatasetManifest, DatasetRef, DatasetVersion
from gantry.registry.errors import DatasetNotFoundError, DatasetVersionNotFoundError


def _utc_now() -> datetime:
    return datetime.now(UTC)


class InMemoryDatasetRegistry:
    """A registry held in process memory. Not durable, by design."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock: Callable[[], datetime] = clock or _utc_now
        self._versions: dict[str, list[DatasetVersion]] = {}

    async def register(self, manifest: DatasetManifest) -> DatasetVersion:
        history = self._versions.setdefault(manifest.name, [])
        if history and history[-1].content_hash == manifest.content_hash:
            return history[-1]

        version = DatasetVersion(
            name=manifest.name,
            version=len(history) + 1,
            manifest=manifest,
            content_hash=manifest.content_hash,
            registered_at=self._clock(),
        )
        history.append(version)
        return version

    async def get(self, ref: DatasetRef) -> DatasetVersion:
        history = self._versions.get(ref.name)
        if not history:
            raise DatasetNotFoundError(ref.name)
        if ref.version is None:
            return history[-1]
        if ref.version > len(history):
            raise DatasetVersionNotFoundError(ref.name, ref.version, len(history))
        return history[ref.version - 1]

    async def versions(self, name: str) -> Sequence[DatasetVersion]:
        history = self._versions.get(name)
        if not history:
            raise DatasetNotFoundError(name)
        return tuple(history)

    async def list(self) -> Sequence[DatasetVersion]:
        return tuple(history[-1] for _, history in sorted(self._versions.items()))
