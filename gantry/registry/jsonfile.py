"""JSON-file Dataset registry.

Backs the CLI so registrations survive between invocations before the metadata
store exists. Writes are atomic (temp file + rename) so an interrupted write
cannot truncate the registry - the same rule the durable store will follow, at
a much smaller scale.

Single-process only: there is no locking. Concurrency arrives with Postgres.
"""

from __future__ import annotations

import builtins
import json
import os
import tempfile
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

from gantry.core.dataset import DatasetManifest, DatasetRef, DatasetVersion
from gantry.registry.errors import DatasetNotFoundError, DatasetVersionNotFoundError

DEFAULT_REGISTRY_PATH = Path(".gantry") / "registry.json"

_FORMAT_VERSION = 1


def _utc_now() -> datetime:
    return datetime.now(UTC)


class JsonFileDatasetRegistry:
    """A registry persisted to a single JSON document."""

    def __init__(self, path: Path, clock: Callable[[], datetime] | None = None) -> None:
        self._path = path
        self._clock: Callable[[], datetime] = clock or _utc_now

    @property
    def path(self) -> Path:
        return self._path

    def register(self, manifest: DatasetManifest) -> DatasetVersion:
        store = self._load()
        history = store.setdefault(manifest.name, [])
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
        self._save(store)
        return version

    def get(self, ref: DatasetRef) -> DatasetVersion:
        history = self._load().get(ref.name)
        if not history:
            raise DatasetNotFoundError(ref.name)
        if ref.version is None:
            return history[-1]
        if ref.version > len(history):
            raise DatasetVersionNotFoundError(ref.name, ref.version, len(history))
        return history[ref.version - 1]

    def versions(self, name: str) -> Sequence[DatasetVersion]:
        history = self._load().get(name)
        if not history:
            raise DatasetNotFoundError(name)
        return tuple(history)

    def list(self) -> Sequence[DatasetVersion]:
        return tuple(history[-1] for _, history in sorted(self._load().items()))

    def _load(self) -> dict[str, builtins.list[DatasetVersion]]:
        if not self._path.is_file():
            return {}
        raw: dict[str, object] = json.loads(self._path.read_text(encoding="utf-8"))
        datasets = raw.get("datasets", {})
        if not isinstance(datasets, dict):
            raise ValueError(f"{self._path}: 'datasets' must be a mapping")

        store: dict[str, builtins.list[DatasetVersion]] = {}
        for name, entries in datasets.items():
            if not isinstance(entries, list):
                raise ValueError(f"{self._path}: versions of {name!r} must be a list")
            store[str(name)] = [DatasetVersion.model_validate(entry) for entry in entries]
        return store

    def _save(self, store: dict[str, builtins.list[DatasetVersion]]) -> None:
        payload = {
            "formatVersion": _FORMAT_VERSION,
            "datasets": {
                name: [version.model_dump(mode="json") for version in history]
                for name, history in sorted(store.items())
            },
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Write beside the target so the rename stays on one filesystem.
        handle, temp_name = tempfile.mkstemp(
            dir=self._path.parent, prefix=f".{self._path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, self._path)
        except BaseException:
            Path(temp_name).unlink(missing_ok=True)
            raise
