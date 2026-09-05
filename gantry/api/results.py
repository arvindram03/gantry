"""`gantry.results.{get,explain,provenance,refresh}`.

The same four verbs the CLI exposes, returning values rather than printing
them. Every one of them is a question about a Result that has already been
written; nothing here computes a finding, which is why none of it needs the
access gate - a Result is Gantry's own output, not Dataset content.
"""

from __future__ import annotations

from collections.abc import Sequence

from gantry.adapters.engine.base import EngineAdapter
from gantry.analysis.artifact import GeneratedArtifact
from gantry.core.results import Result
from gantry.registry.base import DatasetRegistry
from gantry.results.provenance import ProvenanceChain, resolve
from gantry.results.refresh import RefreshReport, refresh
from gantry.results.store import ResultStore
from gantry.state.artifacts import ArtifactStore


class ResultsApi:
    """Reading Results and their provenance."""

    def __init__(
        self,
        *,
        results: ResultStore,
        registry: DatasetRegistry,
        artifacts: ArtifactStore,
        adapter: EngineAdapter | None = None,
    ) -> None:
        self._results = results
        self._registry = registry
        self._artifacts = artifacts
        self._adapter = adapter

    async def get(self, name: str) -> Result | None:
        return await self._results.get(name)

    async def explain(self, name: str) -> Sequence[GeneratedArtifact]:
        """The computations a Result came from, by content hash."""
        result = await self._results.get(name)
        if result is None:
            return ()
        found: list[GeneratedArtifact] = []
        for reference in result.provenance.artifacts:
            if reference.content_hash is None:
                continue
            artifact = await self._artifacts.get(reference.content_hash)
            if artifact is not None:
                found.append(artifact)
        return tuple(found)

    async def provenance(self, name: str) -> ProvenanceChain | None:
        return await resolve(
            name, results=self._results, registry=self._registry, artifacts=self._artifacts
        )

    async def refresh(self, name: str) -> RefreshReport | None:
        if self._adapter is None:
            raise RuntimeError("refresh needs an engine adapter to re-run the computation")
        return await refresh(
            name, results=self._results, artifacts=self._artifacts, adapter=self._adapter
        )
