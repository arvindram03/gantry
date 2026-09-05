"""Resolving why a Result should be believed.

The chain the design document asks for, walked in one call:

    finding -> Result -> generated artifact -> Dataset versions
            -> the Movement that produced those Datasets -> its checkpoints

Each link answers a different question. The artifact says what computation ran.
The Dataset versions say what it read - the exact versions, not whatever is
current. The Movement checkpoints say how far the data had got when it was
read, which is the link that turns "these numbers" into "these numbers, from
data that was complete up to here".
"""

from __future__ import annotations

from dataclasses import dataclass, field

from gantry.analysis.artifact import GeneratedArtifact
from gantry.core.dataset import DatasetRef, DatasetVersion
from gantry.core.positions import Checkpoint
from gantry.core.provenance import DatasetPin
from gantry.core.results import Result
from gantry.registry.base import DatasetRegistry
from gantry.registry.errors import RegistryError
from gantry.results.store import ResultStore
from gantry.state.artifacts import ArtifactStore


@dataclass
class ProvenanceChain:
    """Everything known about where a Result came from."""

    result: Result
    artifacts: list[GeneratedArtifact] = field(default_factory=list)
    datasets: list[DatasetVersion] = field(default_factory=list)
    checkpoints: list[Checkpoint] = field(default_factory=list)
    upstream_operations: list[str] = field(default_factory=list)
    # Links the chain could not resolve, named rather than omitted: a gap that
    # is invisible looks like a chain that is complete.
    unresolved: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return not self.unresolved

    def describe(self) -> str:
        return (
            f"{len(self.artifacts)} artifacts, {len(self.datasets)} dataset versions, "
            f"{len(self.checkpoints)} checkpoints"
            + (f", {len(self.unresolved)} unresolved" if self.unresolved else "")
        )


async def resolve(
    name: str,
    *,
    results: ResultStore,
    registry: DatasetRegistry,
    artifacts: ArtifactStore | None = None,
) -> ProvenanceChain | None:
    """Walk a Result's provenance back to its sources."""
    result = await results.get(name)
    if result is None:
        return None

    chain = ProvenanceChain(result=result)
    chain.checkpoints.extend(result.provenance.checkpoints)

    for reference in result.provenance.artifacts:
        if reference.content_hash is None or artifacts is None:
            chain.unresolved.append(f"artifact {reference.reference}")
            continue
        stored = await artifacts.get(reference.content_hash)
        if stored is None:
            chain.unresolved.append(f"artifact {reference.content_hash}")
            continue
        chain.artifacts.append(stored)

    for pin in result.provenance.lineage.inputs:
        version = await _resolve_pin(registry, pin)
        if version is None:
            chain.unresolved.append(f"dataset {pin}")
            continue
        chain.datasets.append(version)

    # Any Movement that produced one of these Datasets contributed the state
    # they were in when this Analysis read them.
    for other in await results.for_operation_outputs(
        tuple(version.name for version in chain.datasets)
    ):
        if other.name == result.name:
            continue
        chain.upstream_operations.append(other.name)
        chain.checkpoints.extend(other.provenance.checkpoints)

    return chain


async def _resolve_pin(registry: DatasetRegistry, pin: DatasetPin) -> DatasetVersion | None:
    try:
        version = await registry.get(DatasetRef(name=pin.name, version=pin.version))
    except RegistryError:
        return None
    # A pin names an exact version by content. If the stored version's content
    # differs, the pin is dangling and saying so beats returning the wrong one.
    return version if version.content_hash == pin.content_hash else None
