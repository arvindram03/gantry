"""Walking a Result back to the data it was computed from.

The chain is only worth having if it is honest about its gaps. A chain that
silently drops a link it could not resolve looks exactly like a chain that
resolved completely, which is the failure mode these tests exist to prevent.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from gantry.analysis.artifact import GeneratedArtifact
from gantry.analysis.result import AnalysisResult
from gantry.core import DatasetManifest, DatasetSchema, PhysicalRef
from gantry.core.positions import Checkpoint, CheckpointScope, PositionKind, SourcePosition
from gantry.core.provenance import ArtifactKind, ArtifactRef, DatasetPin, Lineage, Provenance
from gantry.core.results import Result, ResultKind, ResultStatus
from gantry.registry.memory import InMemoryDatasetRegistry
from gantry.results.provenance import resolve

NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


class MemoryArtifactStore:
    def __init__(self) -> None:
        self._by_hash: dict[str, GeneratedArtifact] = {}

    async def put(self, artifact: GeneratedArtifact) -> str:
        self._by_hash[artifact.content_hash] = artifact
        return artifact.content_hash

    async def get(self, content_hash: str) -> GeneratedArtifact | None:
        return self._by_hash.get(content_hash)

    async def for_analysis(self, analysis: str) -> Sequence[GeneratedArtifact]:
        return tuple(a for a in self._by_hash.values() if a.analysis == analysis)


class MemoryResultStore:
    def __init__(self) -> None:
        self._by_name: dict[str, Result] = {}

    async def put(self, result: Result) -> None:
        self._by_name[result.name] = result

    async def get(self, name: str) -> Result | None:
        return self._by_name.get(name)

    async def for_operation(self, operation: str) -> Sequence[Result]:
        return tuple(r for r in self._by_name.values() if r.provenance.operation == operation)

    async def for_operation_outputs(self, datasets: Sequence[str]) -> Sequence[Result]:
        wanted = set(datasets)
        return tuple(
            r
            for r in self._by_name.values()
            if {pin.name for pin in r.provenance.lineage.inputs} & wanted
        )


def manifest(name: str) -> DatasetManifest:
    return DatasetManifest(
        name=name,
        physical=PhysicalRef(adapter="postgres", reference=f"public.{name}"),
        dataset_schema=DatasetSchema(keys=("id",)),
    )


def artifact() -> GeneratedArtifact:
    return GeneratedArtifact(
        analysis="checkout-regression",
        engine="postgres",
        body="SELECT 1",
        inputs=("request_logs",),
        generated_at=NOW,
    )


def checkpoint(scope_id: str, lsn: str) -> Checkpoint:
    return Checkpoint(
        scope=CheckpointScope.PARTITION,
        scope_id=scope_id,
        position=SourcePosition(kind=PositionKind.LSN, value=lsn),
        committed_at=NOW,
    )


async def wired() -> tuple[
    MemoryResultStore, InMemoryDatasetRegistry, MemoryArtifactStore, DatasetPin
]:
    """An Analysis Result over a Dataset a Movement produced."""
    registry = InMemoryDatasetRegistry()
    version = await registry.register(manifest("request_logs"))
    pin = DatasetPin.from_version(version)

    artifacts = MemoryArtifactStore()
    compiled = artifact()
    await artifacts.put(compiled)

    results = MemoryResultStore()
    await results.put(
        Result(
            name="logs-movement.movement",
            kind=ResultKind.MOVEMENT,
            status=ResultStatus.OK,
            created_at=NOW,
            provenance=Provenance(
                generated_at=NOW,
                operation="logs-movement",
                lineage=Lineage(inputs=(pin,)),
                checkpoints=(checkpoint("partition/request_logs/00000", "32777693472"),),
            ),
        )
    )
    await results.put(
        AnalysisResult(
            name="checkout-regression.analysis",
            status=ResultStatus.OK,
            created_at=NOW,
            started_at=NOW,
            finished_at=NOW,
            artifact_hash=compiled.content_hash,
            engine="postgres",
            provenance=Provenance(
                generated_at=NOW,
                operation="checkout-regression",
                lineage=Lineage(inputs=(pin,)),
                artifacts=(compiled.to_ref(),),
            ),
        )
    )
    return results, registry, artifacts, pin


async def test_an_unknown_result_resolves_to_nothing() -> None:
    results, registry, artifacts, _ = await wired()
    assert (
        await resolve("nope.analysis", results=results, registry=registry, artifacts=artifacts)
        is None
    )


async def test_the_chain_reaches_the_movement_checkpoint_under_the_finding() -> None:
    """The whole point: from a conclusion to how far the data had got."""
    results, registry, artifacts, _ = await wired()
    chain = await resolve(
        "checkout-regression.analysis", results=results, registry=registry, artifacts=artifacts
    )

    assert chain is not None
    assert chain.complete
    assert [a.content_hash for a in chain.artifacts] == [artifact().content_hash]
    assert [d.name for d in chain.datasets] == ["request_logs"]
    assert chain.upstream_operations == ["logs-movement.movement"]
    assert [c.position.value for c in chain.checkpoints] == ["32777693472"]


async def test_a_result_does_not_list_itself_as_its_own_upstream() -> None:
    """Both Results pin the same Dataset; only the other one is upstream."""
    results, registry, artifacts, _ = await wired()
    chain = await resolve(
        "checkout-regression.analysis", results=results, registry=registry, artifacts=artifacts
    )
    assert chain is not None
    assert "checkout-regression.analysis" not in chain.upstream_operations


async def test_a_missing_artifact_store_is_named_not_skipped() -> None:
    results, registry, _, _ = await wired()
    chain = await resolve("checkout-regression.analysis", results=results, registry=registry)

    assert chain is not None
    assert not chain.complete
    assert chain.artifacts == []
    assert any("artifact" in gap for gap in chain.unresolved)


async def test_an_unstored_artifact_is_named_not_skipped() -> None:
    results, registry, _, _ = await wired()
    chain = await resolve(
        "checkout-regression.analysis",
        results=results,
        registry=registry,
        artifacts=MemoryArtifactStore(),
    )
    assert chain is not None
    assert any(artifact().content_hash in gap for gap in chain.unresolved)


async def test_an_unhashed_artifact_reference_cannot_be_resolved() -> None:
    """A reference without a content hash names a thing, not a version of it."""
    results, registry, artifacts, pin = await wired()
    await results.put(
        AnalysisResult(
            name="loose.analysis",
            status=ResultStatus.OK,
            created_at=NOW,
            started_at=NOW,
            finished_at=NOW,
            provenance=Provenance(
                generated_at=NOW,
                operation="loose",
                lineage=Lineage(inputs=(pin,)),
                artifacts=(ArtifactRef(kind=ArtifactKind.SQL, reference="checkout-regression"),),
            ),
        )
    )
    chain = await resolve("loose.analysis", results=results, registry=registry, artifacts=artifacts)
    assert chain is not None
    assert chain.unresolved == ["artifact checkout-regression"]


async def test_an_unregistered_dataset_pin_is_named_not_skipped() -> None:
    results, registry, artifacts, _ = await wired()
    await results.put(
        AnalysisResult(
            name="orphan.analysis",
            status=ResultStatus.OK,
            created_at=NOW,
            started_at=NOW,
            finished_at=NOW,
            provenance=Provenance(
                generated_at=NOW,
                operation="orphan",
                lineage=Lineage(
                    inputs=(
                        DatasetPin(name="ghosts", version=1, content_hash="sha256:" + "0" * 64),
                    )
                ),
            ),
        )
    )
    chain = await resolve(
        "orphan.analysis", results=results, registry=registry, artifacts=artifacts
    )
    assert chain is not None
    assert chain.datasets == []
    assert any("ghosts" in gap for gap in chain.unresolved)


async def test_a_pin_whose_content_changed_is_dangling_not_satisfied() -> None:
    """The version number still exists; the content behind it does not match.
    Returning it would silently answer with the wrong data."""
    registry = InMemoryDatasetRegistry()
    version = await registry.register(manifest("request_logs"))
    tampered = DatasetPin(
        name=version.name, version=version.version, content_hash="sha256:" + "1" * 64
    )

    results = MemoryResultStore()
    await results.put(
        AnalysisResult(
            name="drifted.analysis",
            status=ResultStatus.OK,
            created_at=NOW,
            started_at=NOW,
            finished_at=NOW,
            provenance=Provenance(
                generated_at=NOW, operation="drifted", lineage=Lineage(inputs=(tampered,))
            ),
        )
    )
    chain = await resolve("drifted.analysis", results=results, registry=registry)
    assert chain is not None
    assert chain.datasets == []
    assert any("request_logs" in gap for gap in chain.unresolved)


def test_describe_counts_the_gaps_it_could_not_close() -> None:
    from gantry.results.provenance import ProvenanceChain

    chain = ProvenanceChain(
        result=Result(
            name="x.analysis",
            kind=ResultKind.FINDING,
            status=ResultStatus.OK,
            created_at=NOW,
            provenance=Provenance(generated_at=NOW, operation="x"),
        ),
        unresolved=["dataset ghosts@1"],
    )
    assert "1 unresolved" in chain.describe()
