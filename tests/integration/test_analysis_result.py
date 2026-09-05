"""The guarantee boundary, end to end, against real data.

Two claims are under test, and they are the two the design document cares
about most:

  1. An Analysis the engine ran successfully is still rejected when the
     runtime's own verification says the numbers are not trustworthy - and the
     findings are withheld rather than published with a caveat.
  2. A finding that is published can be traced, in one call, back to the
     Movement checkpoint the data underneath it had reached.

Requires: make dev-up && uv run alembic upgrade head, and the scenario tables.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from gantry.adapters.engine.postgres import PostgresEngineAdapter
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.analysis.compiler import compile_analysis
from gantry.analysis.model import Analysis
from gantry.analysis.result import AnalysisResult, StrengthBasis
from gantry.analysis.service import AnalysisService, pins_for
from gantry.core.dataset import DatasetManifest
from gantry.core.evidence import VerificationStatus
from gantry.core.operation import OperationType
from gantry.core.positions import Checkpoint, CheckpointScope, PositionKind, SourcePosition
from gantry.core.provenance import Lineage, Provenance
from gantry.core.results import ResultStatus
from gantry.core.verification import CheckName
from gantry.movement.result import MovementResult
from gantry.results.provenance import resolve
from gantry.results.refresh import MissingArtifactError, refresh
from gantry.results.store import PostgresResultStore
from gantry.state.artifacts import PostgresArtifactStore
from gantry.state.database import create_engine, transaction
from gantry.state.operations import OperationStore
from gantry.state.registry import PostgresDatasetRegistry
from gantry.state.tables import results
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.conftest import clear_operation, ensure_checkout_scenario

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)

DATASET_SPECS = (
    "spec/examples/dataset-request-logs.yaml",
    "spec/examples/dataset-deploy-events.yaml",
)
ANALYSIS_SPEC = "spec/examples/analysis-checkout-regression.yaml"
UPSTREAM = "test-logs-movement.movement"


@pytest.fixture
async def source() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(SOURCE_URL)
    try:
        async with transaction(engine) as connection:
            present = (
                await connection.execute(
                    text("SELECT to_regclass('public.request_logs') IS NOT NULL")
                )
            ).scalar_one()
        if not present:
            await ensure_checkout_scenario(engine)
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def meta() -> AsyncIterator[AsyncEngine]:
    """The metadata store, with this test's operations registered and removed.

    Results carry a foreign key to the operation that produced them, so the
    operation rows have to exist. Creating them here rather than relying on
    whatever a previous run left behind is the difference between a test and a
    test that happens to pass on this machine.
    """
    engine = create_engine(META_URL)
    store = OperationStore(engine)
    try:
        for name, kind in (
            ("checkout-regression", OperationType.ANALYSIS),
            ("checkout-regression-expanding", OperationType.ANALYSIS),
            ("test-logs-movement", OperationType.MOVEMENT),
        ):
            await store.ensure(name, kind)
        yield engine
        for name in ("checkout-regression-expanding", "test-logs-movement"):
            await clear_operation(engine, name)
    finally:
        await engine.dispose()


async def manifests_for(source: AsyncEngine) -> dict[str, DatasetManifest]:
    """Catalog facts, with the time semantics the Dataset specs declare."""
    from gantry.spec import load_dataset_spec

    adapter = PostgresSourceAdapter(source)
    discovered = {m.name: await adapter.profile(m) for m in await adapter.discover()}

    manifests: dict[str, DatasetManifest] = {}
    for path in DATASET_SPECS:
        declared = load_dataset_spec(path).to_manifest()
        catalog = discovered[declared.name]
        manifests[declared.name] = catalog.model_copy(
            update={
                "dataset_schema": catalog.dataset_schema.model_copy(
                    update={"time_field": declared.dataset_schema.time_field}
                )
            }
        )
    return manifests


def analysis_spec() -> Analysis:
    from gantry.spec import load_analysis_spec

    return load_analysis_spec(ANALYSIS_SPEC).to_analysis()


def without_temporal_join(analysis: Analysis) -> Analysis:
    """The same Analysis with the temporal qualifier removed.

    Every request then matches every deploy for its service instead of the one
    that was live, so the join multiplies rows. The engine is perfectly happy
    with it; that is the point.
    """
    return analysis.model_copy(
        update={
            "name": "checkout-regression-expanding",
            "joins": tuple(join.model_copy(update={"temporal": None}) for join in analysis.joins),
        }
    )


async def upstream_movement(meta: AsyncEngine, source: AsyncEngine) -> MovementResult:
    """A Movement Result over the same Datasets, with checkpoints.

    Stands in for the Movement that landed this data. What matters downstream
    is not how it ran but that its checkpoints say how far the data had got.
    """
    registry = PostgresDatasetRegistry(meta)
    versions = [await registry.register(m) for m in (await manifests_for(source)).values()]

    now = datetime.now(UTC)
    result = MovementResult(
        name=UPSTREAM,
        status=ResultStatus.OK,
        created_at=now,
        started_at=now - timedelta(seconds=30),
        finished_at=now,
        rows_inserted=40_000,
        partitions_total=3,
        partitions_complete=3,
        partitions_verified=3,
        provenance=Provenance(
            generated_at=now,
            operation="test-logs-movement",
            plan_version=1,
            lineage=Lineage(inputs=pins_for(versions)),
            checkpoints=tuple(
                Checkpoint(
                    scope=CheckpointScope.PARTITION,
                    scope_id=f"partition/public.request_logs/{index:05d}",
                    position=SourcePosition(kind=PositionKind.LSN, value=str(32777693472 + index)),
                    committed_at=now,
                )
                for index in range(3)
            ),
        ),
    )
    await PostgresResultStore(meta).put(result)
    return result


async def run(analysis: Analysis, source: AsyncEngine, meta: AsyncEngine) -> AnalysisResult:
    manifests = await manifests_for(source)
    registry = PostgresDatasetRegistry(meta)
    versions = [await registry.register(m) for m in manifests.values()]

    adapter = PostgresEngineAdapter(source)
    service = AnalysisService(
        adapter=adapter, manifests=manifests, pins=pins_for(versions), checkpoints=()
    )
    completed = await service.run(analysis)
    assert completed.result is not None, completed.describe()
    return completed.result


async def test_a_well_formed_analysis_passes_verification_and_publishes_findings(
    source: AsyncEngine, meta: AsyncEngine
) -> None:
    result = await run(analysis_spec(), source, meta)

    assert result.status is ResultStatus.OK
    assert all(v.status is VerificationStatus.PASSED for v in result.verification)
    assert result.findings, "a passing analysis published nothing"
    assert all(f.strength_basis is StrengthBasis.STATISTICAL for f in result.measured_findings)


async def test_an_expanding_join_is_rejected_though_the_engine_succeeded(
    source: AsyncEngine, meta: AsyncEngine
) -> None:
    """The guarantee boundary: SQL that runs is not SQL that is trustworthy."""
    result = await run(without_temporal_join(analysis_spec()), source, meta)

    assert result.rows_returned > 0, "the engine did not actually run it"
    assert result.status is ResultStatus.VERIFICATION_FAILED

    expansion = next(v for v in result.verification if v.check is CheckName.ROW_EXPANSION)
    assert expansion.status is VerificationStatus.FAILED
    assert "2.00" in (expansion.difference or ""), expansion.difference


async def test_findings_are_withheld_rather_than_published_with_a_caveat(
    source: AsyncEngine, meta: AsyncEngine
) -> None:
    """A conclusion drawn from a rejected computation is not a weak finding.
    It is not a finding, and publishing it caveated invites it to be quoted."""
    result = await run(without_temporal_join(analysis_spec()), source, meta)
    assert result.findings == ()


async def test_a_result_round_trips_through_the_store_as_its_own_type(
    source: AsyncEngine, meta: AsyncEngine
) -> None:
    """Rehydrating an AnalysisResult as the base Result loses every field that
    makes it worth storing, and the base model rejects them outright."""
    result = await run(analysis_spec(), source, meta)
    store = PostgresResultStore(meta)
    await store.put(result)

    loaded = await store.get(result.name)
    assert isinstance(loaded, AnalysisResult)
    assert loaded.findings == result.findings
    assert loaded.artifact_hash == result.artifact_hash
    assert loaded.engine == result.engine
    assert loaded.rows_returned == result.rows_returned


async def test_a_finding_traces_back_to_the_movement_checkpoint(
    source: AsyncEngine, meta: AsyncEngine
) -> None:
    """The exit criterion, in one call: from a published conclusion to the
    position the data underneath it had reached."""
    movement = await upstream_movement(meta, source)
    result = await run(analysis_spec(), source, meta)

    store = PostgresResultStore(meta)
    await store.put(result)
    # The Result names its artifact by hash; provenance can only show the
    # computation if the artifact was retained.
    await PostgresArtifactStore(meta).put(
        compile_analysis(analysis_spec(), await manifests_for(source))
    )

    chain = await resolve(
        result.name,
        results=store,
        registry=PostgresDatasetRegistry(meta),
        artifacts=PostgresArtifactStore(meta),
    )

    assert chain is not None, "the Result vanished between writing and reading"
    assert chain.complete, chain.unresolved
    assert [a.content_hash for a in chain.artifacts] == [result.artifact_hash]
    assert {d.name for d in chain.datasets} == {"public.request_logs", "public.deploy_events"}
    assert UPSTREAM in chain.upstream_operations

    # Containment, not equality. Any Movement that produced one of these
    # Datasets contributes its checkpoints, and on a shared stack there is
    # usually more than one — a real demo Movement over the same tables is not
    # contamination, it is another honest answer to "where did this data get to".
    reached = {c.position.value for c in chain.checkpoints}
    assert reached >= {c.position.value for c in movement.provenance.checkpoints}


async def test_refresh_re_runs_the_same_computation_on_current_data(
    source: AsyncEngine, meta: AsyncEngine
) -> None:
    """Refresh must re-execute the artifact the Result came from, by hash. On
    unchanged data every measurement lands where the Result left it - which is
    also the check that the finding's references still address a real row."""
    result = await run(analysis_spec(), source, meta)
    store = PostgresResultStore(meta)
    await store.put(result)
    artifacts = PostgresArtifactStore(meta)
    await artifacts.put(compile_analysis(analysis_spec(), await manifests_for(source)))

    report = await refresh(
        result.name,
        results=store,
        artifacts=artifacts,
        adapter=PostgresEngineAdapter(source),
    )

    assert report is not None
    assert report.artifact.content_hash == result.artifact_hash
    assert report.drifts, "no measurement was re-measured"
    assert all(drift.resolved for drift in report.drifts), "a finding's group went missing"
    assert report.holds, [d.describe() for d in report.moved]


async def test_refusing_to_refresh_a_result_whose_artifact_was_not_kept(
    source: AsyncEngine, meta: AsyncEngine
) -> None:
    """Recompiling would produce a different computation unless proven
    otherwise, and comparing against it would answer a different question."""
    result = await run(analysis_spec(), source, meta)
    # Stored under its own name: overwriting the real Result with a dangling
    # artifact hash would leave the store broken for everything after it.
    tampered = result.model_copy(
        update={
            "name": "checkout-regression-unretained.analysis",
            "artifact_hash": "sha256:" + "0" * 64,
        }
    )
    store = PostgresResultStore(meta)
    await store.put(tampered)

    try:
        with pytest.raises(MissingArtifactError):
            await refresh(
                tampered.name,
                results=store,
                artifacts=PostgresArtifactStore(meta),
                adapter=PostgresEngineAdapter(source),
            )
    finally:
        async with transaction(meta) as connection:
            await connection.execute(delete(results).where(results.c.name == tampered.name))
