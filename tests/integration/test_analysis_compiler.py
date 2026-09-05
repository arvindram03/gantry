"""Compiled Analysis SQL, executed against real data.

Requires: make dev-up && uv run alembic upgrade head, and the scenario tables.

Compiling to plausible SQL is not the same as compiling to SQL that runs. The
ambiguous-column bug this file caught looked correct on the page.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.analysis.compiler import compile_analysis
from gantry.core.dataset import DatasetManifest
from gantry.state.artifacts import PostgresArtifactStore
from gantry.state.database import create_engine, transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.conftest import ensure_checkout_scenario

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
SPEC = "spec/examples/analysis-checkout-regression.yaml"


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


async def scenario(source: AsyncEngine) -> tuple[object, dict[str, DatasetManifest]]:
    """Load the spec and the manifests, with the declared time semantics.

    Discovery cannot know which column carries time, so the semantics come from
    the Dataset specs and are merged onto what the catalog reports.
    """
    from gantry.spec import load_analysis_spec, load_dataset_spec

    adapter = PostgresSourceAdapter(source)
    discovered = {m.name: await adapter.profile(m) for m in await adapter.discover()}

    manifests: dict[str, DatasetManifest] = {}
    for path in (
        "spec/examples/dataset-request-logs.yaml",
        "spec/examples/dataset-deploy-events.yaml",
    ):
        declared = load_dataset_spec(path).to_manifest()
        catalog = discovered[declared.name]
        manifests[declared.name] = catalog.model_copy(
            update={
                "dataset_schema": catalog.dataset_schema.model_copy(
                    update={"time_field": declared.dataset_schema.time_field}
                )
            }
        )
    return load_analysis_spec(SPEC).to_analysis(), manifests


async def test_the_compiled_sql_actually_runs(source: AsyncEngine) -> None:
    """Plausible SQL and valid SQL are different things."""
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)  # type: ignore[arg-type]

    async with transaction(source) as connection:
        rows = (await connection.execute(text(artifact.body))).all()
    assert rows, "the analysis returned nothing"


async def test_the_temporal_join_attributes_each_row_to_one_deploy(
    source: AsyncEngine,
) -> None:
    """A plain time comparison would match every deploy in range and multiply
    the left side; the lateral join takes exactly one."""
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)  # type: ignore[arg-type]

    async with transaction(source) as connection:
        rows = (await connection.execute(text(artifact.body))).all()
        total = (
            await connection.execute(
                text(
                    "SELECT count(*) FROM public.request_logs "
                    "WHERE event_time >= TIMESTAMPTZ '2026-09-03T00:00:00Z' "
                    "  AND event_time <  TIMESTAMPTZ '2026-09-04T00:00:00Z'"
                )
            )
        ).scalar_one()

    assert sum(row.row_count for row in rows) == total, "rows were duplicated or lost"


async def test_the_analysis_finds_the_regression(source: AsyncEngine) -> None:
    """The scenario the compiler exists for: a deploy that changed behaviour."""
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)  # type: ignore[arg-type]

    async with transaction(source) as connection:
        rows = (await connection.execute(text(artifact.body))).all()

    by_commit = {row.commit_sha: row for row in rows}
    assert {"3a1f00", "8f3142"} <= set(by_commit)

    before, after = by_commit["3a1f00"], by_commit["8f3142"]
    assert float(after.p95_latency) > float(before.p95_latency) * 3
    assert after.timeout_count > before.timeout_count


async def test_an_artifact_round_trips_through_the_store(source: AsyncEngine) -> None:
    """A Result has to be able to show the exact SQL it came from."""
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests, generated_at=datetime.now(UTC))  # type: ignore[arg-type]

    meta = create_engine(META_URL)
    try:
        store = PostgresArtifactStore(meta)
        stored = await store.put(artifact)
        assert stored == artifact.content_hash

        loaded = await store.get(artifact.content_hash)
        assert loaded is not None
        assert loaded.body == artifact.body
        assert loaded.inputs == artifact.inputs
        assert loaded.content_hash == artifact.content_hash

        # Storing again is a no-op, because the hash is the identity.
        assert await store.put(artifact) == artifact.content_hash
    finally:
        await meta.dispose()


async def test_an_unstored_artifact_reads_as_absent() -> None:
    meta = create_engine(META_URL)
    try:
        assert await PostgresArtifactStore(meta).get("sha256:" + "0" * 64) is None
    finally:
        await meta.dispose()
