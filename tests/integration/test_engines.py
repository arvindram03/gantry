"""Validation and execution, on two real engines.

Requires: make dev-up, the scenario tables, and network for DuckDB's postgres
extension on first use.

Two engines is the minimum that keeps the abstraction honest. With one, "engine
adapter" means whatever PostgreSQL happens to do - and the differences this
file records were invisible until a second engine ran the same SQL.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from gantry.adapters.engine.base import EngineAdapter
from gantry.adapters.engine.duckdb import DuckDBEngineAdapter
from gantry.adapters.engine.postgres import PostgresEngineAdapter
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.analysis.compiler import compile_analysis
from gantry.analysis.model import Analysis
from gantry.analysis.validate import (
    Repair,
    ValidationCheck,
    ValidationPolicy,
    validate,
)
from gantry.core.dataset import DatasetManifest
from gantry.spec import load_analysis_spec, load_dataset_spec
from gantry.state.database import create_engine, transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.conftest import ensure_checkout_scenario

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
DUCKDB_DSN = os.environ.get(
    "GANTRY_DUCKDB_PG_DSN",
    "host=localhost port=15432 dbname=gantry user=gantry password=gantry",
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


async def scenario(source: AsyncEngine) -> tuple[Analysis, dict[str, DatasetManifest]]:
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


@pytest.fixture
async def engines(source: AsyncEngine) -> AsyncIterator[tuple[EngineAdapter, EngineAdapter]]:
    postgres = PostgresEngineAdapter(source)
    try:
        duck = DuckDBEngineAdapter(attach_postgres=DUCKDB_DSN)
    except Exception as error:
        pytest.skip(f"duckdb postgres extension unavailable: {error}")
    try:
        yield postgres, duck
    finally:
        await duck.close()


def number(value: object) -> float:
    return float(value) if isinstance(value, int | float | Decimal) else float("nan")


# --- the exit criterion ----------------------------------------------------


async def test_both_engines_produce_the_same_answer(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """Same SQL, same data, two engines.

    Counts must match exactly. Interpolating aggregates are compared to a
    tolerance, for a reason recorded below rather than hidden in the
    comparison.
    """
    postgres, duck = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    left = {row["commit_sha"]: row for row in (await postgres.execute(artifact)).as_dicts()}
    right = {row["commit_sha"]: row for row in (await duck.execute(artifact)).as_dicts()}

    assert set(left) == set(right), "the engines grouped differently"
    for commit in left:
        assert left[commit]["row_count"] == right[commit]["row_count"]
        assert left[commit]["timeout_count"] == right[commit]["timeout_count"]
        for signal in ("error_rate", "database_calls_per_request", "database_wait_time"):
            assert number(left[commit][signal]) == pytest.approx(
                number(right[commit][signal]), rel=1e-9
            )


async def test_percentiles_agree_only_to_the_inputs_precision(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """A real cross-engine difference, recorded rather than papered over.

    `percentile_cont` interpolates. DuckDB keeps the input's DECIMAL scale
    through the interpolation; PostgreSQL promotes to double. Over a
    numeric(10,2) column the two therefore differ in the hundredths - the same
    computation, different intermediate types.
    """
    postgres, duck = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    left = {row["commit_sha"]: row for row in (await postgres.execute(artifact)).as_dicts()}
    right = {row["commit_sha"]: row for row in (await duck.execute(artifact)).as_dicts()}

    # Absolute, not relative. The guarantee is that they agree to the scale of
    # the input column - a numeric(10,2), so one hundredth - and that does not
    # get looser as the values get larger. A relative bound calibrated against
    # one dataset's magnitude silently changes meaning on the next.
    for commit in left:
        assert number(left[commit]["p95_latency"]) == pytest.approx(
            number(right[commit]["p95_latency"]), abs=0.01
        )


async def test_engines_disagree_about_python_types(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """The other difference worth knowing about.

    The same aggregate comes back as Decimal from one engine and float from the
    other, in both directions. Anything comparing results across engines has to
    normalise, and a Result carrying raw driver types would compare unequal on
    values that agree.
    """
    postgres, duck = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    left = (await postgres.execute(artifact)).as_dicts()[0]
    right = (await duck.execute(artifact)).as_dicts()[0]

    assert type(left["database_wait_time"]) is not type(right["database_wait_time"])
    assert number(left["database_wait_time"]) == pytest.approx(
        number(right["database_wait_time"]), rel=1e-9
    )


# --- validation ------------------------------------------------------------


async def test_a_valid_artifact_is_accepted_on_both_engines(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    for adapter in engines:
        report = await validate(analysis, artifact, adapter, manifests)
        assert report.accepted, report.describe()
        assert report.sample_rows is not None


async def test_a_missing_column_fails_validation_with_a_repairable_error(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """The exit criterion: refused before execution, with something to act on.

    An agent reading this needs to know what to change, not to be handed a
    stack trace.
    """
    postgres, _ = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)
    broken = artifact.model_copy(
        update={"body": artifact.body.replace('"latency_ms"', '"latency_millis"')}
    )

    report = await validate(analysis, broken, postgres, manifests)

    assert not report.accepted
    failure = report.failures[0]
    assert failure.check is ValidationCheck.SYNTAX
    assert failure.repair is Repair.EDIT_SPEC
    assert failure.detail is not None
    assert "latency_millis" in failure.detail
    # The engine's own words, not a stack trace.
    assert "Traceback" not in failure.detail


async def test_validation_refuses_before_it_runs_anything(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """A plan that will not plan is never sampled."""
    postgres, _ = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)
    broken = artifact.model_copy(update={"body": "SELECT * FROM nonexistent_table"})

    report = await validate(analysis, broken, postgres, manifests)
    assert not report.accepted
    assert report.sample_rows is None, "a plan that failed should not have been sampled"


async def test_unregistered_inputs_fail_before_the_engine_is_asked(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    postgres, _ = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    report = await validate(analysis, artifact, postgres, {})
    assert not report.accepted
    assert report.failures[0].check is ValidationCheck.INPUTS_EXIST
    assert report.failures[0].repair is Repair.REDISCOVER
    assert report.explain is None, "the engine should not have been asked"


async def test_a_cost_limit_refuses_an_expensive_artifact(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """Estimates, because the point is to refuse before running."""
    postgres, _ = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    report = await validate(
        analysis,
        artifact,
        postgres,
        manifests,
        policy=ValidationPolicy(max_estimated_cost=1.0),
    )
    assert not report.accepted
    assert report.failures[0].check is ValidationCheck.COST
    assert report.failures[0].repair is Repair.RAISE_LIMIT


async def test_a_generous_limit_accepts(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    postgres, _ = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    report = await validate(
        analysis,
        artifact,
        postgres,
        manifests,
        policy=ValidationPolicy(max_estimated_cost=1e12, max_estimated_rows=10**9),
    )
    assert report.accepted, report.describe()


async def test_explain_reports_an_estimate_where_the_engine_gives_one(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """PostgreSQL puts an estimate on the first plan line; DuckDB does not.

    Reporting none is better than inventing one, so the DuckDB adapter says so
    rather than parsing a number out of a rendered tree.
    """
    postgres, duck = engines
    analysis, manifests = await scenario(source)
    artifact = compile_analysis(analysis, manifests)

    pg_plan = await postgres.explain(artifact)
    assert pg_plan.estimated_cost is not None
    assert pg_plan.plan

    duck_plan = await duck.explain(artifact)
    assert duck_plan.plan
    assert duck_plan.estimated_cost is None


async def test_a_sample_is_bounded(
    source: AsyncEngine, engines: tuple[EngineAdapter, EngineAdapter]
) -> None:
    """Running a little of it must not become running it."""
    for adapter in engines:
        analysis, manifests = await scenario(source)
        artifact = compile_analysis(analysis, manifests)
        sample = await adapter.sample(artifact, limit=1)
        assert sample.row_count <= 1


def test_bounding_wraps_rather_than_appends() -> None:
    """An artifact may already end in a clause where LIMIT changes the meaning."""
    from gantry.adapters.engine.base import bounded

    wrapped = bounded("SELECT 1 LIMIT 5", 2)
    assert wrapped.startswith("SELECT * FROM (")
    assert wrapped.endswith("LIMIT 2")


def test_a_query_result_zips_columns_to_values() -> None:
    from gantry.adapters.engine.base import QueryResult

    result = QueryResult(columns=("a", "b"), rows=((1, 2), (3, 4)))
    assert result.as_dicts() == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    assert result.row_count == 2
