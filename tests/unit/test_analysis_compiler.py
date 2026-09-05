"""Compiling an Analysis into engine SQL.

Compilation is a pure function of the spec and the manifests, so this needs no
database. The tests that matter are the ones about what the compiler refuses:
the scope fence is as much of the design as the SQL it emits.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from gantry.analysis.artifact import (
    CompilationError,
    GeneratedArtifact,
    UnknownSignalError,
)
from gantry.analysis.compiler import compile_analysis, resolve_engine
from gantry.analysis.model import (
    Analysis,
    ExecutionEngine,
    FieldNormalization,
    Join,
    TemporalJoin,
    TemporalJoinStrategy,
)
from gantry.analysis.signals import known_signals
from gantry.core.dataset import DatasetManifest, PhysicalRef
from gantry.core.schema import DatasetSchema, FieldSchema
from gantry.core.timewindow import TimeWindow

AT = datetime(2026, 9, 28, tzinfo=UTC)
WINDOW = TimeWindow(start=datetime(2026, 9, 3, tzinfo=UTC), end=datetime(2026, 9, 4, tzinfo=UTC))


def logs(adapter: str = "postgres") -> DatasetManifest:
    return DatasetManifest(
        name="public.request_logs",
        physical=PhysicalRef(adapter=adapter, reference="public.request_logs"),
        dataset_schema=DatasetSchema(
            keys=("request_id",),
            time_field="event_time",
            fields=(
                FieldSchema(name="request_id", type="bigint"),
                FieldSchema(name="svc", type="text"),
                FieldSchema(name="latency_ms", type="numeric(10,2)"),
                FieldSchema(name="status", type="integer"),
                FieldSchema(name="event_time", type="timestamp with time zone"),
            ),
        ),
    )


def deploys(adapter: str = "postgres") -> DatasetManifest:
    return DatasetManifest(
        name="public.deploy_events",
        physical=PhysicalRef(adapter=adapter, reference="public.deploy_events"),
        dataset_schema=DatasetSchema(
            keys=("deploy_id",),
            time_field="deployed_at",
            fields=(
                FieldSchema(name="deploy_id", type="bigint"),
                FieldSchema(name="service_name", type="text"),
                FieldSchema(name="commit_sha", type="text"),
                FieldSchema(name="deployed_at", type="timestamp with time zone"),
            ),
        ),
    )


def manifests(adapter: str = "postgres") -> dict[str, DatasetManifest]:
    return {"public.request_logs": logs(adapter), "public.deploy_events": deploys(adapter)}


def analysis(**overrides: object) -> Analysis:
    base: dict[str, object] = {
        "name": "checkout-regression",
        "inputs": ("public.request_logs", "public.deploy_events"),
        "window": WINDOW,
        "normalize": (FieldNormalization(canonical="service", aliases=("svc", "service_name")),),
        "joins": (
            Join(
                left="public.request_logs",
                right="public.deploy_events",
                on=("service",),
                temporal=TemporalJoin(
                    strategy=TemporalJoinStrategy.NEAREST_PRECEDING,
                    max_distance_seconds=86400,
                ),
            ),
        ),
        "signals": ("p95_latency", "error_rate"),
    }
    base.update(overrides)
    return Analysis.model_validate(base)


def compile_it(**overrides: object) -> GeneratedArtifact:
    return compile_analysis(analysis(**overrides), manifests(), generated_at=AT)


# --- determinism -----------------------------------------------------------


def test_compilation_is_deterministic() -> None:
    """An artifact hash is only a usable identity if it is reproducible."""
    assert compile_it().content_hash == compile_it().content_hash


def test_compilation_time_is_outside_the_hash() -> None:
    """Recompiling the same Analysis must yield the same artifact, whenever."""
    early = compile_analysis(analysis(), manifests(), generated_at=AT)
    late = compile_analysis(analysis(), manifests(), generated_at=datetime.now(UTC))
    assert early.content_hash == late.content_hash
    assert early.generated_at != late.generated_at


def test_a_different_engine_is_a_different_artifact() -> None:
    postgres = compile_analysis(analysis(), manifests(), engine=ExecutionEngine.POSTGRES)
    duckdb = compile_analysis(analysis(), manifests(), engine=ExecutionEngine.DUCKDB)
    assert postgres.content_hash != duckdb.content_hash


def test_a_changed_spec_is_a_different_artifact() -> None:
    assert compile_it().content_hash != compile_it(signals=("p95_latency",)).content_hash


# --- what it emits ---------------------------------------------------------


def test_inputs_are_normalised_before_anything_joins() -> None:
    """So a join names one column, not a disjunction of source spellings."""
    body = compile_it().body
    assert '"svc" AS "service"' in body
    assert '"service_name" AS "service"' in body


def test_the_window_is_pushed_into_each_input() -> None:
    """Applied per input so the engine can use an index."""
    body = compile_it().body
    assert body.count("2026-09-03T00:00:00+00:00") == 2


def test_a_temporal_join_takes_one_row() -> None:
    """A plain join on a time comparison matches every candidate in range and
    multiplies the left side - the row expansion verification exists to catch."""
    body = compile_it().body
    assert "LEFT JOIN LATERAL" in body
    assert "LIMIT 1" in body
    assert "ORDER BY" in body


def test_nearest_preceding_looks_backwards() -> None:
    body = compile_it().body
    assert '"deployed_at" <= ' in body
    assert "- INTERVAL '86400 seconds'" in body


def test_nearest_following_looks_forwards() -> None:
    forwards = Join(
        left="public.request_logs",
        right="public.deploy_events",
        on=("service",),
        temporal=TemporalJoin(strategy=TemporalJoinStrategy.NEAREST_FOLLOWING),
    )
    body = compile_it(joins=(forwards,)).body
    assert '"deployed_at" >= ' in body


def test_ambiguous_columns_are_qualified() -> None:
    """Normalising two fields onto one name puts it on both sides."""
    body = compile_it().body
    assert 'GROUP BY "public_request_logs"."service"' in body


def test_the_artifact_records_what_it_reads() -> None:
    """So provenance need not be recovered by parsing the SQL back out."""
    artifact = compile_it()
    assert artifact.inputs == ("public.request_logs", "public.deploy_events")
    assert artifact.parameters["window_start"] == "2026-09-03T00:00:00+00:00"


def test_the_artifact_says_it_is_generated() -> None:
    assert compile_it().body.startswith("-- Generated by Gantry")


# --- the scope fence -------------------------------------------------------


def test_an_unknown_signal_is_refused() -> None:
    """A spec that accepted arbitrary expressions would be a query language."""
    with pytest.raises(UnknownSignalError, match="unknown signal"):
        compile_it(signals=("mean_time_to_enlightenment",))


def test_an_unknown_signal_lists_what_is_known() -> None:
    with pytest.raises(UnknownSignalError) as caught:
        compile_it(signals=("nonsense",))
    assert "p95_latency" in str(caught.value)
    assert set(known_signals()) >= {"p95_latency", "error_rate", "row_count"}


def test_a_signal_its_inputs_cannot_support_fails_at_compile_time() -> None:
    """Rather than at execution time, when it has already cost something."""
    with pytest.raises(CompilationError, match="columns the inputs do not provide"):
        compile_it(signals=("database_calls_per_request",))


def test_the_nearest_strategy_is_not_supported() -> None:
    """Ambiguous by definition: nearest in which direction?"""
    ambiguous = Join(
        left="public.request_logs",
        right="public.deploy_events",
        on=("service",),
        temporal=TemporalJoin(strategy=TemporalJoinStrategy.NEAREST),
    )
    with pytest.raises(CompilationError, match="not supported in v1"):
        compile_it(joins=(ambiguous,))


def test_a_temporal_join_needs_time_on_both_sides() -> None:
    without_time = manifests()
    without_time["public.deploy_events"] = deploys().model_copy(
        update={"dataset_schema": deploys().dataset_schema.model_copy(update={"time_field": None})}
    )
    with pytest.raises(CompilationError, match="needs a time field on both"):
        compile_analysis(analysis(), without_time, generated_at=AT)


def test_an_analysis_with_no_signals_is_refused() -> None:
    with pytest.raises(CompilationError, match="nothing to select"):
        compile_analysis(analysis(signals=(), normalize=()), manifests(), generated_at=AT)


def test_missing_manifests_are_refused() -> None:
    """Engine resolution notices first, since it has nothing to resolve from."""
    with pytest.raises(CompilationError, match="no manifests for the declared inputs"):
        compile_analysis(analysis(), {}, generated_at=AT)


def test_a_partially_known_input_set_is_refused() -> None:
    only_logs = {"public.request_logs": logs()}
    with pytest.raises(CompilationError, match="no manifest for inputs"):
        compile_analysis(analysis(), only_logs, engine=ExecutionEngine.POSTGRES, generated_at=AT)


# --- engine selection ------------------------------------------------------


def test_auto_resolves_from_where_the_data_lives() -> None:
    assert resolve_engine(analysis(), manifests("postgres")) is ExecutionEngine.POSTGRES
    assert resolve_engine(analysis(), manifests("duckdb")) is ExecutionEngine.DUCKDB


def test_an_explicit_engine_wins() -> None:
    chosen = analysis(engine=ExecutionEngine.DUCKDB)
    assert resolve_engine(chosen, manifests("postgres")) is ExecutionEngine.DUCKDB


def test_inputs_spanning_engines_are_refused() -> None:
    """Better to say so than to pick one and read the rest wrongly."""
    mixed = {"public.request_logs": logs("postgres"), "public.deploy_events": deploys("duckdb")}
    with pytest.raises(CompilationError, match="span more than one engine"):
        resolve_engine(analysis(), mixed)


def test_an_unsupported_adapter_is_refused() -> None:
    with pytest.raises(CompilationError, match="no engine for adapter"):
        resolve_engine(analysis(), manifests("cassandra"))


# --- artifacts -------------------------------------------------------------


def test_an_empty_artifact_is_rejected() -> None:
    with pytest.raises(ValueError, match="not an artifact"):
        GeneratedArtifact(analysis="a", engine="postgres", body="  ", generated_at=AT)


def test_an_artifact_can_be_previewed() -> None:
    preview = compile_it().preview(lines=3)
    # Three lines of body, plus the line saying how much was elided.
    assert len(preview.splitlines()) == 4
    assert "more lines" in preview


def test_the_compiled_sql_orders_by_the_grouping_keys() -> None:
    """Found by running the same artifact on two engines.

    PostgreSQL and DuckDB returned the groups in different orders, and two
    things downstream read rows by position: cross-engine comparison, and
    finding derivation, which takes the first group as the baseline and the
    last as the current. Without an ORDER BY, where an Analysis ran decided
    which direction its findings pointed.
    """
    body = compile_it().body

    # rindex, not index: percentile_cont carries its own ORDER BY inside the
    # projection, and matching that one would pass without an outer ordering.
    group_by = body[body.rindex("GROUP BY") :]
    grouping, _, ordering = group_by.partition("ORDER BY")
    assert ordering, "the compiled query does not order its groups"
    assert ordering.split() == grouping.replace("GROUP BY", "").split(), (
        "the ordering must be exactly the grouping keys, or row position "
        "still depends on the engine"
    )


def test_ordering_does_not_change_the_artifact_identity_rules() -> None:
    """Compiling twice still yields one hash: ordering is derived, not chosen."""
    assert compile_it().content_hash == compile_it().content_hash
