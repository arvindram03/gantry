"""Movement and Analysis specs as siblings.

The point of these tests is not that each spec parses, but that both parse
through one base with one verification vocabulary.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from gantry.core.verification import (
    JoinCoverageCheck,
    NullRateCheck,
    RowExpansionCheck,
    TemporalAlignmentCheck,
)
from gantry.spec import (
    AnalysisSpec,
    CheckName,
    MovementSpec,
    OperationSpec,
    OrderingScope,
    SpecValidationError,
    UnsupportedKindError,
    load_analysis_spec,
    load_movement_spec,
    load_spec,
    spec_json_schema,
)

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"
MOVEMENT_EXAMPLE = EXAMPLES / "movement-orders-replication.yaml"
ANALYSIS_EXAMPLE = EXAMPLES / "analysis-checkout-latency.yaml"

MINIMAL_MOVEMENT = """
apiVersion: gantry.dev/v1alpha1
kind: Movement
metadata:
  name: m
source: {adapter: postgres, connectionRef: src}
destination: {adapter: postgres, connectionRef: dst}
strategy: {mode: snapshot}
datasets:
  - name: orders
    source: public.orders
    target: public.orders
    key: {columns: [order_id]}
"""

MINIMAL_ANALYSIS = """
apiVersion: gantry.dev/v1alpha1
kind: Analysis
metadata:
  name: a
inputs:
  - dataset: logs
"""


def movement(extra: str = "") -> MovementSpec:
    return load_movement_spec("<test>", text=MINIMAL_MOVEMENT + extra)


def analysis(extra: str = "") -> AnalysisSpec:
    return load_analysis_spec("<test>", text=MINIMAL_ANALYSIS + extra)


# --- shared base -----------------------------------------------------------


def test_both_kinds_share_the_operation_base() -> None:
    assert isinstance(movement(), OperationSpec)
    assert isinstance(analysis(), OperationSpec)


def test_both_kinds_expose_a_name_through_the_base() -> None:
    assert movement().name == "m"
    assert analysis().name == "a"


def test_loader_dispatches_on_kind() -> None:
    assert isinstance(load_spec(MOVEMENT_EXAMPLE), MovementSpec)
    assert isinstance(load_spec(ANALYSIS_EXAMPLE), AnalysisSpec)


def test_typed_loaders_reject_the_other_kind() -> None:
    with pytest.raises(UnsupportedKindError, match="Analysis"):
        load_analysis_spec(MOVEMENT_EXAMPLE)
    with pytest.raises(UnsupportedKindError, match="Movement"):
        load_movement_spec(ANALYSIS_EXAMPLE)


# --- one verification vocabulary -------------------------------------------


def test_movement_and_analysis_verification_share_one_model() -> None:
    """Bare names and parameterised entries produce the same requirement type."""
    from_movement = movement(
        "    verification:\n      required: [row_count, chunk_checksum]\n"
    ).datasets[0]
    assert from_movement.verification.required[0].check is CheckName.ROW_COUNT

    from_analysis = analysis("verify:\n  - rowExpansion: {max: 1.1}\n").verify
    assert from_analysis[0].check is CheckName.ROW_EXPANSION

    assert (
        type(from_movement.verification.required[0]).__mro__[1] is type(from_analysis[0]).__mro__[1]
    )


def test_analysis_checks_parse_their_parameters() -> None:
    """The discriminated union must yield the right concrete check type."""
    by_name = {check.check: check for check in load_analysis_spec(ANALYSIS_EXAMPLE).verify}

    expansion = by_name[CheckName.ROW_EXPANSION]
    assert isinstance(expansion, RowExpansionCheck)
    assert expansion.max == 1.1

    coverage = by_name[CheckName.JOIN_COVERAGE]
    assert isinstance(coverage, JoinCoverageCheck)
    assert coverage.min == 0.95

    nulls = by_name[CheckName.NULL_RATE]
    assert isinstance(nulls, NullRateCheck)
    assert nulls.field == "trace_id"
    assert nulls.max == 0.05

    alignment = by_name[CheckName.TEMPORAL_ALIGNMENT]
    assert isinstance(alignment, TemporalAlignmentCheck)
    assert alignment.max_difference == timedelta(minutes=5)


def test_movement_may_use_parameterised_checks_too() -> None:
    spec = movement(
        "    verification:\n      required:\n        - nullRate: {field: x, max: 0.1}\n"
    )
    check = spec.datasets[0].verification.required[0]
    assert isinstance(check, NullRateCheck)
    assert check.max == 0.1


def test_parameterless_check_rejects_parameters() -> None:
    with pytest.raises(SpecValidationError, match="takes no parameters"):
        movement("    verification:\n      required:\n        - row_count: {max: 1}\n")


def test_verification_entry_with_two_keys_is_rejected() -> None:
    with pytest.raises(SpecValidationError, match="exactly one check name"):
        analysis("verify:\n  - {rowExpansion: {max: 1.1}, joinCoverage: {min: 0.5}}\n")


def test_unknown_check_names_its_field() -> None:
    with pytest.raises(SpecValidationError) as caught:
        analysis("verify:\n  - notACheck: {}\n")
    assert any("verify" in line for line in caught.value.errors)


# --- Movement --------------------------------------------------------------


def test_movement_example_parses_and_normalizes() -> None:
    spec = load_movement_spec(MOVEMENT_EXAMPLE)
    assert spec.name == "orders-replication"
    assert [d.name for d in spec.datasets] == ["customers", "orders"]

    customers, orders = spec.datasets
    assert customers.ordering.scope is OrderingScope.KEY
    assert customers.partitioning is not None
    assert customers.partitioning.rows_per_partition == 5_000_000
    assert orders.partitioning is not None
    assert orders.partitioning.interval == timedelta(days=1)
    assert orders.depends_on == ("customers",)
    assert spec.runtime.max_concurrency == 32
    assert spec.runtime.rate_limits.source_rows_per_second == 100_000


def test_ordering_scope_defaults_are_explicit_after_normalization() -> None:
    """An implicit ordering guarantee is not a guarantee."""
    assert movement().datasets[0].ordering.scope is OrderingScope.NONE


def test_key_ordering_requires_a_version_field() -> None:
    with pytest.raises(SpecValidationError, match="requires ordering"):
        movement("    ordering: {scope: key}\n")


def test_key_ordering_accepts_a_version_field() -> None:
    spec = movement("    ordering: {scope: key, versionField: source_lsn}\n")
    assert spec.datasets[0].ordering.version_field == "source_lsn"


@pytest.mark.parametrize(
    ("partitioning", "missing"),
    [
        ("{strategy: range, column: id}", "rowsPerPartition"),
        ("{strategy: time_range, column: created_at}", "interval"),
        ("{strategy: hash, column: id}", "buckets"),
    ],
)
def test_partition_strategy_requires_its_parameter(partitioning: str, missing: str) -> None:
    with pytest.raises(SpecValidationError, match=missing):
        movement(f"    partitioning: {partitioning}\n")


def test_streaming_strategy_requires_cdc() -> None:
    text = MINIMAL_MOVEMENT.replace("mode: snapshot", "mode: snapshot_then_stream")
    with pytest.raises(SpecValidationError, match="requires a cdc block"):
        load_movement_spec("<test>", text=text)


def test_unknown_dependency_is_rejected() -> None:
    with pytest.raises(SpecValidationError, match="unknown datasets"):
        movement("    dependsOn: [ghost]\n")


def test_self_dependency_is_rejected() -> None:
    with pytest.raises(SpecValidationError, match="cannot depend on itself"):
        movement("    dependsOn: [orders]\n")


def test_dependency_cycle_is_rejected_at_spec_time() -> None:
    text = (
        MINIMAL_MOVEMENT
        + """    dependsOn: [customers]
  - name: customers
    source: public.customers
    target: public.customers
    dependsOn: [orders]
    key: {columns: [customer_id]}
"""
    )
    with pytest.raises(SpecValidationError, match="dependency cycle"):
        load_movement_spec("<test>", text=text)


def test_duplicate_dataset_names_are_rejected() -> None:
    text = (
        MINIMAL_MOVEMENT
        + """  - name: orders
    source: other.orders
    target: other.orders
    key: {columns: [order_id]}
"""
    )
    with pytest.raises(SpecValidationError, match="duplicate dataset names"):
        load_movement_spec("<test>", text=text)


def test_cutover_and_rollback_parse_but_are_flagged_as_migration_blocks() -> None:
    """A Movement does not imply a cutover; the example carries them anyway."""
    spec = load_movement_spec(MOVEMENT_EXAMPLE)
    assert spec.has_migration_blocks
    assert spec.cutover is not None
    assert spec.cutover.gates.max_cdc_lag == timedelta(seconds=2)
    assert spec.rollback is not None
    assert spec.rollback.window == timedelta(hours=24)


def test_movement_without_migration_blocks_is_not_flagged() -> None:
    assert not movement().has_migration_blocks


def test_connection_is_a_reference_not_a_secret() -> None:
    assert movement().source.connection_ref == "src"


# --- Analysis --------------------------------------------------------------


def test_analysis_example_parses_and_normalizes() -> None:
    spec = load_analysis_spec(ANALYSIS_EXAMPLE)
    assert spec.name == "checkout-latency-regression"
    assert spec.input_names == (
        "application_logs",
        "traces",
        "deploy_events",
        "postgres_metrics",
    )
    assert spec.window is not None
    assert spec.normalize.fields["request_id"].aliases == ("request_id", "trace_id")
    assert len(spec.joins) == 2


def test_join_on_field_survives_yaml_boolean_coercion() -> None:
    """`on:` is a YAML 1.1 boolean; it must stay a field name."""
    spec = load_analysis_spec(ANALYSIS_EXAMPLE)
    assert spec.joins[0].on == ("request_id",)


def test_temporal_join_parses_strategy_and_distance() -> None:
    temporal = load_analysis_spec(ANALYSIS_EXAMPLE).joins[1].temporal
    assert temporal is not None
    assert temporal.strategy.value == "nearest_preceding"
    assert temporal.max_distance == timedelta(hours=24)


def test_continuous_mode_is_rejected_in_v1() -> None:
    with pytest.raises(SpecValidationError, match="not supported in v1"):
        analysis("mode: continuous\n")


def test_unsupported_engine_is_rejected_with_the_supported_list() -> None:
    with pytest.raises(SpecValidationError, match="not supported in v1"):
        analysis("execution: {engine: spark}\n")


@pytest.mark.parametrize("engine", ["auto", "postgres", "duckdb"])
def test_v1_engines_are_accepted(engine: str) -> None:
    assert analysis(f"execution: {{engine: {engine}}}\n").execution.engine.value == engine


def test_join_referencing_a_non_input_is_rejected() -> None:
    with pytest.raises(SpecValidationError, match="not inputs"):
        analysis("joins:\n  - {left: logs, right: ghost, on: [id]}\n")


def test_join_of_a_dataset_with_itself_is_rejected() -> None:
    with pytest.raises(SpecValidationError, match="must differ"):
        analysis("joins:\n  - {left: logs, right: logs, on: [id]}\n")


def test_duplicate_inputs_are_rejected() -> None:
    with pytest.raises(SpecValidationError, match="duplicate inputs"):
        analysis("  - dataset: logs\n")


def test_window_must_be_absolute_or_sliding_not_both() -> None:
    with pytest.raises(SpecValidationError, match="not both"):
        analysis('window: {start: "2026-09-03T00:00:00Z", type: sliding, duration: 30m}\n')


def test_absolute_window_requires_both_bounds() -> None:
    with pytest.raises(SpecValidationError, match="requires both start and end"):
        analysis('window: {start: "2026-09-03T00:00:00Z"}\n')


def test_window_bounds_must_be_ordered() -> None:
    with pytest.raises(SpecValidationError, match="must be after"):
        analysis('window: {start: "2026-09-04T00:00:00Z", end: "2026-09-03T00:00:00Z"}\n')


def test_naive_window_bounds_are_rejected() -> None:
    with pytest.raises(SpecValidationError, match="timezone-aware"):
        analysis('window: {start: "2026-09-03T00:00:00", end: "2026-09-04T00:00:00"}\n')


# --- schema ----------------------------------------------------------------


@pytest.mark.parametrize("kind", ["Dataset", "Movement", "Analysis"])
def test_json_schema_is_generated_for_every_kind(kind: str) -> None:
    import json

    schema = json.loads(spec_json_schema(kind))
    assert schema["title"] == f"Gantry {kind}"


def test_json_schema_rejects_unknown_kind() -> None:
    with pytest.raises(UnsupportedKindError):
        spec_json_schema("Nonsense")
