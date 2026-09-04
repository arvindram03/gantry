"""The spec/domain seam.

These tests exist to keep spec format cheap to change. If the runtime ever
binds to spec field names again, they should start failing.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from gantry.analysis.model import Analysis, ExecutionEngine
from gantry.core.dataset import DatasetManifest, PhysicalRef
from gantry.core.schema import DatasetSchema
from gantry.movement.model import Movement, MovementMode, OrderingScope, WriteMode
from gantry.spec import load_analysis_spec, load_movement_spec

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"
MOVEMENT_EXAMPLE = EXAMPLES / "movement-orders-replication.yaml"
ANALYSIS_EXAMPLE = EXAMPLES / "analysis-checkout-latency.yaml"


def movement() -> Movement:
    return load_movement_spec(MOVEMENT_EXAMPLE).to_movement()


def analysis() -> Analysis:
    return load_analysis_spec(ANALYSIS_EXAMPLE).to_analysis()


# --- the seam --------------------------------------------------------------


def test_movement_converts_to_a_domain_model() -> None:
    domain = movement()
    assert domain.name == "orders-replication"
    assert domain.mode is MovementMode.SNAPSHOT_THEN_STREAM
    assert [d.name for d in domain.datasets] == ["customers", "orders"]


def test_domain_movement_has_no_migration_fields() -> None:
    """A Movement does not imply a cutover, so the domain model has no such field.

    This is the clearest evidence the seam is real rather than ceremony: the
    spec carries cutover and rollback, and the domain model does not.
    """
    domain = movement()
    assert not hasattr(domain, "cutover")
    assert not hasattr(domain, "rollback")
    assert load_movement_spec(MOVEMENT_EXAMPLE).has_migration_blocks


def test_domain_model_uses_core_types_not_spec_types() -> None:
    dataset = movement().datasets[1]
    assert dataset.partitioning is not None
    # Durations become plain seconds; nothing downstream parses "1d".
    assert dataset.partitioning.interval_seconds == 86400
    assert dataset.ordering.scope is OrderingScope.KEY
    assert dataset.write_mode is WriteMode.UPSERT


def test_analysis_converts_to_a_domain_model() -> None:
    domain = analysis()
    assert domain.objective == "checkout latency regression"
    assert domain.inputs[0] == "application_logs"
    assert domain.engine is ExecutionEngine.AUTO
    assert domain.window is not None
    assert domain.window.duration == timedelta(days=1)


def test_analysis_normalization_is_ordered_deterministically() -> None:
    """A mapping in YAML has no order; the domain model must impose one."""
    canonical = [item.canonical for item in analysis().normalize]
    assert canonical == sorted(canonical)


def test_analysis_temporal_join_becomes_seconds() -> None:
    temporal = analysis().joins[1].temporal
    assert temporal is not None
    assert temporal.max_distance_seconds == 86400


# --- guarantee fingerprints ------------------------------------------------


def test_movement_fingerprint_is_stable() -> None:
    assert movement().guarantee_fingerprint() == movement().guarantee_fingerprint()


def test_tuning_concurrency_does_not_change_the_fingerprint() -> None:
    """Concurrency is a runtime adjustment, not a change of guarantees."""
    base = movement()
    tuned = base.model_copy(
        update={"limits": base.limits.model_copy(update={"max_concurrency": 4})}
    )
    assert tuned.guarantee_fingerprint() == base.guarantee_fingerprint()


def test_changing_ordering_scope_changes_the_fingerprint() -> None:
    """A different ordering guarantee is a different migration."""
    base = movement()
    dataset = base.datasets[0]
    changed = dataset.model_copy(
        update={"ordering": dataset.ordering.model_copy(update={"scope": OrderingScope.NONE})}
    )
    mutated = base.model_copy(update={"datasets": (changed, *base.datasets[1:])})
    assert mutated.guarantee_fingerprint() != base.guarantee_fingerprint()


def test_changing_verification_changes_the_fingerprint() -> None:
    base = movement()
    dataset = base.datasets[0]
    stripped = dataset.model_copy(update={"verification": ()})
    mutated = base.model_copy(update={"datasets": (stripped, *base.datasets[1:])})
    assert mutated.guarantee_fingerprint() != base.guarantee_fingerprint()


def test_analysis_engine_choice_does_not_change_the_fingerprint() -> None:
    """Which engine runs it does not change what the Result means."""
    base = analysis()
    on_duckdb = base.model_copy(update={"engine": ExecutionEngine.DUCKDB})
    assert on_duckdb.guarantee_fingerprint() == base.guarantee_fingerprint()


def test_analysis_join_change_changes_the_fingerprint() -> None:
    base = analysis()
    assert base.model_copy(update={"joins": ()}).guarantee_fingerprint() != (
        base.guarantee_fingerprint()
    )


def test_duplicate_datasets_are_rejected_in_the_domain_model_too() -> None:
    """Domain invariants are enforced independently of the spec layer."""
    base = movement()
    payload = base.model_dump()
    payload["datasets"] = [payload["datasets"][0], payload["datasets"][0]]
    with pytest.raises(ValueError, match="duplicate dataset names"):
        Movement.model_validate(payload)


# --- manifest hash stability ----------------------------------------------


def test_manifest_hash_excludes_defaults() -> None:
    """Additive schema changes must not re-version every registered Dataset.

    Excluding defaults means the hashed payload carries only what was actually
    declared, so a new optional field cannot perturb existing hashes.
    """
    manifest = DatasetManifest(
        name="orders",
        physical=PhysicalRef(adapter="postgres", reference="public.orders"),
        dataset_schema=DatasetSchema(keys=("order_id",)),
    )
    rendered = manifest.canonical_json()
    assert "access" not in rendered
    assert "statistics" not in rendered
    assert "sensitive_fields" not in rendered


def test_explicitly_set_defaults_hash_the_same_as_omitting_them() -> None:
    """Declaring the default value means the same thing as leaving it out."""
    from gantry.core.dataset import AccessPolicy, AgentAccessPolicy

    physical = PhysicalRef(adapter="postgres", reference="public.orders")
    implicit = DatasetManifest(name="orders", physical=physical)
    explicit = DatasetManifest(
        name="orders",
        physical=physical,
        access=AccessPolicy(agent_policy=AgentAccessPolicy.AGGREGATE_OR_MASKED),
    )
    assert implicit.content_hash == explicit.content_hash
