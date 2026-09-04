"""Plan compilation, determinism and replan rules."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from gantry.analysis.planner import compile_analysis
from gantry.core.operation import LifecycleStage, OperationType
from gantry.lifecycle.plan import (
    NodeKind,
    PlanNode,
    PlanVersion,
    ReplanRequiredError,
    next_version,
    node_id,
)
from gantry.movement.model import OrderingScope
from gantry.movement.planner import compile_movement
from gantry.spec import load_analysis_spec, load_movement_spec

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "spec" / "examples"
GOLDEN = ROOT / "tests" / "golden"

AT = datetime(2026, 9, 10, tzinfo=UTC)


def movement_plan(**kwargs: object) -> PlanVersion:
    domain = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml").to_movement()
    return compile_movement(domain, created_at=AT, **kwargs)  # type: ignore[arg-type]


def analysis_plan() -> PlanVersion:
    domain = load_analysis_spec(EXAMPLES / "analysis-checkout-latency.yaml").to_analysis()
    return compile_analysis(domain, created_at=AT)


# --- determinism -----------------------------------------------------------


def test_node_ids_are_stable_across_processes() -> None:
    """Derived from content, not from `hash()`, which is randomised per process."""
    assert node_id("m", NodeKind.DISCOVER) == node_id("m", NodeKind.DISCOVER)
    assert node_id("m", NodeKind.DISCOVER) != node_id("m", NodeKind.FINALIZE)
    assert node_id("m", NodeKind.VERIFY_DATASET, "a") != node_id("m", NodeKind.VERIFY_DATASET, "b")


def test_recompiling_produces_an_identical_plan() -> None:
    assert movement_plan().content_hash == movement_plan().content_hash
    assert analysis_plan().content_hash == analysis_plan().content_hash


def test_compilation_time_is_outside_the_content_hash() -> None:
    """Two compilations of the same Operation must agree regardless of when they ran."""
    domain = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml").to_movement()
    early = compile_movement(domain, created_at=AT)
    late = compile_movement(domain, created_at=AT + timedelta(days=30))
    assert early.content_hash == late.content_hash
    assert early.created_at != late.created_at


def test_plan_hash_follows_the_domain_not_the_spec() -> None:
    """A cosmetic spec change must not invalidate a running plan.

    Section 8.5 requires a replay to retain the same PlanVersion, so writing
    the deprecated apiVersion or the alternate time-field spelling cannot
    change the plan.
    """
    original = (EXAMPLES / "movement-orders-replication.yaml").read_text()
    restyled = original.replace("gantry.dev/v1alpha1", "gantry.io/v1alpha1")
    domain = load_movement_spec("<test>", text=restyled).to_movement()
    assert compile_movement(domain, created_at=AT).content_hash == movement_plan().content_hash


@pytest.mark.parametrize("name", ["movement-orders-replication", "analysis-checkout-latency"])
def test_plan_matches_its_golden_file(name: str) -> None:
    """Byte-for-byte plan stability across runs and machines.

    Regenerate deliberately with GANTRY_UPDATE_GOLDEN=1 when a plan change is
    intended; an accidental change fails here.
    """
    plan = movement_plan() if name.startswith("movement") else analysis_plan()
    golden = GOLDEN / f"{name}.plan.json"
    rendered = json.dumps(json.loads(plan.canonical_json()), indent=2, sort_keys=True) + "\n"

    if os.environ.get("GANTRY_UPDATE_GOLDEN"):  # pragma: no cover - maintenance path
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_text(rendered)

    assert golden.read_text() == rendered


# --- graph shape -----------------------------------------------------------


def test_movement_plan_orders_dependent_datasets() -> None:
    plan = movement_plan()
    order = [node.id for node in plan.topological_order()]
    customers = node_id("orders-replication", NodeKind.SNAPSHOT_PARTITION, "customers")
    orders = node_id("orders-replication", NodeKind.SNAPSHOT_PARTITION, "orders")
    assert order.index(customers) < order.index(orders)


def test_cdc_starts_before_any_snapshot() -> None:
    """Capturing the position first is what closes the snapshot/CDC gap."""
    plan = movement_plan()
    order = [node.id for node in plan.topological_order()]
    start_cdc = node_id("orders-replication", NodeKind.START_CDC)
    snapshot = node_id("orders-replication", NodeKind.SNAPSHOT_PARTITION, "customers")
    assert order.index(start_cdc) < order.index(snapshot)


def test_snapshot_only_movement_has_no_cdc_nodes() -> None:
    text = (EXAMPLES / "movement-orders-replication.yaml").read_text()
    text = text.replace("mode: snapshot_then_stream", "mode: snapshot")
    domain = load_movement_spec("<test>", text=text).to_movement()
    kinds = {node.kind for node in compile_movement(domain, created_at=AT).nodes}
    assert NodeKind.START_CDC not in kinds
    assert NodeKind.APPLY_CDC not in kinds


def test_analysis_plan_follows_the_lifecycle_stages() -> None:
    """Generation must not reach execution without passing validation."""
    stages = [node.stage for node in analysis_plan().topological_order()]
    assert stages == [
        LifecycleStage.GENERATE,
        LifecycleStage.VALIDATE,
        LifecycleStage.EXECUTE,
        LifecycleStage.VERIFY,
        LifecycleStage.RESULT,
    ]


def test_every_plan_ends_at_finalize() -> None:
    for plan in (movement_plan(), analysis_plan()):
        assert plan.topological_order()[-1].kind is NodeKind.FINALIZE


def test_topological_order_is_reproducible() -> None:
    """Ties are broken by id, so the order is reproducible rather than merely valid."""
    first = [node.id for node in movement_plan().topological_order()]
    second = [node.id for node in movement_plan().topological_order()]
    assert first == second


def test_operation_types_are_recorded() -> None:
    assert movement_plan().operation_type is OperationType.MOVEMENT
    assert analysis_plan().operation_type is OperationType.ANALYSIS


# --- graph validation ------------------------------------------------------


def _node(identifier: str, depends_on: tuple[str, ...] = ()) -> PlanNode:
    return PlanNode(
        id=identifier,
        kind=NodeKind.FINALIZE,
        stage=LifecycleStage.RESULT,
        depends_on=depends_on,
    )


def test_duplicate_node_ids_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate node ids"):
        PlanVersion(
            operation="m",
            operation_type=OperationType.MOVEMENT,
            version=1,
            nodes=(_node("a"), _node("a")),
            guarantee_fingerprint="sha256:" + "0" * 64,
            created_at=AT,
        )


def test_dangling_dependency_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown nodes"):
        PlanVersion(
            operation="m",
            operation_type=OperationType.MOVEMENT,
            version=1,
            nodes=(_node("a", ("ghost",)),),
            guarantee_fingerprint="sha256:" + "0" * 64,
            created_at=AT,
        )


def test_dependency_cycle_is_rejected() -> None:
    with pytest.raises(ValueError, match="dependency cycle"):
        PlanVersion(
            operation="m",
            operation_type=OperationType.MOVEMENT,
            version=1,
            nodes=(_node("a", ("b",)), _node("b", ("a",))),
            guarantee_fingerprint="sha256:" + "0" * 64,
            created_at=AT,
        )


def test_self_dependency_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot depend on itself"):
        _node("a", ("a",))


def test_naive_created_at_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        PlanVersion(
            operation="m",
            operation_type=OperationType.MOVEMENT,
            version=1,
            nodes=(_node("a"),),
            guarantee_fingerprint="sha256:" + "0" * 64,
            created_at=datetime(2026, 9, 10),
        )


# --- replan rules ----------------------------------------------------------


def test_first_plan_is_version_one() -> None:
    assert next_version(None, "sha256:" + "a" * 64) == 1


def test_unchanged_guarantees_reuse_the_version() -> None:
    plan = movement_plan()
    assert next_version(plan, plan.guarantee_fingerprint) == plan.version


def test_changed_guarantees_require_an_explicit_replan() -> None:
    plan = movement_plan()
    with pytest.raises(ReplanRequiredError, match="explicit replan"):
        next_version(plan, "sha256:" + "b" * 64)


def test_explicit_replan_advances_the_version() -> None:
    plan = movement_plan()
    assert next_version(plan, "sha256:" + "b" * 64, replan=True) == plan.version + 1


def test_tuning_limits_does_not_require_a_replan() -> None:
    """Concurrency is adjustable within a plan version; ordering is not."""
    plan = movement_plan()
    domain = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml").to_movement()
    tuned = domain.model_copy(
        update={"limits": domain.limits.model_copy(update={"max_concurrency": 4})}
    )
    assert next_version(plan, tuned.guarantee_fingerprint()) == plan.version


def test_changing_ordering_requires_a_replan() -> None:
    plan = movement_plan()
    domain = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml").to_movement()
    dataset = domain.datasets[0]
    changed = dataset.model_copy(
        update={"ordering": dataset.ordering.model_copy(update={"scope": OrderingScope.NONE})}
    )
    mutated = domain.model_copy(update={"datasets": (changed, *domain.datasets[1:])})
    with pytest.raises(ReplanRequiredError):
        next_version(plan, mutated.guarantee_fingerprint())
