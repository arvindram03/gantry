"""M0: the abstractions hold before any database is attached.

The load-bearing claims:
  - a Movement and an Analysis complete through one engine
  - a worker dying mid-operation resumes from checkpoint, with no lost work
    and no duplicated effect
  - a verification failure produces VERIFICATION_FAILED with evidence
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from gantry.adapters.fake import FakeTarget, FaultSpec, SimulatedCrashError
from gantry.analysis.planner import compile_analysis
from gantry.core.operation import OperationState
from gantry.core.positions import CheckpointScope
from gantry.lifecycle.engine import Outcome, StageResult
from gantry.lifecycle.plan import NodeKind, PlanVersion, node_id
from gantry.movement.planner import compile_movement
from gantry.scheduler.backend import TaskState
from gantry.scheduler.worker import Worker
from gantry.simulator.runner import Simulator, duplicate_free
from gantry.spec import load_analysis_spec, load_movement_spec

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"
AT = datetime(2026, 9, 11, tzinfo=UTC)

pytestmark = pytest.mark.chaos


def movement_plan() -> PlanVersion:
    domain = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml").to_movement()
    return compile_movement(domain, created_at=AT)


def analysis_plan() -> PlanVersion:
    domain = load_analysis_spec(EXAMPLES / "analysis-checkout-latency.yaml").to_analysis()
    return compile_analysis(domain, created_at=AT)


# --- both operation types, one engine --------------------------------------


@pytest.mark.parametrize("plan", [movement_plan(), analysis_plan()], ids=["movement", "analysis"])
async def test_both_operation_types_complete_through_one_engine(plan: PlanVersion) -> None:
    report = await Simulator(plan, start=AT, verification=duplicate_free).run()
    assert report.completed
    assert len(report.completed_nodes) == len(plan.nodes)
    assert not report.crashes


@pytest.mark.parametrize("plan", [movement_plan(), analysis_plan()], ids=["movement", "analysis"])
async def test_every_node_is_checkpointed(plan: PlanVersion) -> None:
    simulator = Simulator(plan, start=AT)
    await simulator.run()
    checkpoints = await simulator.checkpoints.all(plan.operation)
    assert len(checkpoints) == len(plan.nodes)


async def test_dependencies_are_respected_under_leasing() -> None:
    """A node must not run before the nodes it depends on are done."""
    plan = movement_plan()
    simulator = Simulator(plan, start=AT)
    report = await simulator.run()

    order = report.completed_nodes
    customers = node_id("orders-replication", NodeKind.SNAPSHOT_PARTITION, "customers")
    orders = node_id("orders-replication", NodeKind.SNAPSHOT_PARTITION, "orders")
    assert order.index(customers) < order.index(orders)


# --- the hardest case: crash after commit, before checkpoint ---------------


async def test_crash_after_commit_replays_without_duplicating() -> None:
    """Section 8.7's worst case, made routine.

    The effect commits, the process dies before the checkpoint advances, the
    lease expires, another worker replays the node. Idempotency is what keeps
    that from producing a second effect.
    """
    plan = movement_plan()
    victim = node_id("orders-replication", NodeKind.SNAPSHOT_PARTITION, "customers")
    target = FakeTarget()

    report = await Simulator(
        plan,
        faults=FaultSpec(crash_after_commit={victim}),
        target=target,
        verification=duplicate_free,
        start=AT,
    ).run()

    assert report.crashes, "the crash should have happened"
    assert report.completed, "and the operation should still finish"
    # The node ran twice; the effect exists once.
    assert target.effect_count == len(plan.nodes)
    assert target.suppressed_duplicates == 1
    assert "duplicates suppressed" in report.verification_evidence


async def test_crash_before_commit_loses_no_work() -> None:
    plan = movement_plan()
    victim = node_id("orders-replication", NodeKind.CREATE_SCHEMA, "orders")
    target = FakeTarget()

    report = await Simulator(
        plan, faults=FaultSpec(crash_before_commit={victim}), target=target, start=AT
    ).run()

    assert report.completed
    assert target.effect_count == len(plan.nodes)
    assert target.suppressed_duplicates == 0


async def test_checkpoint_is_not_recorded_when_the_worker_dies_after_commit() -> None:
    """Progress metadata must never run ahead of durable state."""
    plan = movement_plan()
    victim = node_id("orders-replication", NodeKind.SNAPSHOT_PARTITION, "customers")
    simulator = Simulator(plan, faults=FaultSpec(crash_after_commit={victim}), start=AT)
    await simulator.backend.submit(plan)

    worker = Worker(
        "doomed",
        simulator.backend,
        simulator.checkpoints,
        simulator.workload,
        plan,
        clock=simulator.clock,
    )
    with pytest.raises(SimulatedCrashError):
        await worker.run()

    # The effect landed, and no checkpoint claims it did.
    assert simulator.target.effect_count >= 1
    assert (
        await simulator.checkpoints.get(plan.operation, CheckpointScope.PARTITION, victim)
    ) is None


# --- duplicate delivery ----------------------------------------------------


async def test_duplicate_delivery_is_a_no_op() -> None:
    plan = analysis_plan()
    everything = {node.id for node in plan.nodes}
    target = FakeTarget()

    report = await Simulator(
        plan,
        faults=FaultSpec(duplicate_delivery=everything),
        target=target,
        verification=duplicate_free,
        start=AT,
    ).run()

    assert report.completed
    assert target.apply_calls == 2 * len(plan.nodes)
    assert target.effect_count == len(plan.nodes)
    assert target.suppressed_duplicates == len(plan.nodes)


# --- transient failures and quarantine -------------------------------------


async def test_transient_failures_are_retried() -> None:
    plan = analysis_plan()
    flaky = node_id("checkout-latency-regression", NodeKind.EXECUTE_ARTIFACT)

    report = await Simulator(plan, faults=FaultSpec(transient_failures={flaky: 3}), start=AT).run()

    assert report.completed
    assert not report.quarantined_nodes


async def test_a_poison_task_is_quarantined_not_retried_forever() -> None:
    """One bad task must not consume the worker pool indefinitely."""
    plan = analysis_plan()
    poison = node_id("checkout-latency-regression", NodeKind.EXECUTE_ARTIFACT)

    simulator = Simulator(plan, faults=FaultSpec(transient_failures={poison: 999}), start=AT)
    report = await simulator.run()

    assert not report.completed
    assert poison in report.quarantined_nodes
    tasks = {t.node_id: t for t in await simulator.backend.tasks(plan.operation)}
    assert tasks[poison].state is TaskState.QUARANTINED
    assert tasks[poison].last_error is not None


# --- verification decides --------------------------------------------------


async def test_verification_failure_is_reported_with_evidence_not_a_crash() -> None:
    """An engine can finish every node and still not produce a usable result."""

    def reject(target: FakeTarget) -> StageResult:
        return StageResult(
            outcome=Outcome.VERIFICATION_FAILED,
            detail=f"rowExpansion 20.2x exceeds max 1.1 across {target.effect_count} effects",
        )

    report = await Simulator(analysis_plan(), verification=reject, start=AT).run()

    assert report.final_state is OperationState.VERIFICATION_FAILED
    assert not report.completed
    assert "rowExpansion" in report.verification_evidence


async def test_incomplete_execution_does_not_reach_verification() -> None:
    plan = analysis_plan()
    poison = node_id("checkout-latency-regression", NodeKind.GENERATE_ARTIFACT)
    report = await Simulator(
        plan,
        faults=FaultSpec(transient_failures={poison: 999}),
        verification=duplicate_free,
        start=AT,
    ).run()

    assert report.final_state is OperationState.FAILED
    assert report.verification_evidence == ""
