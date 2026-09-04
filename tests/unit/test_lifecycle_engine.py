"""The lifecycle engine.

The load-bearing test in this file is that a Movement and an Analysis traverse
the same engine. If that ever needs a second code path, the shared-lifecycle
design has failed and Day 16 becomes much more expensive.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from gantry.analysis.planner import compile_analysis
from gantry.core.operation import LifecycleStage, OperationState
from gantry.lifecycle.engine import (
    LifecycleEngine,
    Outcome,
    Stage,
    StageContext,
    StageResult,
    stage_order,
)
from gantry.lifecycle.plan import PlanVersion
from gantry.movement.planner import compile_movement
from gantry.spec import load_analysis_spec, load_movement_spec

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"
AT = datetime(2026, 9, 10, tzinfo=UTC)


def ticking_clock() -> Callable[[], datetime]:
    moments = iter(AT + timedelta(seconds=i) for i in range(1000))
    return lambda: next(moments)


def movement_plan() -> PlanVersion:
    domain = load_movement_spec(EXAMPLES / "movement-orders-replication.yaml").to_movement()
    return compile_movement(domain, created_at=AT)


def analysis_plan() -> PlanVersion:
    domain = load_analysis_spec(EXAMPLES / "analysis-checkout-latency.yaml").to_analysis()
    return compile_analysis(domain, created_at=AT)


def engine(**stages: Stage) -> LifecycleEngine:
    mapped = {LifecycleStage(name): stage for name, stage in stages.items()}
    return LifecycleEngine(mapped, clock=ticking_clock())


def succeed(context: StageContext) -> StageResult:
    return StageResult(outcome=Outcome.OK)


def fail_with(outcome: Outcome, detail: str = "") -> Stage:
    def stage(context: StageContext) -> StageResult:
        return StageResult(outcome=outcome, detail=detail)

    return stage


# --- one engine, both operation types --------------------------------------


@pytest.mark.parametrize(
    "plan_factory", [movement_plan, analysis_plan], ids=["movement", "analysis"]
)
def test_both_operation_types_run_the_same_engine(plan_factory: object) -> None:
    record = engine().run(plan_factory())  # type: ignore[operator]
    assert record.completed
    assert record.final_state is OperationState.COMPLETED


@pytest.mark.parametrize(
    "plan_factory", [movement_plan, analysis_plan], ids=["movement", "analysis"]
)
def test_both_operation_types_visit_every_stage(plan_factory: object) -> None:
    record = engine().run(plan_factory())  # type: ignore[operator]
    assert [stage for stage, _ in record.stages] == list(stage_order())


def test_unregistered_stages_are_no_ops_not_skips() -> None:
    """An operation compiling straight to an engine API still passes GENERATE."""
    record = engine().run(analysis_plan())
    generate = record.stage_result(LifecycleStage.GENERATE)
    assert generate is not None
    assert generate.detail == "no-op"


# --- the guarantee boundary ------------------------------------------------


def test_execution_success_only_reaches_verifying() -> None:
    """An engine reporting SUCCESS does not complete an Operation."""
    reached: list[OperationState] = []

    def record_state(context: StageContext) -> StageResult:
        return StageResult(outcome=Outcome.OK)

    record = LifecycleEngine({LifecycleStage.EXECUTE: record_state}, clock=ticking_clock()).run(
        analysis_plan()
    )
    reached = [step.to_state for step in record.transitions]
    assert OperationState.VERIFYING in reached
    assert reached.index(OperationState.VERIFYING) < reached.index(OperationState.COMPLETED)


def test_verification_failure_blocks_completion() -> None:
    """The clearest expression of the boundary: work succeeded, output rejected."""
    record = LifecycleEngine(
        {
            LifecycleStage.EXECUTE: succeed,
            LifecycleStage.VERIFY: fail_with(
                Outcome.VERIFICATION_FAILED, "rowExpansion 20.2 > 1.1"
            ),
        },
        clock=ticking_clock(),
    ).run(analysis_plan())

    assert not record.completed
    assert record.final_state is OperationState.VERIFICATION_FAILED
    assert "rowExpansion" in record.transitions[-1].reason


def test_validation_failure_returns_to_draft_for_repair() -> None:
    record = LifecycleEngine(
        {LifecycleStage.VALIDATE: fail_with(Outcome.INVALID, "unknown column trace_id")},
        clock=ticking_clock(),
    ).run(analysis_plan())

    assert record.final_state is OperationState.DRAFT
    assert "unknown column" in record.transitions[-1].reason


def test_validation_failure_never_reaches_execute() -> None:
    executed = False

    def execute(context: StageContext) -> StageResult:
        nonlocal executed
        executed = True
        return StageResult(outcome=Outcome.OK)

    LifecycleEngine(
        {LifecycleStage.VALIDATE: fail_with(Outcome.INVALID), LifecycleStage.EXECUTE: execute},
        clock=ticking_clock(),
    ).run(analysis_plan())
    assert not executed


def test_a_failed_stage_stops_the_run() -> None:
    record = LifecycleEngine(
        {LifecycleStage.GENERATE: fail_with(Outcome.FAILED, "engine unreachable")},
        clock=ticking_clock(),
    ).run(analysis_plan())

    assert record.final_state is OperationState.FAILED
    assert [stage for stage, _ in record.stages] == [LifecycleStage.PLAN, LifecycleStage.GENERATE]


def test_stages_cannot_advance_state_themselves() -> None:
    """A stage returns an outcome; the engine decides what it means."""
    assert not hasattr(StageResult(outcome=Outcome.OK), "state")


# --- audit -----------------------------------------------------------------


def test_every_transition_is_recorded_with_a_reason() -> None:
    record = engine().run(movement_plan())
    assert record.transitions
    for step in record.transitions:
        assert step.reason.strip()
        assert step.plan_version == 1
        assert step.occurred_at.tzinfo is not None


def test_transitions_form_a_connected_chain() -> None:
    """Each transition must start where the previous one ended."""
    record = engine().run(movement_plan())
    states = [OperationState.DRAFT] + [step.to_state for step in record.transitions]
    for step, expected in zip(record.transitions, states, strict=False):
        assert step.from_state is expected


def test_stage_context_carries_the_plan() -> None:
    seen: list[StageContext] = []

    def capture(context: StageContext) -> StageResult:
        seen.append(context)
        return StageResult(outcome=Outcome.OK)

    plan = movement_plan()
    LifecycleEngine({LifecycleStage.EXECUTE: capture}, clock=ticking_clock()).run(plan)
    assert seen[0].plan.content_hash == plan.content_hash
    assert seen[0].operation == plan.operation
