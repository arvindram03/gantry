# SPDX-License-Identifier: Apache-2.0
"""The lifecycle engine.

One engine runs both operation types. Movement and Analysis register stage
implementations; the engine owns sequencing, state transitions, failure
classification and the guarantee boundary itself.

The boundary is structural rather than conventional:

- an Operation cannot reach EXECUTE without passing VALIDATE
- an engine reporting success only advances it to VERIFY
- VERIFY decides whether the Result is trustworthy

A stage implementation cannot skip ahead, because it never advances state - it
returns an outcome and the engine decides what that means.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from gantry.core.operation import LifecycleStage, OperationState, OperationType
from gantry.lifecycle.plan import PlanVersion
from gantry.lifecycle.states import ActorKind, StateTransition, transition

_STAGE_ORDER: tuple[LifecycleStage, ...] = (
    LifecycleStage.PLAN,
    LifecycleStage.GENERATE,
    LifecycleStage.VALIDATE,
    LifecycleStage.EXECUTE,
    LifecycleStage.VERIFY,
    LifecycleStage.RESULT,
)

# Stages map onto states in two steps: entering a stage that represents work in
# progress, and completing one. Collapsing these into a single "state after
# success" is wrong - it lets EXECUTE jump straight from VALIDATED to VERIFYING
# and skip EXECUTING altogether.
_STAGE_ENTER: dict[LifecycleStage, OperationState | None] = {
    LifecycleStage.PLAN: None,
    LifecycleStage.GENERATE: None,
    LifecycleStage.VALIDATE: None,
    LifecycleStage.EXECUTE: OperationState.EXECUTING,
    LifecycleStage.VERIFY: OperationState.VERIFYING,
    LifecycleStage.RESULT: None,
}

_STAGE_DONE: dict[LifecycleStage, OperationState | None] = {
    LifecycleStage.PLAN: OperationState.PLANNED,
    LifecycleStage.GENERATE: OperationState.GENERATED,
    LifecycleStage.VALIDATE: OperationState.VALIDATED,
    # Finishing execution does not advance past EXECUTING. Only entering
    # verification does, and only verification can complete an Operation.
    LifecycleStage.EXECUTE: None,
    LifecycleStage.VERIFY: None,
    LifecycleStage.RESULT: OperationState.COMPLETED,
}


class Outcome(StrEnum):
    """What a stage reports. It never reports a state."""

    OK = "ok"
    # Distinct from FAILED: a verification failure means the work completed and
    # the output cannot be trusted, which is a different thing to a crash.
    VERIFICATION_FAILED = "verification_failed"
    # Structured failure suitable for agentic repair.
    INVALID = "invalid"
    FAILED = "failed"


@dataclass(frozen=True)
class StageResult:
    """A stage's report to the engine."""

    outcome: Outcome
    detail: str = ""
    artifacts: tuple[str, ...] = ()


@dataclass(frozen=True)
class StageContext:
    """What a stage is given to do its work."""

    operation: str
    operation_type: OperationType
    plan: PlanVersion
    stage: LifecycleStage


class Stage(Protocol):
    """One lifecycle stage implementation."""

    def __call__(self, context: StageContext) -> StageResult: ...


@dataclass
class RunRecord:
    """What happened during one run of the lifecycle."""

    operation: str
    final_state: OperationState
    transitions: list[StateTransition] = field(default_factory=list)
    stages: list[tuple[LifecycleStage, StageResult]] = field(default_factory=list)

    @property
    def completed(self) -> bool:
        return self.final_state is OperationState.COMPLETED

    def stage_result(self, stage: LifecycleStage) -> StageResult | None:
        return next((result for name, result in self.stages if name is stage), None)


class LifecycleEngine:
    """Runs an Operation through the guarantee boundary.

    Stages are supplied by the operation type. A stage that is not registered
    is treated as a no-op success, so an operation that compiles directly to an
    engine API still passes through GENERATE rather than bypassing the shape.
    """

    def __init__(
        self,
        stages: dict[LifecycleStage, Stage],
        *,
        clock: Callable[[], datetime],
        actor: ActorKind = ActorKind.RUNTIME,
    ) -> None:
        self._stages = stages
        self._clock = clock
        self._actor = actor

    def run(self, plan: PlanVersion) -> RunRecord:
        """Drive one Operation from DRAFT to a terminal state."""
        record = RunRecord(operation=plan.operation, final_state=OperationState.DRAFT)
        state = OperationState.DRAFT

        for stage in _STAGE_ORDER:
            state = self._advance(
                record, plan, state, _STAGE_ENTER[stage], f"{stage.value} started"
            )

            result = self._run_stage(plan, stage)
            record.stages.append((stage, result))

            if result.outcome is Outcome.OK:
                state = self._advance(
                    record, plan, state, _STAGE_DONE[stage], self._reason(stage, result)
                )
                continue

            state = self._advance(
                record,
                plan,
                state,
                self._failure_state(result),
                self._reason(stage, result),
            )
            break

        record.final_state = state
        return record

    def _advance(
        self,
        record: RunRecord,
        plan: PlanVersion,
        current: OperationState,
        target: OperationState | None,
        reason: str,
    ) -> OperationState:
        if target is None or target is current:
            return current
        record.transitions.append(
            transition(
                plan.operation,
                current,
                target,
                actor=self._actor,
                reason=reason,
                occurred_at=self._clock(),
                plan_version=plan.version,
            )
        )
        return target

    def _run_stage(self, plan: PlanVersion, stage: LifecycleStage) -> StageResult:
        implementation = self._stages.get(stage)
        if implementation is None:
            return StageResult(outcome=Outcome.OK, detail="no-op")
        context = StageContext(
            operation=plan.operation,
            operation_type=plan.operation_type,
            plan=plan,
            stage=stage,
        )
        return implementation(context)

    def _failure_state(self, result: StageResult) -> OperationState:
        match result.outcome:
            case Outcome.VERIFICATION_FAILED:
                return OperationState.VERIFICATION_FAILED
            case Outcome.INVALID:
                # Repairable: back to DRAFT for the planner or an agent.
                return OperationState.DRAFT
            case _:
                return OperationState.FAILED

    def _reason(self, stage: LifecycleStage, result: StageResult) -> str:
        detail = f": {result.detail}" if result.detail else ""
        return f"{stage.value} {result.outcome.value}{detail}"


def stage_order() -> Sequence[LifecycleStage]:
    """The stages, in the order the engine runs them."""
    return _STAGE_ORDER
