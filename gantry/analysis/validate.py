# SPDX-License-Identifier: Apache-2.0
"""Validating a generated artifact before it runs.

The design document is explicit that generation succeeding is not permission to
execute. Validation is the gate, and its output has a specific job: when it
refuses, it must say what to change.

That shapes the whole module. Nothing here raises on a validation failure -
failures are values, structured well enough that a planner or an agent can act
on them. A stack trace tells a human something went wrong and tells an agent
nothing at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from gantry.adapters.engine.base import EngineAdapter, ExplainResult
from gantry.analysis.artifact import GeneratedArtifact
from gantry.analysis.model import Analysis
from gantry.core.dataset import DatasetManifest

# A sample large enough to be evidence, small enough that running it is not
# the execution it is meant to gate.
DEFAULT_SAMPLE_ROWS = 10


class ValidationCheck(StrEnum):
    """What was checked, so a failure says which gate refused."""

    INPUTS_EXIST = "inputs_exist"
    SIGNALS_RESOLVE = "signals_resolve"
    SYNTAX = "syntax"
    PLAN = "plan"
    COST = "cost"
    SAMPLE = "sample"


class Repair(StrEnum):
    """What kind of change would fix this.

    Coarse on purpose: an agent needs to know whether to edit the spec,
    rediscover, or raise a limit - not to be handed a suggested diff it would
    apply without understanding.
    """

    EDIT_SPEC = "edit_spec"
    REDISCOVER = "rediscover"
    RAISE_LIMIT = "raise_limit"
    NONE = "none"


@dataclass(frozen=True)
class ValidationFailure:
    """One reason an artifact was refused."""

    check: ValidationCheck
    problem: str
    repair: Repair
    detail: str | None = None

    def describe(self) -> str:
        suffix = f" — {self.detail}" if self.detail else ""
        return f"{self.check.value}: {self.problem}{suffix}"


@dataclass
class ValidationReport:
    """Whether an artifact may run, and if not, what to change."""

    artifact: GeneratedArtifact
    engine: str
    failures: list[ValidationFailure] = field(default_factory=list)
    explain: ExplainResult | None = None
    sample_rows: int | None = None

    @property
    def accepted(self) -> bool:
        return not self.failures

    @property
    def repairs(self) -> tuple[Repair, ...]:
        return tuple(sorted({failure.repair for failure in self.failures}))

    def describe(self) -> str:
        if self.accepted:
            estimate = self.explain.describe() if self.explain else "no estimate"
            return f"accepted on {self.engine} ({estimate})"
        return "; ".join(failure.describe() for failure in self.failures)


@dataclass(frozen=True)
class ValidationPolicy:
    """The limits validation enforces.

    Estimates, not measurements: the point is to refuse before running, and an
    estimate is the only thing available then.
    """

    max_estimated_rows: int | None = None
    max_estimated_cost: float | None = None
    require_sample: bool = True
    sample_rows: int = DEFAULT_SAMPLE_ROWS


async def validate(
    analysis: Analysis,
    artifact: GeneratedArtifact,
    adapter: EngineAdapter,
    manifests: dict[str, DatasetManifest],
    *,
    policy: ValidationPolicy | None = None,
) -> ValidationReport:
    """Decide whether an artifact may run.

    Checks run cheapest first and stop at the first one that makes later checks
    meaningless: there is no point sampling something the engine cannot plan.
    """
    rules = policy or ValidationPolicy()
    report = ValidationReport(artifact=artifact, engine=adapter.engine)

    _check_inputs(analysis, manifests, report)
    if report.failures:
        return report

    explained = await _check_plan(artifact, adapter, report)
    if explained is None:
        return report

    _check_cost(explained, rules, report)
    if report.failures:
        return report

    if rules.require_sample:
        await _check_sample(artifact, adapter, rules, report)

    return report


def _check_inputs(
    analysis: Analysis, manifests: dict[str, DatasetManifest], report: ValidationReport
) -> None:
    missing = [name for name in analysis.inputs if name not in manifests]
    if missing:
        report.failures.append(
            ValidationFailure(
                check=ValidationCheck.INPUTS_EXIST,
                problem=f"inputs are not registered: {', '.join(missing)}",
                repair=Repair.REDISCOVER,
                detail="discover the source, or correct the dataset names in the spec",
            )
        )


async def _check_plan(
    artifact: GeneratedArtifact, adapter: EngineAdapter, report: ValidationReport
) -> ExplainResult | None:
    """Ask the engine to plan the artifact.

    This is where a reference to a column that does not exist is caught - by
    the engine, which is the only thing that actually knows.
    """
    try:
        explained = await adapter.explain(artifact)
    except Exception as error:
        report.failures.append(
            ValidationFailure(
                check=ValidationCheck.SYNTAX,
                problem="the engine could not plan this artifact",
                repair=Repair.EDIT_SPEC,
                detail=_engine_message(error),
            )
        )
        return None

    report.explain = explained
    return explained


def _check_cost(
    explained: ExplainResult, policy: ValidationPolicy, report: ValidationReport
) -> None:
    over_row_limit = (
        policy.max_estimated_rows is not None
        and explained.estimated_rows is not None
        and explained.estimated_rows > policy.max_estimated_rows
    )
    if over_row_limit:
        report.failures.append(
            ValidationFailure(
                check=ValidationCheck.COST,
                problem=(
                    f"estimated {explained.estimated_rows:,} rows, "
                    f"over the limit of {policy.max_estimated_rows:,}"
                ),
                repair=Repair.RAISE_LIMIT,
                detail="narrow the window, or raise the limit deliberately",
            )
        )

    over_cost_limit = (
        policy.max_estimated_cost is not None
        and explained.estimated_cost is not None
        and explained.estimated_cost > policy.max_estimated_cost
    )
    if over_cost_limit:
        report.failures.append(
            ValidationFailure(
                check=ValidationCheck.COST,
                problem=(
                    f"estimated cost {explained.estimated_cost:,.0f}, "
                    f"over the limit of {policy.max_estimated_cost:,.0f}"
                ),
                repair=Repair.RAISE_LIMIT,
            )
        )


async def _check_sample(
    artifact: GeneratedArtifact,
    adapter: EngineAdapter,
    policy: ValidationPolicy,
    report: ValidationReport,
) -> None:
    """Run a little of it.

    Planning proves an artifact is well formed. Running a bounded slice proves
    the engine can produce rows from it, which is a different claim and the one
    that catches errors only execution reveals.
    """
    try:
        sample = await adapter.sample(artifact, limit=policy.sample_rows)
    except Exception as error:
        report.failures.append(
            ValidationFailure(
                check=ValidationCheck.SAMPLE,
                problem="the artifact planned but failed to run",
                repair=Repair.EDIT_SPEC,
                detail=_engine_message(error),
            )
        )
        return

    report.sample_rows = sample.row_count


def _engine_message(error: BaseException) -> str:
    """The engine's own words, trimmed to the part that identifies the problem.

    Drivers wrap their errors several layers deep and append the whole query;
    an agent reading this needs the first line, not the stack.
    """
    text = str(error).strip()
    for line in text.splitlines():
        cleaned = line.strip()
        if cleaned and not cleaned.startswith("["):
            return cleaned[:300]
    return text[:300]
