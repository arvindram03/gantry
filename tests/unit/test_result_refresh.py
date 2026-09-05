"""Re-running a Result's computation and comparing what came back.

Refresh answers "does this still hold" the only way it honestly can: by
re-executing the exact artifact the Result came from and comparing the numbers
behind each finding. These tests pin the ways that comparison can go wrong.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import pytest
from gantry.adapters.engine.base import ExplainResult, QueryResult
from gantry.analysis.artifact import GeneratedArtifact
from gantry.analysis.result import AnalysisResult, Finding, Measurement, StrengthBasis
from gantry.core.provenance import Provenance
from gantry.core.results import Result, ResultKind, ResultStatus
from gantry.movement.result import MovementResult
from gantry.results.refresh import MissingArtifactError, RefreshReport, refresh
from gantry.results.store import ResultStore

NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)

COLUMNS = ("commit_sha", "p95_latency", "error_rate")
BEFORE = ("3a1f00", 58.98, 0.005)
AFTER = ("8f3142", 532.0, 0.025)


class MemoryResultStore:
    def __init__(self, *results: Result) -> None:
        self._by_name = {result.name: result for result in results}

    async def put(self, result: Result) -> None:
        self._by_name[result.name] = result

    async def get(self, name: str) -> Result | None:
        return self._by_name.get(name)

    async def for_operation(self, operation: str) -> Sequence[Result]:
        return tuple(r for r in self._by_name.values() if r.provenance.operation == operation)

    async def for_operation_outputs(self, datasets: Sequence[str]) -> Sequence[Result]:
        return ()


class MemoryArtifactStore:
    def __init__(self, *artifacts: GeneratedArtifact) -> None:
        self._by_hash = {a.content_hash: a for a in artifacts}

    async def put(self, artifact: GeneratedArtifact) -> str:
        self._by_hash[artifact.content_hash] = artifact
        return artifact.content_hash

    async def get(self, content_hash: str) -> GeneratedArtifact | None:
        return self._by_hash.get(content_hash)

    async def for_analysis(self, analysis: str) -> Sequence[GeneratedArtifact]:
        return tuple(self._by_hash.values())


class ReplayingAdapter:
    """Returns prepared rows, and records what it was asked to run."""

    def __init__(self, result: QueryResult) -> None:
        self._result = result
        self.executed: list[str] = []

    @property
    def engine(self) -> str:
        return "fake"

    async def explain(self, artifact: GeneratedArtifact) -> ExplainResult:
        return ExplainResult(plan="")

    async def sample(self, artifact: GeneratedArtifact, *, limit: int) -> QueryResult:
        return self._result

    async def execute(self, artifact: GeneratedArtifact) -> QueryResult:
        self.executed.append(artifact.content_hash)
        return self._result

    async def close(self) -> None:
        return None


def artifact(body: str = "SELECT 1") -> GeneratedArtifact:
    return GeneratedArtifact(
        analysis="checkout-regression",
        engine="postgres",
        body=body,
        inputs=("request_logs",),
        generated_at=NOW,
    )


def finding(**overrides: object) -> Finding:
    return Finding(
        **{
            "id": "finding-1",
            "claim": "p95 latency increased after the deploy",
            "strength": 0.95,
            "strength_basis": StrengthBasis.STATISTICAL,
            "measurements": (Measurement(name="p95_latency", value=532.0, baseline=58.98),),
            "references": {"commit_sha": "8f3142", "commit_sha_baseline": "3a1f00"},
            **overrides,
        }
    )


def analysis_result(compiled: GeneratedArtifact, **overrides: object) -> AnalysisResult:
    return AnalysisResult(
        **{
            "name": "checkout-regression.analysis",
            "status": ResultStatus.OK,
            "provenance": Provenance(generated_at=NOW, operation="checkout-regression"),
            "created_at": NOW,
            "started_at": NOW,
            "finished_at": NOW,
            "artifact_hash": compiled.content_hash,
            "engine": "postgres",
            "findings": (finding(),),
            **overrides,
        }
    )


def rows(*values: tuple[object, ...]) -> QueryResult:
    return QueryResult(columns=COLUMNS, rows=values)


async def run(
    output: QueryResult,
    *,
    stored: AnalysisResult | None = None,
    compiled: GeneratedArtifact | None = None,
    retained: bool = True,
) -> tuple[RefreshReport | None, ReplayingAdapter]:
    compiled = compiled or artifact()
    stored = stored or analysis_result(compiled)
    artifacts = MemoryArtifactStore(*(compiled,) if retained else ())
    adapter = ReplayingAdapter(output)
    report = await refresh(
        stored.name,
        results=MemoryResultStore(stored),
        artifacts=artifacts,
        adapter=adapter,
    )
    return report, adapter


async def test_an_unchanged_measurement_reports_the_finding_still_holds() -> None:
    report, _ = await run(rows(BEFORE, AFTER))

    assert report is not None
    assert report.holds
    assert [d.measurement for d in report.drifts] == ["p95_latency"]
    assert report.drifts[0].current == pytest.approx(532.0)


async def test_a_moved_measurement_is_reported_with_its_change() -> None:
    report, _ = await run(rows(BEFORE, ("8f3142", 66.0, 0.005)))

    assert report is not None
    assert not report.holds
    drift = report.moved[0]
    assert drift.change == pytest.approx((66.0 - 532.0) / 532.0)
    assert "532 -> 66" in drift.describe()


async def test_a_move_inside_the_tolerance_is_not_a_move() -> None:
    """Row-level churn and float noise must not read as the finding changing."""
    report, _ = await run(rows(BEFORE, ("8f3142", 532.4, 0.025)))

    assert report is not None
    assert report.holds
    assert report.drifts[0].current == pytest.approx(532.4)


async def test_a_vanished_group_is_reported_rather_than_treated_as_zero() -> None:
    """Zero would be a measurement. The group being gone is a different fact."""
    report, _ = await run(rows(BEFORE))

    assert report is not None
    assert not report.holds
    drift = report.drifts[0]
    assert not drift.resolved
    assert drift.change is None
    assert "no longer present" in drift.describe()


async def test_refresh_re_runs_the_stored_artifact_not_a_recompilation() -> None:
    """The Result came from one exact computation; comparing against a
    recompiled one would answer a question nobody asked."""
    compiled = artifact()
    _, adapter = await run(rows(BEFORE, AFTER), compiled=compiled)
    assert adapter.executed == [compiled.content_hash]


async def test_an_unretained_artifact_refuses_rather_than_recompiling() -> None:
    with pytest.raises(MissingArtifactError, match="cannot be re-run"):
        await run(rows(BEFORE, AFTER), retained=False)


async def test_a_result_with_no_artifact_hash_refuses() -> None:
    compiled = artifact()
    with pytest.raises(MissingArtifactError, match="unrecorded"):
        await run(
            rows(BEFORE, AFTER),
            stored=analysis_result(compiled, artifact_hash=None),
            compiled=compiled,
        )


async def test_a_movement_result_cannot_be_refreshed() -> None:
    """Refresh re-runs a computation. A Movement is not one."""
    store = MemoryResultStore(
        MovementResult(
            name="orders.movement",
            status=ResultStatus.OK,
            created_at=NOW,
            started_at=NOW,
            finished_at=NOW,
            provenance=Provenance(generated_at=NOW, operation="orders"),
        )
    )
    with pytest.raises(TypeError, match="movement result"):
        await refresh(
            "orders.movement",
            results=store,
            artifacts=MemoryArtifactStore(),
            adapter=ReplayingAdapter(rows()),
        )


async def test_an_unknown_result_refreshes_to_nothing() -> None:
    report = await refresh(
        "nope.analysis",
        results=MemoryResultStore(),
        artifacts=MemoryArtifactStore(),
        adapter=ReplayingAdapter(rows()),
    )
    assert report is None


async def test_a_finding_without_a_baseline_reference_has_nothing_to_compare() -> None:
    """No grouping means no row to find; saying so beats guessing at one."""
    compiled = artifact()
    stored = analysis_result(compiled, findings=(finding(references={"service": "checkout"}),))
    report, _ = await run(rows(BEFORE, AFTER), stored=stored, compiled=compiled)

    assert report is not None
    assert report.drifts == ()
    assert report.holds
    assert "nothing measurable" in report.describe()


def test_the_protocol_is_satisfied_by_the_stores_under_test() -> None:
    """If the fakes drift from the Protocol these tests stop meaning anything."""
    store: ResultStore = MemoryResultStore()
    assert store is not None
    assert ResultKind.FINDING in tuple(ResultKind)
