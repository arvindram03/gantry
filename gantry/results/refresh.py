# SPDX-License-Identifier: Apache-2.0
"""Re-running the computation a Result came from.

A Result is a claim about data as it was. Refresh asks the narrow, answerable
version of "is it still true": re-execute **the artifact the Result was
produced from** - the same content hash, not a recompilation - and report how
the measurements behind each finding have moved.

What refresh deliberately does not do is write a new Result. A Result needs
findings derived under the spec's own rules and a verification pass that says
they can be trusted, and neither survives in the stored artifact. Emitting a
new Result from a re-execution alone would produce something that looks
verified and is not.
"""

from __future__ import annotations

from dataclasses import dataclass

from gantry.adapters.engine.base import EngineAdapter, as_float
from gantry.analysis.artifact import GeneratedArtifact
from gantry.analysis.result import AnalysisResult, Finding
from gantry.core.results import Result
from gantry.results.store import ResultStore
from gantry.state.artifacts import ArtifactStore

# Below this relative move a measurement is treated as unchanged. Floating
# point and row-level churn both produce noise at this scale.
DEFAULT_TOLERANCE = 0.01


@dataclass(frozen=True)
class MeasurementDrift:
    """One measurement, then and now."""

    finding: str
    measurement: str
    previous: float
    current: float | None

    @property
    def resolved(self) -> bool:
        return self.current is not None

    @property
    def change(self) -> float | None:
        if self.current is None or self.previous == 0:
            return None
        return (self.current - self.previous) / self.previous

    def describe(self) -> str:
        if self.current is None:
            return f"{self.finding}  {self.measurement}  group no longer present"
        change = self.change
        moved = "" if change is None else f" ({change:+.1%})"
        return (
            f"{self.finding}  {self.measurement}  "
            f"{self.previous:,.4g} -> {self.current:,.4g}{moved}"
        )


@dataclass
class RefreshReport:
    """What re-running the artifact showed."""

    result: Result
    artifact: GeneratedArtifact
    rows_returned: int
    drifts: tuple[MeasurementDrift, ...] = ()
    tolerance: float = DEFAULT_TOLERANCE

    @property
    def moved(self) -> tuple[MeasurementDrift, ...]:
        return tuple(
            drift
            for drift in self.drifts
            if not drift.resolved or abs(drift.change or 0.0) > self.tolerance
        )

    @property
    def holds(self) -> bool:
        """Whether every measurement is still where the Result left it."""
        return not self.moved

    def describe(self) -> str:
        if not self.drifts:
            return f"{self.rows_returned:,} rows, nothing measurable to compare"
        return (
            f"{self.rows_returned:,} rows, "
            f"{len(self.moved)} of {len(self.drifts)} measurements moved"
        )


class MissingArtifactError(Exception):
    """The artifact a Result came from was not retained.

    Refresh cannot fall back to recompiling: a recompilation is a different
    computation unless proven otherwise, and comparing against it would answer
    a question nobody asked.
    """

    def __init__(self, content_hash: str | None) -> None:
        super().__init__(
            f"artifact {content_hash or '(unrecorded)'} is not in the store; "
            "the computation this Result came from cannot be re-run"
        )


async def refresh(
    name: str,
    *,
    results: ResultStore,
    artifacts: ArtifactStore,
    adapter: EngineAdapter,
    tolerance: float = DEFAULT_TOLERANCE,
) -> RefreshReport | None:
    """Re-execute a Result's artifact and report measurement drift."""
    result = await results.get(name)
    if result is None:
        return None
    if not isinstance(result, AnalysisResult):
        raise TypeError(f"{name} is a {result.kind.value} result; refresh re-runs an Analysis")

    stored = None if result.artifact_hash is None else await artifacts.get(result.artifact_hash)
    if stored is None:
        raise MissingArtifactError(result.artifact_hash)

    output = await adapter.execute(stored)
    rows = output.as_dicts()

    return RefreshReport(
        result=result,
        artifact=stored,
        rows_returned=output.row_count,
        drifts=tuple(drift for finding in result.findings for drift in _drifts(finding, rows)),
        tolerance=tolerance,
    )


def _drifts(finding: Finding, rows: list[dict[str, object]]) -> tuple[MeasurementDrift, ...]:
    grouping = _grouping(finding)
    if grouping is None:
        return ()

    current = _row_for(rows, grouping, finding.references.get(grouping))
    if current is None:
        return tuple(
            MeasurementDrift(finding.id, m.name, m.value, None) for m in finding.measurements
        )

    return tuple(
        MeasurementDrift(
            finding=finding.id,
            measurement=measurement.name,
            previous=measurement.value,
            current=(
                None if measurement.name not in current else as_float(current[measurement.name])
            ),
        )
        for measurement in finding.measurements
    )


def _grouping(finding: Finding) -> str | None:
    """The reference the finding is about.

    Findings record both the group and its baseline, so the grouping column is
    the reference that has a `_baseline` twin. Reconstructing it beats storing
    it twice and letting the two disagree.
    """
    return next(
        (key for key in finding.references if f"{key}_baseline" in finding.references),
        None,
    )


def _row_for(
    rows: list[dict[str, object]], column: str, value: str | None
) -> dict[str, object] | None:
    if value is None:
        return None
    return next((row for row in rows if str(row.get(column)) == value), None)
