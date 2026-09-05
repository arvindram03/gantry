# SPDX-License-Identifier: Apache-2.0
"""Deriving findings from what an Analysis computed.

Findings are produced by comparing measured groups, not by asking a model what
it thinks. That is why every finding here carries `StrengthBasis.STATISTICAL`
and the measurements the strength came from: the number means something
checkable.

A model-authored finding is a legitimate thing for a planner to produce later,
and the model has room for it - but it must arrive labelled `model_judgement`,
so nothing downstream mistakes an opinion for a measurement.
"""

from __future__ import annotations

from gantry.adapters.engine.base import QueryResult, as_float
from gantry.analysis.model import Analysis
from gantry.analysis.result import Finding, Measurement, StrengthBasis

# Below this relative change, a difference between groups is not worth
# reporting as a finding. Chosen to be obviously arbitrary rather than
# quietly so: it is a reporting threshold, not a significance test.
DEFAULT_MIN_CHANGE = 0.25

# Signals where a rise is the interesting direction.
_HIGHER_IS_WORSE = {
    "p95_latency",
    "p50_latency",
    "error_rate",
    "timeout_count",
    "database_calls_per_request",
    "database_wait_time",
}


def derive_findings(
    analysis: Analysis,
    result: QueryResult,
    *,
    min_change: float = DEFAULT_MIN_CHANGE,
) -> tuple[Finding, ...]:
    """Compare the measured groups and report what changed.

    Deliberately simple: two groups are compared on each signal, and a change
    beyond the threshold becomes a finding. Anything more sophisticated is a
    statistical claim the runtime has not earned the right to make.
    """
    rows = result.as_dicts()
    if len(rows) < 2:
        return ()

    grouping = _grouping_column(analysis, result)
    if grouping is None:
        return ()

    baseline, current = rows[0], rows[-1]
    findings: list[Finding] = []

    for signal in analysis.signals:
        if signal not in result.columns or signal == "row_count":
            continue
        before = as_float(baseline.get(signal))
        after = as_float(current.get(signal))
        if before == 0:
            continue

        change = (after - before) / before
        if abs(change) < min_change:
            continue
        if change < 0 and signal in _HIGHER_IS_WORSE:
            # A fall in a bad-when-high signal is not a regression; reporting
            # it as one would fill a Result with noise.
            continue

        findings.append(
            Finding(
                id=f"finding-{len(findings) + 1}",
                claim=_claim(signal, grouping, baseline, current, change),
                # Strength scales with the size of the change and is capped
                # well below certainty: this compares two groups, which is
                # evidence of a difference, not proof of a cause.
                strength=min(0.95, 0.5 + min(abs(change), 2.0) / 4),
                strength_basis=StrengthBasis.STATISTICAL,
                measurements=(Measurement(name=signal, value=after, baseline=before),),
                references={
                    grouping: str(current.get(grouping, "")),
                    f"{grouping}_baseline": str(baseline.get(grouping, "")),
                    **_extra_references(analysis, current),
                },
            )
        )

    return tuple(findings)


def _grouping_column(analysis: Analysis, result: QueryResult) -> str | None:
    """The column whose change a finding is about.

    The last normalised field that varies between groups - normalising a field
    is the spec saying it identifies something, and the one that differs is
    what a finding should name.
    """
    candidates = [item.canonical for item in analysis.normalize]
    rows = result.as_dicts()
    varying = [
        column
        for column in candidates
        if column in result.columns and len({str(row.get(column)) for row in rows}) > 1
    ]
    return varying[-1] if varying else (candidates[-1] if candidates else None)


def _extra_references(analysis: Analysis, row: dict[str, object]) -> dict[str, str]:
    """Whatever else identifies this group, for a consuming system to act on."""
    return {
        item.canonical: str(row[item.canonical])
        for item in analysis.normalize
        if item.canonical in row and row[item.canonical] is not None
    }


def _claim(
    signal: str,
    grouping: str,
    baseline: dict[str, object],
    current: dict[str, object],
    change: float,
) -> str:
    direction = "increased" if change > 0 else "decreased"
    readable = signal.replace("_", " ")
    return (
        f"{readable} {direction} {abs(change):.0%} "
        f"for {grouping} {current.get(grouping)!r} "
        f"compared with {baseline.get(grouping)!r}"
    )
