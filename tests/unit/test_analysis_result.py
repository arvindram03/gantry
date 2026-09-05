"""The rules an Analysis Result enforces on itself.

The design document's sharpest requirement about findings is not about
structure, it is about honesty: a strength number must say where it came from.
Most of this file is about the ways a Result can claim more than it knows.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from gantry.analysis.result import (
    AnalysisResult,
    Finding,
    Measurement,
    StrengthBasis,
    result_name,
)
from gantry.core.names import ResourceName
from gantry.core.provenance import Provenance
from gantry.core.results import ResultKind, ResultStatus
from pydantic import ValidationError

NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def measurement(**overrides: object) -> Measurement:
    return Measurement(**{"name": "p95_latency", "value": 532.0, "unit": "ms", **overrides})


def finding(**overrides: object) -> Finding:
    return Finding(
        **{
            "id": "finding-1",
            "claim": "p95 latency increased after the deploy",
            "strength": 0.95,
            "strength_basis": StrengthBasis.STATISTICAL,
            "measurements": (measurement(baseline=58.98),),
            **overrides,
        }
    )


def analysis_result(**overrides: object) -> AnalysisResult:
    return AnalysisResult(
        **{
            "name": "checkout-regression.analysis",
            "status": ResultStatus.OK,
            "provenance": Provenance(generated_at=NOW, operation="checkout-regression"),
            "created_at": NOW,
            "started_at": NOW,
            "finished_at": NOW + timedelta(seconds=3),
            **overrides,
        }
    )


class TestMeasurement:
    def test_change_is_relative_to_the_baseline(self) -> None:
        assert measurement(value=120.0, baseline=100.0).change == pytest.approx(0.2)

    def test_a_measurement_without_a_baseline_reports_no_change(self) -> None:
        """Not zero change - unknown change. Zero would be a claim."""
        assert measurement().change is None

    def test_a_zero_baseline_reports_no_change_rather_than_infinity(self) -> None:
        assert measurement(baseline=0.0).change is None

    def test_describe_carries_the_unit(self) -> None:
        assert "ms" in measurement(baseline=58.98).describe()


class TestFinding:
    def test_a_measured_finding_must_carry_its_measurements(self) -> None:
        """A statistical strength with nothing behind it is a number someone
        made up, and it is indistinguishable from one that is not."""
        with pytest.raises(ValidationError, match="measurements"):
            finding(measurements=())

    def test_a_deterministic_finding_must_carry_its_measurements_too(self) -> None:
        with pytest.raises(ValidationError, match="measurements"):
            finding(strength_basis=StrengthBasis.DETERMINISTIC, measurements=())

    def test_a_model_judgement_may_stand_alone_but_says_so(self) -> None:
        judged = finding(strength_basis=StrengthBasis.MODEL_JUDGEMENT, measurements=())
        assert not judged.is_measured
        assert "model judgement" in judged.describe()

    def test_an_empty_claim_is_not_a_finding(self) -> None:
        with pytest.raises(ValidationError, match="without a claim"):
            finding(claim="   ")

    @pytest.mark.parametrize("strength", [-0.1, 1.1])
    def test_strength_stays_a_probability(self, strength: float) -> None:
        with pytest.raises(ValidationError):
            finding(strength=strength)

    def test_a_finding_is_frozen(self) -> None:
        with pytest.raises(ValidationError):
            finding().strength = 0.1  # type: ignore[misc]

    def test_unknown_fields_are_rejected(self) -> None:
        """Silently dropping a field would lose evidence without saying so."""
        with pytest.raises(ValidationError):
            finding(confidence=0.9)


class TestAnalysisResult:
    def test_it_defaults_to_the_finding_kind(self) -> None:
        assert analysis_result().kind is ResultKind.FINDING

    def test_times_must_be_timezone_aware(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            analysis_result(started_at=datetime(2026, 9, 4, 12, 0))

    def test_a_run_cannot_finish_before_it_started(self) -> None:
        with pytest.raises(ValidationError, match="must not precede"):
            analysis_result(finished_at=NOW - timedelta(seconds=1))

    def test_measured_findings_exclude_model_judgement(self) -> None:
        judged = finding(
            id="finding-2", strength_basis=StrengthBasis.MODEL_JUDGEMENT, measurements=()
        )
        result = analysis_result(findings=(finding(), judged))
        assert [f.id for f in result.measured_findings] == ["finding-1"]

    def test_strongest_picks_the_highest_strength(self) -> None:
        weak = finding(id="finding-2", strength=0.2)
        result = analysis_result(findings=(weak, finding()))
        assert result.strongest is not None
        assert result.strongest.id == "finding-1"

    def test_strongest_of_nothing_is_nothing(self) -> None:
        assert analysis_result().strongest is None

    def test_a_finding_can_be_addressed_by_id(self) -> None:
        result = analysis_result(findings=(finding(),))
        assert result.finding("finding-1") is not None
        assert result.finding("finding-404") is None


def test_result_name_is_derived_from_the_analysis() -> None:
    """`.analysis`, not `/analysis`: a ResourceName has no path separator, and
    the previous spelling produced a name the store could not round-trip."""
    name = result_name(ResourceName("checkout-regression"))
    assert name == "checkout-regression.analysis"
    ResourceName(name)
