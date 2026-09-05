# SPDX-License-Identifier: Apache-2.0
"""The Result an Analysis produces.

The sibling of `MovementResult`, carrying findings instead of row counts and
inheriting the same provenance contract. A Movement Result says what moved; an
Analysis Result says what was concluded and why anyone should believe it.

The design document is specific about one field, and it is the field most
likely to be quietly misused: `strength` must not silently mean model
confidence. A number that sometimes means "measured" and sometimes means "an
LLM felt fairly sure" is worse than no number, because nothing downstream can
tell which it is. So every finding says where its strength came from, and a
subjective one has to say so.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ResourceName
from gantry.core.results import Result, ResultKind


class StrengthBasis(StrEnum):
    """Where a finding's strength came from.

    Required, and deliberately not defaulted: a finding that does not say how
    strongly it is supported, and why, is an assertion wearing a number.
    """

    # Derived from a measurement that either holds or does not.
    DETERMINISTIC = "deterministic"
    # Derived from a statistic - an effect size, a ratio, a test.
    STATISTICAL = "statistical"
    # A model's opinion. Labelled so nothing downstream mistakes it for either
    # of the above.
    MODEL_JUDGEMENT = "model_judgement"


class Measurement(BaseModel):
    """A number supporting a claim, with the units it is in."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    value: float
    unit: str | None = None
    # What the same measurement was before, where there is a before.
    baseline: float | None = None

    @property
    def change(self) -> float | None:
        if self.baseline is None or self.baseline == 0:
            return None
        return (self.value - self.baseline) / self.baseline

    def describe(self) -> str:
        unit = f" {self.unit}" if self.unit else ""
        if self.change is None:
            return f"{self.name} {self.value:,.4g}{unit}"
        return f"{self.name} {self.baseline:,.4g} -> {self.value:,.4g}{unit} ({self.change:+.1%})"


class Finding(BaseModel):
    """One conclusion, with the evidence for it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    claim: str
    strength: float = Field(ge=0.0, le=1.0)
    strength_basis: StrengthBasis
    measurements: tuple[Measurement, ...] = ()
    # Things an SDLC system can act on: a deployment id, a service, a commit.
    # Gantry carries these; connecting them to code is the consuming system's
    # job.
    references: dict[str, str] = {}

    @model_validator(mode="after")
    def _check_finding(self) -> Finding:
        if not self.claim.strip():
            raise ValueError("a finding without a claim is not a finding")
        if self.strength_basis is not StrengthBasis.MODEL_JUDGEMENT and not self.measurements:
            raise ValueError(
                f"a {self.strength_basis.value} finding must carry the measurements "
                f"its strength was derived from"
            )
        return self

    @property
    def is_measured(self) -> bool:
        """Whether the strength rests on evidence rather than on an opinion."""
        return self.strength_basis is not StrengthBasis.MODEL_JUDGEMENT

    def describe(self) -> str:
        basis = "" if self.is_measured else " [model judgement]"
        return f"{self.claim} (strength {self.strength:.2f}{basis})"


class AnalysisResult(Result):
    """What an Analysis concluded."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ResultKind = ResultKind.FINDING

    findings: tuple[Finding, ...] = ()
    # The artifact that produced this, by content hash. Without it a Result
    # cannot show the computation it came from.
    artifact_hash: str | None = None
    engine: str | None = None
    rows_returned: int = Field(default=0, ge=0)
    started_at: datetime
    finished_at: datetime

    @model_validator(mode="after")
    def _check_times(self) -> AnalysisResult:
        for label, value in (("started_at", self.started_at), ("finished_at", self.finished_at)):
            if value.tzinfo is None:
                raise ValueError(f"{label} must be timezone-aware")
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must not precede started_at")
        return self

    @property
    def measured_findings(self) -> tuple[Finding, ...]:
        return tuple(finding for finding in self.findings if finding.is_measured)

    @property
    def strongest(self) -> Finding | None:
        return max(self.findings, key=lambda f: f.strength, default=None)

    def finding(self, identifier: str) -> Finding | None:
        return next((f for f in self.findings if f.id == identifier), None)


def result_name(analysis: ResourceName) -> str:
    return f"{analysis}.analysis"
