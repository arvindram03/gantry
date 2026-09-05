# SPDX-License-Identifier: Apache-2.0
"""Verification results.

A verification result is evidence, and evidence has to answer more than
pass/fail. What was checked, over what scope, what each side reported, and how
far apart they were - without those, a failure tells an operator that something
is wrong and nothing about where to look.

The shape is shared by Movement and Analysis. A row-count comparison and a
row-expansion bound are the same kind of statement about the same kind of
scope; only the verifier behind them differs.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ResourceName
from gantry.core.verification import CheckName


class VerificationStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    # The check could not run - a missing column, an unreachable target. Not a
    # pass: an unanswered question is not a satisfied one.
    ERRORED = "errored"
    SKIPPED = "skipped"


class Severity(StrEnum):
    """How much a failure matters.

    Cutover gates count critical failures. A warning records something an
    operator should see without blocking a migration on it.
    """

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class ScopeKind(StrEnum):
    """The verification hierarchy.

    Checks run at the cheapest scope that can answer the question, and drill
    down only where something disagrees.
    """

    OPERATION = "operation"
    DATASET = "dataset"
    PARTITION = "partition"
    CHUNK = "chunk"
    ROW = "row"


class VerificationScope(BaseModel):
    """What a check covered."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ScopeKind
    dataset: str | None = None
    # A partition id, a chunk range, a key - whatever identifies this scope
    # within its dataset.
    identifier: str | None = None

    def __str__(self) -> str:
        parts = [self.kind.value]
        if self.dataset:
            parts.append(self.dataset)
        if self.identifier:
            # A partition id already carries its dataset, so appending both
            # renders "partition/orders/orders/00003". Operator-facing text
            # should not stutter.
            prefix = f"{self.dataset}/" if self.dataset else ""
            parts.append(self.identifier.removeprefix(prefix) if prefix else self.identifier)
        return "/".join(parts)


class VerificationResult(BaseModel):
    """One check's finding."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    check: CheckName
    status: VerificationStatus
    scope: VerificationScope
    severity: Severity = Severity.CRITICAL
    operation: ResourceName
    plan_version: int = Field(ge=1)

    # What each side reported, as text: a count, a checksum, a ratio. Kept
    # unparsed so a result stays readable without knowing which check produced
    # it.
    source_result: str | None = None
    target_result: str | None = None
    difference: str | None = None

    # Anything that helps someone act on this: the query that ran, the bounds,
    # a threshold that was exceeded.
    evidence: dict[str, str] = {}
    observed_at: datetime

    @model_validator(mode="after")
    def _check_result(self) -> VerificationResult:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.status is VerificationStatus.FAILED and not self.difference:
            raise ValueError(f"a failed {self.check.value} check must record how the sides differ")
        return self

    @property
    def passed(self) -> bool:
        return self.status is VerificationStatus.PASSED

    @property
    def blocks_cutover(self) -> bool:
        """Whether this finding is severe enough to stop a cutover."""
        return (
            self.status in (VerificationStatus.FAILED, VerificationStatus.ERRORED)
            and self.severity is Severity.CRITICAL
        )

    def describe(self) -> str:
        detail = f": {self.difference}" if self.difference else ""
        return f"{self.check.value} {self.status.value} at {self.scope}{detail}"
