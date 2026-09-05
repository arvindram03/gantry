# SPDX-License-Identifier: Apache-2.0
"""The Result resource.

Spec: a Result is a bounded, structured output of a Movement or an Analysis,
carrying provenance. `MovementResult` and `AnalysisResult` specialise this base;
both land later in the plan, and both inherit the provenance contract from here.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator

from gantry.core.evidence import VerificationResult
from gantry.core.names import ResourceName
from gantry.core.provenance import Provenance


class ResultKind(StrEnum):
    """Semantic forms a Result may take (RFC: AnalysisResult specialisations).

    `EVIDENCE` is one specialisation among several, not a top-level resource.
    """

    MOVEMENT = "movement"
    AGGREGATE = "aggregate"
    PROFILE = "profile"
    DIFF = "diff"
    DATASET = "dataset"
    FINDING = "finding"
    EVIDENCE = "evidence"


class ResultStatus(StrEnum):
    """Whether the Result may be trusted.

    `VERIFICATION_FAILED` is distinct from `FAILED` on purpose: the engine can
    report success while Gantry rejects the output.
    """

    OK = "ok"
    VERIFICATION_FAILED = "verification_failed"
    FAILED = "failed"


class Result(BaseModel):
    """Base Result. Provenance is required - a Result without it is not one."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    kind: ResultKind
    status: ResultStatus
    provenance: Provenance
    created_at: datetime
    # Verification findings are carried on the Result rather than looked up
    # beside it: a Result that cannot show why it is trustworthy is not
    # evidence of anything. Shared by both operation types, because a row-count
    # comparison and a row-expansion bound are the same kind of statement.
    verification: tuple[VerificationResult, ...] = ()

    @model_validator(mode="after")
    def _require_tz(self) -> Result:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        return self

    @property
    def is_trustworthy(self) -> bool:
        return self.status is ResultStatus.OK

    @property
    def blocking_failures(self) -> tuple[VerificationResult, ...]:
        """Findings severe enough to stop a cutover."""
        return tuple(finding for finding in self.verification if finding.blocks_cutover)
