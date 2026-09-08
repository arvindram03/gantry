# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field

from gantry.failure import Failure
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.output import OutputRef
from gantry.result import ResultStatus
from gantry.sql.output import InlineRows
from gantry.verifier import VerificationResult


@dataclass(frozen=True, slots=True)
class SQLResult:
    status: ResultStatus
    handle: ExecutionHandle | None = None
    inline: InlineRows | None = None
    outputs: tuple[OutputRef, ...] = ()
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    verification: VerificationResult | None = None
    failure: Failure | None = None

    @property
    def ok(self) -> bool:
        return self.status is ResultStatus.ACCEPTED
