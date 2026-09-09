# SPDX-License-Identifier: Apache-2.0
"""Flink readiness, health, and result envelopes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from gantry.execution import Execution
from gantry.failure import Failure
from gantry.flink.metrics import FlinkMetrics
from gantry.handle import ExecutionHandle
from gantry.output import OutputRef
from gantry.result import ResultStatus
from gantry.verifier import VerificationResult


@dataclass(frozen=True, slots=True)
class StreamingHealth:
    healthy: bool
    execution: Execution
    metrics: FlinkMetrics
    verification: VerificationResult = field(default_factory=VerificationResult.passed)

    @property
    def checks(self) -> Mapping[str, bool]:
        return {check.name: check.ok for check in self.verification.checks}


@dataclass(frozen=True, slots=True)
class FlinkResult:
    status: ResultStatus
    handle: ExecutionHandle | None = None
    execution: Execution | None = None
    outputs: tuple[OutputRef, ...] = ()
    metrics: FlinkMetrics = field(default_factory=FlinkMetrics)
    verification: VerificationResult | None = None
    health: StreamingHealth | None = None
    failure: Failure | None = None

    @property
    def is_accepted(self) -> bool:
        return self.status is ResultStatus.ACCEPTED

    @property
    def ok(self) -> bool:
        return self.status is ResultStatus.ACCEPTED

    @property
    def uri(self) -> str | None:
        return None if not self.outputs else self.outputs[0].uri
