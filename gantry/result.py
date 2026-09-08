# SPDX-License-Identifier: Apache-2.0
"""Final accepted, rejected, failed, cancelled, or unknown outcomes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from gantry.admission import AdmissionDecision
from gantry.execution import Execution, ExecutionResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.output import OutputRef
from gantry.verifier import VerificationResult


class ResultStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class Result:
    status: ResultStatus
    handle: ExecutionHandle | None = None
    execution: Execution | None = None
    outputs: tuple[OutputRef, ...] = ()
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    verification: VerificationResult | None = None
    failure: Failure | None = None
    admission: AdmissionDecision | None = None

    @property
    def is_accepted(self) -> bool:
        return self.status is ResultStatus.ACCEPTED

    @classmethod
    def rejected(cls, admission: AdmissionDecision) -> Result:
        failure = (
            admission.reasons[0]
            if admission.reasons
            else Failure(
                kind=FailureKind.POLICY_REJECTED,
                retryable=False,
                message="execution was not admitted",
            )
        )
        return cls(status=ResultStatus.REJECTED, failure=failure, admission=admission)

    @classmethod
    def terminal_failure(
        cls,
        status: ResultStatus,
        failure: Failure,
        *,
        handle: ExecutionHandle | None = None,
        execution: Execution | None = None,
        admission: AdmissionDecision | None = None,
    ) -> Result:
        return cls(
            status=status,
            handle=handle,
            execution=execution,
            metrics=execution.metrics if execution is not None else ExecutionMetrics(),
            failure=failure,
            admission=admission,
        )

    @classmethod
    def from_execution(
        cls,
        *,
        execution: Execution,
        engine_result: ExecutionResult,
        verification: VerificationResult,
        admission: AdmissionDecision,
    ) -> Result:
        if not verification.ok:
            message = next(
                (check.message for check in verification.checks if not check.ok and check.message),
                "verification failed",
            )
            return cls(
                status=ResultStatus.VERIFICATION_FAILED,
                handle=execution.handle,
                execution=execution,
                outputs=engine_result.outputs,
                metrics=engine_result.metrics,
                verification=verification,
                failure=Failure(
                    kind=FailureKind.VERIFICATION_FAILED,
                    retryable=False,
                    message=message,
                ),
                admission=admission,
            )
        return cls(
            status=ResultStatus.ACCEPTED,
            handle=execution.handle,
            execution=execution,
            outputs=engine_result.outputs,
            metrics=engine_result.metrics,
            verification=verification,
            admission=admission,
        )
