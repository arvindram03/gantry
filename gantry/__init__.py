# SPDX-License-Identifier: Apache-2.0
"""Gantry: the execution control plane for agent-generated data work."""

from gantry import actor, batch, nosql, runs, sql, stream, verify
from gantry.actor import ActorRef, ActorType
from gantry.adapter import ExecutionAdapter
from gantry.admission import AdmissionDecision, admit
from gantry.artifact import Artifact
from gantry.capabilities import AdapterCapabilities
from gantry.context import Context
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.output import OutputKind, OutputRef
from gantry.policy import PolicyRequirements
from gantry.result import Result, ResultStatus
from gantry.runs.model import Run
from gantry.runs.status import RunStatus
from gantry.runtime import (
    ControlPlane,
    SubmissionError,
    cancel,
    configure,
    get,
    register_adapter,
    run,
    submit,
    wait,
)
from gantry.store import ExecutionStore, MemoryExecutionStore, RunRecord
from gantry.target import ExecutionTarget
from gantry.tool import Tool
from gantry.verifier import (
    CheckResult,
    CheckSource,
    VerificationResult,
    VerificationSource,
    Verifier,
)

__version__ = "0.5.0"

__all__ = [
    "ActorRef",
    "ActorType",
    "AdapterCapabilities",
    "AdmissionDecision",
    "Artifact",
    "CheckResult",
    "CheckSource",
    "Context",
    "ControlPlane",
    "EvidenceBundle",
    "Execution",
    "ExecutionAdapter",
    "ExecutionHandle",
    "ExecutionMetrics",
    "ExecutionResult",
    "ExecutionState",
    "ExecutionStore",
    "ExecutionTarget",
    "Failure",
    "FailureKind",
    "MemoryExecutionStore",
    "Observation",
    "ObservationSource",
    "OutputKind",
    "OutputRef",
    "PolicyRequirements",
    "Result",
    "ResultStatus",
    "Run",
    "RunRecord",
    "RunStatus",
    "SubmissionError",
    "Tool",
    "ValidationResult",
    "VerificationResult",
    "VerificationSource",
    "Verifier",
    "__version__",
    "actor",
    "admit",
    "batch",
    "cancel",
    "configure",
    "get",
    "nosql",
    "register_adapter",
    "run",
    "runs",
    "sql",
    "stream",
    "submit",
    "verify",
    "wait",
]
