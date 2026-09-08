# SPDX-License-Identifier: Apache-2.0
"""Normalized validation, live execution, and engine-result models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from gantry.failure import Failure
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.output import OutputRef


@dataclass(frozen=True, slots=True)
class ValidationResult:
    ok: bool
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def accepted(
        cls,
        *,
        warnings: tuple[str, ...] = (),
        metadata: Mapping[str, object] | None = None,
    ) -> ValidationResult:
        return cls(ok=True, warnings=warnings, metadata={} if metadata is None else metadata)

    @classmethod
    def rejected(cls, *errors: str) -> ValidationResult:
        return cls(ok=False, errors=errors)


class ExecutionState(StrEnum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class Execution:
    """Gantry's normalized live view of an engine job."""

    handle: ExecutionHandle
    state: ExecutionState
    started_at: datetime | None = None
    updated_at: datetime | None = None
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    failure: Failure | None = None
    native: Mapping[str, object] = field(default_factory=dict)

    @property
    def terminal(self) -> bool:
        return self.state in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
            ExecutionState.UNKNOWN,
        }


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """An engine completion envelope; success still requires verification."""

    ok: bool
    handle: ExecutionHandle
    outputs: tuple[OutputRef, ...] = ()
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    failure: Failure | None = None
    native: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def succeeded(
        cls,
        handle: ExecutionHandle,
        *,
        outputs: tuple[OutputRef, ...] = (),
        metrics: ExecutionMetrics | None = None,
        native: Mapping[str, object] | None = None,
    ) -> ExecutionResult:
        return cls(
            ok=True,
            handle=handle,
            outputs=outputs,
            metrics=ExecutionMetrics() if metrics is None else metrics,
            native={} if native is None else native,
        )

    @classmethod
    def failed(cls, handle: ExecutionHandle, failure: Failure) -> ExecutionResult:
        return cls(ok=False, handle=handle, failure=failure)
