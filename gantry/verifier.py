# SPDX-License-Identifier: Apache-2.0
"""Portable verification contracts and structured check results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from gantry.artifact import Artifact
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    ok: bool
    expected: object | None = None
    actual: object | None = None
    message: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class VerificationResult:
    ok: bool
    checks: tuple[CheckResult, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def passed(cls, *checks: CheckResult) -> VerificationResult:
        return cls(ok=True, checks=checks)

    @classmethod
    def failed(
        cls,
        message: str,
        *,
        name: str = "verification",
        expected: object | None = None,
        actual: object | None = None,
    ) -> VerificationResult:
        return cls(
            ok=False,
            checks=(
                CheckResult(
                    name=name,
                    ok=False,
                    expected=expected,
                    actual=actual,
                    message=message,
                ),
            ),
        )


class Verifier(Protocol):
    async def verify(
        self,
        *,
        artifact: Artifact,
        context: Context,
        execution: Execution,
        result: ExecutionResult,
    ) -> VerificationResult: ...
