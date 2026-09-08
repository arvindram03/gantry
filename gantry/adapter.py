# SPDX-License-Identifier: Apache-2.0
"""The provider-neutral execution adapter contract."""

from __future__ import annotations

from typing import Protocol

from gantry.artifact import Artifact
from gantry.capabilities import AdapterCapabilities
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ValidationResult
from gantry.handle import ExecutionHandle
from gantry.policy import PolicyRequirements
from gantry.target import ExecutionTarget


class ExecutionAdapter(Protocol):
    def capabilities(self) -> AdapterCapabilities: ...

    async def validate(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
        policy: PolicyRequirements,
    ) -> ValidationResult: ...

    async def submit(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
    ) -> ExecutionHandle: ...

    async def status(self, *, handle: ExecutionHandle) -> Execution: ...

    async def result(self, *, handle: ExecutionHandle) -> ExecutionResult: ...

    async def cancel(self, *, handle: ExecutionHandle, mode: str = "default") -> Execution: ...
