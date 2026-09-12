# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Protocol

from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ValidationResult
from gantry.handle import ExecutionHandle
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.pipeline import Pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.target import NoSQLTarget


class NoSQLAdapter(Protocol):
    def capabilities(self) -> NoSQLCapabilities: ...

    async def validate(
        self,
        pipeline: Pipeline,
        target: NoSQLTarget,
        context: Context,
        policy: NoSQLPolicy,
    ) -> ValidationResult: ...

    async def submit(
        self,
        pipeline: Pipeline,
        target: NoSQLTarget,
        context: Context,
    ) -> ExecutionHandle: ...

    async def status(self, handle: ExecutionHandle) -> Execution: ...

    async def result(self, handle: ExecutionHandle) -> ExecutionResult: ...

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution: ...
