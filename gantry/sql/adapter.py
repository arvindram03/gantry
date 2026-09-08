# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Protocol

from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ValidationResult
from gantry.handle import ExecutionHandle
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.explain import ExplainResult
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import DatabaseSchema
from gantry.sql.target import SQLTarget


class SQLAdapter(Protocol):
    def capabilities(self) -> SQLCapabilities: ...

    async def describe(self, target: SQLTarget) -> DatabaseSchema: ...

    async def validate(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
        policy: SQLPolicy,
    ) -> ValidationResult: ...

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult: ...

    async def submit(self, sql: str, target: SQLTarget, context: Context) -> ExecutionHandle: ...

    async def status(self, handle: ExecutionHandle) -> Execution: ...

    async def result(self, handle: ExecutionHandle) -> ExecutionResult: ...

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution: ...
