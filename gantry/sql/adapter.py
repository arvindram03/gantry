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
    """The contract a SQL backend implements to be governed by Gantry.

    Declare what you can enforce (`capabilities`), expose the schema
    (`describe`), ask the engine to check and estimate a statement (`validate`,
    `explain`), then run and track it (`submit`, `status`, `result`, `cancel`).
    A capability you do not declare is a bound Gantry will refuse to promise,
    so under-declaring is safe and over-declaring is not.
    """

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
