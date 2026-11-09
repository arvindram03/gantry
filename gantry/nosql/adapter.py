# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Protocol, runtime_checkable

from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ValidationResult
from gantry.handle import ExecutionHandle
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.pipeline import Pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.target import NoSQLTarget


@runtime_checkable
class DescribableAdapter(Protocol):
    """An adapter that can say what a database holds.

    Optional, like `MaterializationAdapter`: a store that cannot enumerate its
    own collections is still governable, and an adapter should not have to
    implement a listing it has no way to produce.
    """

    async def collections(self, target: NoSQLTarget) -> tuple[str, ...]: ...


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
