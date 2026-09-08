# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from gantry.artifact import Artifact
from gantry.capabilities import AdapterCapabilities
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ValidationResult
from gantry.handle import ExecutionHandle
from gantry.policy import PolicyRequirements
from gantry.sql.adapter import SQLAdapter
from gantry.sql.dialect import SQLDialect
from gantry.sql.enforcement import policy_errors
from gantry.sql.policy import SQLPolicy
from gantry.sql.target import SQLTarget
from gantry.target import ExecutionTarget


class SQLExecutionAdapter:
    """Adapts one governed SQL connection to the core execution protocol."""

    def __init__(
        self,
        adapter: SQLAdapter,
        dialect: SQLDialect,
        target: SQLTarget,
        policy: SQLPolicy,
    ) -> None:
        self.adapter = adapter
        self.dialect = dialect
        self.target = target
        self.policy = policy

    def capabilities(self) -> AdapterCapabilities:
        return self.adapter.capabilities().core_capabilities()

    async def validate(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
        policy: PolicyRequirements,
    ) -> ValidationResult:
        if artifact.kind != "sql" or not isinstance(artifact.payload, str):
            return ValidationResult.rejected("SQL adapter requires a string SQL artifact")
        if target.kind != self.target.provider:
            return ValidationResult.rejected("SQL execution target does not match the connection")

        classification = self.dialect.classify(artifact.payload)
        capabilities = self.adapter.capabilities()
        explain = None
        if self.policy.max_bytes_scanned is not None or self.policy.max_cost_usd is not None:
            try:
                explain = await self.adapter.explain(artifact.payload, self.target)
            except Exception as error:
                return ValidationResult.rejected(
                    f"SQL explain raised {type(error).__name__}: {error}"
                )
        errors = list(policy_errors(classification, self.policy, capabilities, explain))
        try:
            native_value: object = await self.adapter.validate(
                artifact.payload,
                self.target,
                context,
                self.policy,
            )
        except Exception as error:
            errors.append(f"SQL validation raised {type(error).__name__}: {error}")
            native_value = ValidationResult.rejected()
        if not isinstance(native_value, ValidationResult):
            errors.append("SQL adapter returned an invalid validation result")
            native = ValidationResult.rejected()
        else:
            native = native_value
        errors.extend(native.errors)
        return ValidationResult(
            ok=not errors and native.ok,
            errors=tuple(dict.fromkeys(errors)),
            warnings=native.warnings,
            metadata={
                **native.metadata,
                "classification": classification,
                "explain": explain,
            },
        )

    async def submit(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
    ) -> ExecutionHandle:
        if not isinstance(artifact.payload, str):
            raise TypeError("SQL artifact payload must be a string")
        sql_context = Context(
            resources=context.resources,
            metadata={**context.metadata, "gantry.sql.policy": self.policy},
        )
        return await self.adapter.submit(artifact.payload, self.target, sql_context)

    async def status(self, *, handle: ExecutionHandle) -> Execution:
        return await self.adapter.status(handle)

    async def result(self, *, handle: ExecutionHandle) -> ExecutionResult:
        return await self.adapter.result(handle)

    async def cancel(self, *, handle: ExecutionHandle, mode: str = "default") -> Execution:
        return await self.adapter.cancel(handle, mode)
