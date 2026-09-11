# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence

from gantry.artifact import Artifact
from gantry.capabilities import AdapterCapabilities
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ValidationResult
from gantry.handle import ExecutionHandle
from gantry.nosql.adapter import NoSQLAdapter
from gantry.nosql.enforcement import policy_errors
from gantry.nosql.pipeline import Pipeline, classify_pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.target import NoSQLTarget
from gantry.target import ExecutionTarget


class NoSQLExecutionAdapter:
    def __init__(self, adapter: NoSQLAdapter, target: NoSQLTarget, policy: NoSQLPolicy) -> None:
        self.adapter = adapter
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
        policy: object,
    ) -> ValidationResult:
        if artifact.kind != "nosql" or not (
            isinstance(artifact.payload, (Mapping, Sequence))
            and not isinstance(artifact.payload, (str, bytes))
        ):
            return ValidationResult.rejected("NoSQL adapter requires a pipeline artifact")
        pipeline: Pipeline = artifact.payload
        if target.kind != self.target.provider:
            return ValidationResult.rejected("NoSQL execution target does not match the connection")
        collection = context.metadata.get("gantry.nosql.collection")
        if not isinstance(collection, str) or not collection.strip():
            return ValidationResult.rejected("NoSQL execution requires a target collection")
        try:
            classification = classify_pipeline(collection, pipeline)
        except (TypeError, ValueError) as error:
            return ValidationResult.rejected(f"pipeline classification failed: {error}")
        capabilities = self.adapter.capabilities()
        errors = list(policy_errors(classification, self.policy, capabilities))
        try:
            native_value: object = await self.adapter.validate(
                pipeline, self.target, context, self.policy
            )
        except Exception as error:  # noqa: BLE001
            errors.append(f"NoSQL validation raised {type(error).__name__}: {error}")
            native_value = ValidationResult.rejected()
        if not isinstance(native_value, ValidationResult):
            errors.append("NoSQL adapter returned an invalid validation result")
            native = ValidationResult.rejected()
        else:
            native = native_value
        errors.extend(native.errors)
        return ValidationResult(
            ok=not errors and native.ok,
            errors=tuple(dict.fromkeys(errors)),
            warnings=native.warnings,
            metadata={**native.metadata, "classification": classification},
        )

    async def submit(
        self,
        *,
        artifact: Artifact,
        target: ExecutionTarget,
        context: Context,
    ) -> ExecutionHandle:
        if not (
            isinstance(artifact.payload, (Mapping, Sequence))
            and not isinstance(artifact.payload, (str, bytes))
        ):
            raise TypeError("NoSQL artifact payload must be a pipeline")
        nosql_context = Context(
            resources=context.resources,
            metadata={**context.metadata, "gantry.nosql.policy": self.policy},
        )
        return await self.adapter.submit(artifact.payload, self.target, nosql_context)

    async def status(self, *, handle: ExecutionHandle) -> Execution:
        return await self.adapter.status(handle)

    async def result(self, *, handle: ExecutionHandle) -> ExecutionResult:
        return await self.adapter.result(handle)

    async def cancel(self, *, handle: ExecutionHandle, mode: str = "default") -> Execution:
        return await self.adapter.cancel(handle, mode)
