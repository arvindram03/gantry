# SPDX-License-Identifier: Apache-2.0
"""Credential-free, framework-neutral Flink SQL agent tool."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from gantry.execution import Execution, ValidationResult
from gantry.flink.artifact import FlinkMode, FlinkSQLArtifact
from gantry.flink.execution import FlinkResult, StreamingHealth
from gantry.flink.verification import FlinkHealthCheck
from gantry.handle import ExecutionHandle

if TYPE_CHECKING:
    from gantry.flink.api import FlinkConnection

_SUPPORTED_OPERATIONS = frozenset({"validate", "run", "status", "cancel", "health"})


@dataclass(frozen=True, slots=True)
class FlinkToolResult:
    operation: str
    result: ValidationResult | FlinkResult | Execution | StreamingHealth


class FlinkTool:
    name = "gantry_flink_sql"
    description = "Validate, run, observe, and cancel Flink SQL on the configured cluster."

    def __init__(
        self,
        connection: object,
        mode: FlinkMode,
        checks: tuple[FlinkHealthCheck, ...],
        timeout: float | None,
        operations: tuple[str, ...] = ("validate", "run", "status", "cancel", "health"),
    ) -> None:
        unsupported = set(operations) - _SUPPORTED_OPERATIONS
        if unsupported:
            raise ValueError(f"unsupported Flink tool operations: {', '.join(sorted(unsupported))}")
        self._connection = connection
        self._mode = mode
        self._checks = checks
        self._timeout = timeout
        self.operations = operations

    @property
    def input_schema(self) -> Mapping[str, object]:
        return {
            "type": "object",
            "properties": {
                "operation": {"type": "string", "enum": list(self.operations)},
                "sql": {"type": "string"},
                "declared_inputs": {"type": "array", "items": {"type": "string"}},
                "declared_outputs": {"type": "array", "items": {"type": "string"}},
                "handle": {"description": "Previously returned Gantry execution handle"},
            },
            "required": ["operation"],
            "additionalProperties": False,
        }

    async def __call__(
        self,
        sql: str,
        *,
        declared_inputs: tuple[str, ...] = (),
        declared_outputs: tuple[str, ...] = (),
    ) -> FlinkResult:
        connection = self._checked_connection()
        return await connection.run(
            sql=sql,
            mode=self._mode,
            declared_inputs=declared_inputs,
            declared_outputs=declared_outputs,
            checks=self._checks,
            timeout_seconds=self._timeout,
        )

    async def invoke(
        self,
        operation: str,
        *,
        sql: str | None = None,
        handle: ExecutionHandle | None = None,
        declared_inputs: tuple[str, ...] = (),
        declared_outputs: tuple[str, ...] = (),
    ) -> FlinkToolResult:
        requested = operation.lower()
        if requested not in self.operations:
            raise PermissionError(f"{requested} is not exposed by this Flink tool")
        connection = self._checked_connection()
        if requested in {"validate", "run"}:
            if sql is None:
                raise ValueError(f"sql is required for {requested}")
            artifact = FlinkSQLArtifact(
                sql,
                self._mode,
                declared_inputs,
                declared_outputs,
            )
            result: ValidationResult | FlinkResult | Execution | StreamingHealth
            if requested == "validate":
                result = await connection.validate(artifact)
            else:
                result = await connection.run(
                    artifact,
                    checks=self._checks,
                    timeout_seconds=self._timeout,
                )
        else:
            if handle is None:
                raise ValueError(f"handle is required for {requested}")
            if requested == "status":
                result = await connection.status(handle)
            elif requested == "cancel":
                result = await connection.cancel(handle)
            else:
                result = await connection.health(handle, checks=self._checks)
        return FlinkToolResult(requested, result)

    def _checked_connection(self) -> FlinkConnection:
        from gantry.flink.api import FlinkConnection

        if not isinstance(self._connection, FlinkConnection):
            raise TypeError("invalid Flink connection")
        return self._connection
