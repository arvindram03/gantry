# SPDX-License-Identifier: Apache-2.0
"""Framework-neutral, credential-free agent tool."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from gantry.sql.policy import SQLPolicy
from gantry.sql.result import SQLResult
from gantry.sql.schema import DatabaseSchema

_SUPPORTED_OPERATIONS = frozenset({"describe", "query", "explain"})


@dataclass(frozen=True, slots=True)
class SQLToolResult:
    operation: str
    result: SQLResult | DatabaseSchema | object


class SQLTool:
    """A minimal callable suitable for wrapping in any agent framework."""

    name = "gantry_sql"
    description = "Inspect schema and run governed SQL against the configured data system."

    def __init__(
        self,
        connection: object,
        policy: SQLPolicy,
        operations: tuple[str, ...],
    ) -> None:
        normalized = tuple(dict.fromkeys(operation.lower() for operation in operations))
        unsupported = set(normalized) - _SUPPORTED_OPERATIONS
        if unsupported:
            names = ", ".join(sorted(unsupported))
            raise ValueError(f"unsupported SQL tool operations: {names}")
        if not normalized:
            raise ValueError("SQL tool must expose at least one operation")
        self._connection = connection
        self._policy = policy
        self.operations = normalized

    @property
    def input_schema(self) -> Mapping[str, object]:
        return {
            "type": "object",
            "properties": {
                "operation": {"type": "string", "enum": list(self.operations)},
                "sql": {"type": "string"},
            },
            "required": ["operation"],
            "additionalProperties": False,
        }

    async def __call__(self, sql: str) -> SQLResult:
        if "query" not in self.operations:
            raise PermissionError("query is not exposed by this SQL tool")
        from gantry.sql.api import SQLConnection

        connection = self._connection
        if not isinstance(connection, SQLConnection):
            raise TypeError("invalid SQL connection")
        return await connection.query(sql, policy=self._policy)

    async def invoke(self, operation: str, *, sql: str | None = None) -> SQLToolResult:
        requested = operation.lower()
        if requested not in self.operations:
            raise PermissionError(f"{requested} is not exposed by this SQL tool")
        from gantry.sql.api import SQLConnection

        connection = self._connection
        if not isinstance(connection, SQLConnection):
            raise TypeError("invalid SQL connection")
        if requested == "describe":
            result: object = await connection.describe()
        elif requested == "explain":
            if sql is None:
                raise ValueError("sql is required for explain")
            validation = await connection.validate(sql, policy=self._policy)
            result = await connection.explain(sql) if validation.ok else validation
        else:
            if sql is None:
                raise ValueError("sql is required for query")
            result = await connection.query(sql, policy=self._policy)
        return SQLToolResult(requested, result)
