# SPDX-License-Identifier: Apache-2.0
"""Configured, governed SQL query operation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gantry.context import Context
from gantry.sql.policy import SQLPolicy
from gantry.sql.result import SQLResult
from gantry.tool import Tool
from gantry.verifier import Verifier

if TYPE_CHECKING:
    from gantry.sql.api import SQLConnection


@dataclass(frozen=True, slots=True)
class SQLQuery:
    """A query policy configured once for direct calls or agent tools."""

    _connection: SQLConnection = field(repr=False)
    _policy: SQLPolicy = field(repr=False)
    _verify: tuple[Verifier, ...] = field(default=(), repr=False)

    async def __call__(self, sql: str, *, context: Context | None = None) -> SQLResult:
        return await self._connection._query(
            sql,
            policy=self._policy,
            context=context,
            verify=self._verify,
        )

    def tool(
        self,
        *,
        name: str = "query_sql",
        description: str = "Run a governed read-only SQL query.",
    ) -> Tool[SQLResult]:
        """Return the narrow framework-neutral form of this query operation."""

        if not self._policy.read_only:
            raise ValueError(
                "query.tool() requires read_only=True; use db.materialize() for agent writes"
            )
        return Tool(
            name=name,
            description=description,
            input_schema={
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
                "additionalProperties": False,
            },
            _handler=self._invoke_tool,
        )

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> SQLResult:
        _require_only_sql(arguments)
        sql = arguments.get("sql")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        return await self(sql)


def _require_only_sql(arguments: Mapping[str, object]) -> None:
    unexpected = set(arguments) - {"sql"}
    if unexpected:
        names = ", ".join(sorted(unexpected))
        raise ValueError(f"unexpected query tool arguments: {names}")
