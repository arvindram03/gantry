# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from gantry.context import Context
from gantry.nosql.pipeline import Pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.result import NoSQLResult
from gantry.nosql.verify import DocumentCheck
from gantry.tool import Tool


class _QueryConnection(Protocol):
    async def _query(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy,
        context: Context | None = None,
        verify: Sequence[DocumentCheck] = (),
    ) -> NoSQLResult: ...


@dataclass(frozen=True, slots=True)
class NoSQLQuery:
    _connection: _QueryConnection
    _policy: NoSQLPolicy
    _verify: Sequence[DocumentCheck]

    async def __call__(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        context: Context | None = None,
    ) -> NoSQLResult:
        return await self._connection._query(
            collection, pipeline, policy=self._policy, context=context, verify=self._verify
        )

    @property
    def input_schema(self) -> Mapping[str, object]:
        return {
            "type": "object",
            "properties": {
                "collection": {"type": "string"},
                "pipeline": {
                    "type": ["object", "array"],
                    "description": "A MongoDB filter document or aggregation pipeline stages",
                },
            },
            "required": ["collection", "pipeline"],
            "additionalProperties": False,
        }

    def tool(
        self,
        *,
        name: str = "query_nosql",
        description: str = "Run a governed, read-only MongoDB query or aggregation pipeline.",
    ) -> Tool[NoSQLResult]:
        return Tool(
            name=name,
            description=description,
            input_schema=self.input_schema,
            _handler=self._invoke_tool,
        )

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> NoSQLResult:
        unexpected = set(arguments) - {"collection", "pipeline"}
        if unexpected:
            raise ValueError(f"unexpected query tool arguments: {', '.join(sorted(unexpected))}")
        collection = arguments.get("collection")
        if not isinstance(collection, str):
            raise TypeError("collection must be a string")
        pipeline = arguments.get("pipeline")
        if not (
            isinstance(pipeline, (Mapping, Sequence)) and not isinstance(pipeline, (str, bytes))
        ):
            raise TypeError("pipeline must be a mapping or a sequence of stage mappings")
        return await self(collection, pipeline)
