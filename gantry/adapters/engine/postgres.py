"""Running artifacts on PostgreSQL."""

from __future__ import annotations

import re

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.adapters.engine.base import ExplainResult, QueryResult, bounded, explain_text
from gantry.analysis.artifact import GeneratedArtifact
from gantry.state.database import transaction

# PostgreSQL reports its estimate on the first plan line.
_ESTIMATE = re.compile(r"rows=(\d+).*?width=\d+")
_COST = re.compile(r"cost=[\d.]+\.\.([\d.]+)")


class PostgresEngineAdapter:
    """Executes SQL artifacts against PostgreSQL."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @property
    def engine(self) -> str:
        return "postgres"

    async def explain(self, artifact: GeneratedArtifact) -> ExplainResult:
        async with transaction(self._engine) as connection:
            rows = (await connection.execute(text(f"EXPLAIN {artifact.body}"))).all()

        plan = explain_text(rows)
        first = plan.splitlines()[0] if plan else ""
        estimate = _ESTIMATE.search(first)
        cost = _COST.search(first)
        return ExplainResult(
            plan=plan,
            estimated_rows=int(estimate.group(1)) if estimate else None,
            estimated_cost=float(cost.group(1)) if cost else None,
        )

    async def sample(self, artifact: GeneratedArtifact, *, limit: int) -> QueryResult:
        return await self._run(bounded(artifact.body, limit))

    async def execute(self, artifact: GeneratedArtifact) -> QueryResult:
        return await self._run(artifact.body)

    async def _run(self, body: str) -> QueryResult:
        async with transaction(self._engine) as connection:
            result = await connection.execute(text(body))
            columns = tuple(str(name) for name in result.keys())  # noqa: SIM118 - a Result, not a dict
            rows = tuple(tuple(row) for row in result.all())
        return QueryResult(columns=columns, rows=rows)

    async def close(self) -> None:
        await self._engine.dispose()
