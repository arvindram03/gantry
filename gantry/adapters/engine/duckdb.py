"""Running artifacts on DuckDB.

The second engine exists to keep the abstraction honest. With one
implementation, "engine adapter" means whatever PostgreSQL happens to do; with
two, the differences have to be handled rather than assumed away.

DuckDB reads the Postgres tables directly through its `postgres` extension
rather than being handed a copy. Copying would make the two engines agree for
the wrong reason - they would be reading different data that happened to match
at the moment it was copied.

Its Python API is synchronous, so calls run in a worker thread. DuckDB releases
the GIL during query execution, and blocking the event loop on a long
aggregation would stall every other operation in the process.
"""

from __future__ import annotations

import asyncio
from typing import Any

import duckdb

from gantry.adapters.engine.base import ExplainResult, QueryResult, bounded, explain_text
from gantry.analysis.artifact import GeneratedArtifact


class DuckDBEngineAdapter:
    """Executes SQL artifacts on DuckDB."""

    def __init__(
        self,
        *,
        attach_postgres: str | None = None,
        database: str = ":memory:",
    ) -> None:
        self._connection: Any = duckdb.connect(database)
        if attach_postgres is not None:
            self._attach(attach_postgres)

    def _attach(self, dsn: str) -> None:
        """Read the Postgres tables in place.

        `USE` puts the attached database on the search path so an artifact's
        `public.request_logs` resolves without the compiler having to know
        which engine will run it.
        """
        self._connection.execute("INSTALL postgres")
        self._connection.execute("LOAD postgres")
        self._connection.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES, READ_ONLY)")
        self._connection.execute("USE pg")

    @property
    def engine(self) -> str:
        return "duckdb"

    async def explain(self, artifact: GeneratedArtifact) -> ExplainResult:
        rows = await self._fetch(f"EXPLAIN {artifact.body}")
        # DuckDB's EXPLAIN output is a rendered tree rather than a line
        # carrying a row estimate, so there is no number to report. Saying so
        # beats inventing one.
        return ExplainResult(plan=explain_text([tuple(row) for row in rows]))

    async def sample(self, artifact: GeneratedArtifact, *, limit: int) -> QueryResult:
        return await self._run(bounded(artifact.body, limit))

    async def execute(self, artifact: GeneratedArtifact) -> QueryResult:
        return await self._run(artifact.body)

    async def _run(self, body: str) -> QueryResult:
        def query() -> QueryResult:
            cursor = self._connection.execute(body)
            columns = tuple(str(item[0]) for item in cursor.description or ())
            rows = tuple(tuple(row) for row in cursor.fetchall())
            return QueryResult(columns=columns, rows=rows)

        return await asyncio.to_thread(query)

    async def _fetch(self, body: str) -> list[tuple[object, ...]]:
        def query() -> list[tuple[object, ...]]:
            return [tuple(row) for row in self._connection.execute(body).fetchall()]

        return await asyncio.to_thread(query)

    async def close(self) -> None:
        await asyncio.to_thread(self._connection.close)
