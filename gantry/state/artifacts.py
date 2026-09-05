"""Storing generated artifacts.

Generated code is provenance, not a transient string. A Result that cannot show
the exact SQL it came from cannot answer why anyone should believe it, and an
agent-proposed Analysis has to be reviewable before it is allowed to run.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from sqlalchemy import Row, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.analysis.artifact import ArtifactLanguage, GeneratedArtifact
from gantry.state.database import transaction
from gantry.state.tables import analysis_artifacts


class ArtifactStore(Protocol):
    async def put(self, artifact: GeneratedArtifact) -> str: ...

    async def get(self, content_hash: str) -> GeneratedArtifact | None: ...

    async def for_analysis(self, analysis: str) -> Sequence[GeneratedArtifact]: ...


class PostgresArtifactStore:
    """Generated artifacts in the metadata store."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def put(self, artifact: GeneratedArtifact) -> str:
        """Store an artifact, returning its hash.

        Idempotent by construction: the same compilation produces the same
        hash, so re-storing is a no-op rather than a duplicate.
        """
        statement = (
            insert(analysis_artifacts)
            .values(
                content_hash=artifact.content_hash,
                analysis=artifact.analysis,
                engine=artifact.engine,
                language=artifact.language.value,
                body=artifact.body,
                inputs=list(artifact.inputs),
                parameters=artifact.parameters,
                generated_at=artifact.generated_at,
            )
            .on_conflict_do_nothing(index_elements=["content_hash"])
        )
        async with transaction(self._engine) as connection:
            await connection.execute(statement)
        return artifact.content_hash

    async def get(self, content_hash: str) -> GeneratedArtifact | None:
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(
                    select(analysis_artifacts).where(
                        analysis_artifacts.c.content_hash == content_hash
                    )
                )
            ).one_or_none()
        return None if row is None else _rehydrate(row)

    async def for_analysis(self, analysis: str) -> Sequence[GeneratedArtifact]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(analysis_artifacts)
                    .where(analysis_artifacts.c.analysis == analysis)
                    .order_by(analysis_artifacts.c.generated_at)
                )
            ).all()
        return tuple(_rehydrate(row) for row in rows)


def _rehydrate(row: Row[tuple[object, ...]]) -> GeneratedArtifact:
    mapping = row._mapping
    return GeneratedArtifact(
        analysis=mapping["analysis"],
        engine=mapping["engine"],
        language=ArtifactLanguage(mapping["language"]),
        body=mapping["body"],
        inputs=tuple(mapping["inputs"]),
        parameters=dict(mapping["parameters"]),
        generated_at=mapping["generated_at"],
    )
