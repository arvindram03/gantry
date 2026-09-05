# SPDX-License-Identifier: Apache-2.0
"""The durable Dataset registry.

Same contract as the in-memory and JSON stores, with the concurrency guarantee
they cannot offer: two processes registering the same manifest concurrently
produce one version, not two, because the unique constraint on (name,
content_hash) settles the race in the database rather than in application code.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest, DatasetRef, DatasetVersion
from gantry.registry.errors import DatasetNotFoundError, DatasetVersionNotFoundError
from gantry.state.database import transaction
from gantry.state.tables import dataset_versions


def _utc_now() -> datetime:
    return datetime.now(UTC)


class PostgresDatasetRegistry:
    """A Dataset registry backed by the metadata store."""

    def __init__(self, engine: AsyncEngine, clock: Callable[[], datetime] | None = None) -> None:
        self._engine = engine
        self._clock: Callable[[], datetime] = clock or _utc_now

    async def register(self, manifest: DatasetManifest) -> DatasetVersion:
        content_hash = manifest.content_hash
        async with transaction(self._engine) as connection:
            existing = (
                await connection.execute(
                    select(dataset_versions).where(
                        dataset_versions.c.name == manifest.name,
                        dataset_versions.c.content_hash == content_hash,
                    )
                )
            ).one_or_none()
            if existing is not None:
                return _to_version(existing)

            highest = (
                await connection.execute(
                    select(func.coalesce(func.max(dataset_versions.c.version), 0)).where(
                        dataset_versions.c.name == manifest.name
                    )
                )
            ).scalar_one()

            # ON CONFLICT DO NOTHING makes a concurrent identical registration a
            # no-op rather than an error; the follow-up read returns whichever
            # row won.
            statement = (
                insert(dataset_versions)
                .values(
                    name=manifest.name,
                    version=highest + 1,
                    content_hash=content_hash,
                    manifest=manifest.model_dump(mode="json"),
                    registered_at=self._clock(),
                )
                .on_conflict_do_nothing()
                .returning(dataset_versions)
            )
            inserted = (await connection.execute(statement)).one_or_none()
            if inserted is not None:
                return _to_version(inserted)

            winner = (
                await connection.execute(
                    select(dataset_versions).where(
                        dataset_versions.c.name == manifest.name,
                        dataset_versions.c.content_hash == content_hash,
                    )
                )
            ).one()
            return _to_version(winner)

    async def get(self, ref: DatasetRef) -> DatasetVersion:
        async with transaction(self._engine) as connection:
            query = select(dataset_versions).where(dataset_versions.c.name == ref.name)
            if ref.version is None:
                query = query.order_by(dataset_versions.c.version.desc()).limit(1)
            else:
                query = query.where(dataset_versions.c.version == ref.version)
            row = (await connection.execute(query)).one_or_none()

            if row is not None:
                return _to_version(row)

            highest = (
                await connection.execute(
                    select(func.coalesce(func.max(dataset_versions.c.version), 0)).where(
                        dataset_versions.c.name == ref.name
                    )
                )
            ).scalar_one()

        if highest == 0:
            raise DatasetNotFoundError(ref.name)
        raise DatasetVersionNotFoundError(ref.name, ref.version or 0, highest)

    async def versions(self, name: str) -> Sequence[DatasetVersion]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(dataset_versions)
                    .where(dataset_versions.c.name == name)
                    .order_by(dataset_versions.c.version)
                )
            ).all()
        if not rows:
            raise DatasetNotFoundError(name)
        return tuple(_to_version(row) for row in rows)

    async def list(self) -> Sequence[DatasetVersion]:
        latest = (
            select(
                dataset_versions.c.name,
                func.max(dataset_versions.c.version).label("version"),
            )
            .group_by(dataset_versions.c.name)
            .subquery()
        )
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(dataset_versions)
                    .join(
                        latest,
                        (dataset_versions.c.name == latest.c.name)
                        & (dataset_versions.c.version == latest.c.version),
                    )
                    .order_by(dataset_versions.c.name)
                )
            ).all()
        return tuple(_to_version(row) for row in rows)


def _to_version(row: object) -> DatasetVersion:
    mapping = row._mapping  # type: ignore[attr-defined]
    return DatasetVersion(
        name=mapping["name"],
        version=mapping["version"],
        manifest=DatasetManifest.model_validate(mapping["manifest"]),
        content_hash=mapping["content_hash"],
        registered_at=mapping["registered_at"],
    )
