# SPDX-License-Identifier: Apache-2.0
"""Persisting compiled plans.

A plan has to outlive the process that compiled it. Any worker picking up a
partition needs the same bounds, the same dependencies and the same Dataset
versions, and reconstructing them by recompiling would mean trusting that two
compilations agree - which is exactly the assumption content addressing exists
to avoid making.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from sqlalchemy import Row, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.provenance import DatasetPin
from gantry.lifecycle.plan import PlanVersion
from gantry.state.database import transaction
from gantry.state.tables import plan_versions


class PlanMismatchError(Exception):
    """Raised when a plan is re-stored with different content under one version.

    Plans are immutable. Two different plans sharing a version number would
    make every checkpoint referring to that version ambiguous.
    """

    def __init__(self, operation: str, version: int) -> None:
        super().__init__(
            f"plan {operation!r} version {version} already exists with different content; "
            f"a changed plan needs a new version"
        )


class PlanStore(Protocol):
    async def put(self, plan: PlanVersion, pins: Sequence[DatasetPin] = ()) -> None: ...

    async def get(self, operation: str, version: int) -> PlanVersion | None: ...

    async def latest(self, operation: str) -> PlanVersion | None: ...

    async def pins(self, operation: str, version: int) -> Sequence[DatasetPin]: ...


class PostgresPlanStore:
    """Compiled plans in the metadata store."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def put(self, plan: PlanVersion, pins: Sequence[DatasetPin] = ()) -> None:
        existing = await self.get(plan.operation, plan.version)
        if existing is not None and existing.content_hash != plan.content_hash:
            raise PlanMismatchError(plan.operation, plan.version)

        statement = (
            insert(plan_versions)
            .values(
                operation=plan.operation,
                version=plan.version,
                content_hash=plan.content_hash,
                guarantee_fingerprint=plan.guarantee_fingerprint,
                plan=plan.model_dump(mode="json"),
                dataset_pins=[pin.model_dump(mode="json") for pin in pins],
                created_at=plan.created_at,
            )
            .on_conflict_do_nothing(index_elements=["operation", "version"])
        )
        async with transaction(self._engine) as connection:
            await connection.execute(statement)

    async def get(self, operation: str, version: int) -> PlanVersion | None:
        row = await self._row(operation, version)
        return None if row is None else PlanVersion.model_validate(row._mapping["plan"])

    async def latest(self, operation: str) -> PlanVersion | None:
        """The highest stored version, or None if this Operation never planned."""
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(
                    select(plan_versions)
                    .where(plan_versions.c.operation == operation)
                    .order_by(plan_versions.c.version.desc())
                    .limit(1)
                )
            ).one_or_none()
        return None if row is None else PlanVersion.model_validate(row._mapping["plan"])

    async def pins(self, operation: str, version: int) -> Sequence[DatasetPin]:
        row = await self._row(operation, version)
        if row is None:
            return ()
        return tuple(DatasetPin.model_validate(pin) for pin in row._mapping["dataset_pins"])

    async def _row(self, operation: str, version: int) -> Row[tuple[object, ...]] | None:
        async with transaction(self._engine) as connection:
            return (
                await connection.execute(
                    select(plan_versions).where(
                        plan_versions.c.operation == operation,
                        plan_versions.c.version == version,
                    )
                )
            ).one_or_none()
