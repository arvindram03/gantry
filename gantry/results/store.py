"""Persisting and reading Results."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from sqlalchemy import Row, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.results import Result, ResultKind, ResultStatus
from gantry.movement.result import MovementResult
from gantry.state.database import transaction
from gantry.state.tables import results


class ResultStore(Protocol):
    async def put(self, result: Result) -> None: ...

    async def get(self, name: str) -> Result | None: ...

    async def for_operation(self, operation: str) -> Sequence[Result]: ...


class PostgresResultStore:
    """Results in the metadata store.

    Provenance is stored beside the payload rather than inside it, so a result
    can be asked "why do we believe this" without parsing the thing it is
    evidence about.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def put(self, result: Result) -> None:
        payload = result.model_dump(mode="json", exclude={"provenance"})
        statement = (
            insert(results)
            .values(
                name=result.name,
                operation=result.provenance.operation or result.name,
                kind=result.kind.value,
                status=result.status.value,
                provenance=result.provenance.model_dump(mode="json"),
                payload=payload,
                created_at=result.created_at,
            )
            .on_conflict_do_update(
                index_elements=["name"],
                set_={
                    "status": result.status.value,
                    "provenance": result.provenance.model_dump(mode="json"),
                    "payload": payload,
                    "created_at": result.created_at,
                },
            )
        )
        async with transaction(self._engine) as connection:
            await connection.execute(statement)

    async def get(self, name: str) -> Result | None:
        async with transaction(self._engine) as connection:
            row = (
                await connection.execute(select(results).where(results.c.name == name))
            ).one_or_none()
        return None if row is None else _rehydrate(row)

    async def for_operation(self, operation: str) -> Sequence[Result]:
        async with transaction(self._engine) as connection:
            rows = (
                await connection.execute(
                    select(results)
                    .where(results.c.operation == operation)
                    .order_by(results.c.created_at)
                )
            ).all()
        return tuple(_rehydrate(row) for row in rows)


def _rehydrate(row: Row[tuple[object, ...]]) -> Result:
    mapping = row._mapping
    payload = dict(mapping["payload"])
    payload["provenance"] = mapping["provenance"]
    payload["status"] = mapping["status"]

    if ResultKind(mapping["kind"]) is ResultKind.MOVEMENT:
        return MovementResult.model_validate(payload)
    return Result.model_validate(payload)


def is_trustworthy(result: Result) -> bool:
    return result.status is ResultStatus.OK
