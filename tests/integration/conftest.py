"""Shared fixtures for integration tests."""

from __future__ import annotations

from gantry.state.database import transaction
from gantry.state.tables import (
    checkpoints,
    operations,
    plan_versions,
    results,
    state_transitions,
    tasks,
)
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine

# Everything that references an operation, in an order that satisfies the
# foreign keys. Kept in one place so adding a table breaks one function rather
# than every fixture that happens to clean up after itself.
_DEPENDENTS = (results, checkpoints, state_transitions, tasks, plan_versions)


async def clear_operation(engine: AsyncEngine, name: str) -> None:
    """Remove an operation and everything hanging off it."""
    async with transaction(engine) as connection:
        for table in _DEPENDENTS:
            await connection.execute(delete(table).where(table.c.operation == name))
        await connection.execute(delete(operations).where(operations.c.name == name))


async def clear_all_operations(engine: AsyncEngine) -> None:
    """Remove every operation. For tests that need an empty metadata store."""
    async with transaction(engine) as connection:
        for table in _DEPENDENTS:
            await connection.execute(delete(table))
        await connection.execute(delete(operations))
