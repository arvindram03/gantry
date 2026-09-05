"""Shared fixtures for integration tests."""

from __future__ import annotations

from gantry.state.database import transaction
from gantry.state.tables import operation_dependents, operations
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine


async def clear_operation(engine: AsyncEngine, name: str) -> None:
    """Remove an operation and everything hanging off it."""
    async with transaction(engine) as connection:
        for table in operation_dependents():
            await connection.execute(delete(table).where(table.c.operation == name))
        await connection.execute(delete(operations).where(operations.c.name == name))


async def clear_all_operations(engine: AsyncEngine) -> None:
    """Remove every operation. For tests that need an empty metadata store."""
    async with transaction(engine) as connection:
        for table in operation_dependents():
            await connection.execute(delete(table))
        await connection.execute(delete(operations))
