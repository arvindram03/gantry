"""Shared fixtures for integration tests."""

from __future__ import annotations

from gantry.state.database import transaction
from gantry.state.tables import metadata, operations
from sqlalchemy import Table, delete
from sqlalchemy.ext.asyncio import AsyncEngine


def _operation_dependents() -> tuple[Table, ...]:
    """Every table with a foreign key to `operations`, derived from the schema.

    This was a hand-written list twice, and drifted twice - once when
    verification_results arrived and once when dead_letters did, each time
    breaking a dozen unrelated tests with a foreign key violation. Deriving it
    means a new table joins the cleanup by existing.
    """
    return tuple(
        table
        for table in metadata.sorted_tables
        if table is not operations
        and any(
            key.column.table is operations
            for constraint in table.foreign_key_constraints
            for key in constraint.elements
        )
    )


async def clear_operation(engine: AsyncEngine, name: str) -> None:
    """Remove an operation and everything hanging off it."""
    async with transaction(engine) as connection:
        for table in _operation_dependents():
            await connection.execute(delete(table).where(table.c.operation == name))
        await connection.execute(delete(operations).where(operations.c.name == name))


async def clear_all_operations(engine: AsyncEngine) -> None:
    """Remove every operation. For tests that need an empty metadata store."""
    async with transaction(engine) as connection:
        for table in _operation_dependents():
            await connection.execute(delete(table))
        await connection.execute(delete(operations))
