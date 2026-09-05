"""Shared fixtures for integration tests."""

from __future__ import annotations

from gantry.adapters.source.scenario import seed_checkout_scenario
from gantry.adapters.source.seed import seed as seed_source
from gantry.state.database import transaction
from gantry.state.tables import operation_dependents, operations
from sqlalchemy import delete, text
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


async def ensure_checkout_scenario(engine: AsyncEngine) -> None:
    """Seed the checkout-regression tables if they are not already there.

    These used to be created by hand, so every test over them skipped on a
    machine that had never had them - which is the quietest way for a feature
    to stop being covered. Seeding is idempotent and cheap, so the tests now
    make their own fixture rather than hoping for one.
    """
    await seed_checkout_scenario(engine)


async def ensure_source_scale(engine: AsyncEngine, *, orders: int, customers: int) -> None:
    """Top the source up to the scale a test needs.

    Several tests used to assume whatever the last person seeded. That reads as
    a bug in the thing under test when it fails - the crash-replay tests looked
    like a crash-replay regression when `public.customers` was simply short,
    and the checksum tests looked like a checksum regression when the row they
    corrupt did not exist. Seeding is idempotent, so a test that states its own
    requirement costs nothing and fails for the right reason.
    """
    async with transaction(engine) as connection:
        present = (
            await connection.execute(
                text(
                    "SELECT coalesce("
                    "  (SELECT count(*) FROM public.customers), 0)"
                    "  WHERE to_regclass('public.customers') IS NOT NULL"
                )
            )
        ).scalar_one_or_none() or 0
    if present >= customers:
        return
    await seed_source(engine, orders=orders, customers=customers)
