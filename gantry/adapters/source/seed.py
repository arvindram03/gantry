# SPDX-License-Identifier: Apache-2.0
"""Synthetic source data for the demo and for tests.

Generation runs server-side. A hundred million rows produced by a Python loop
would take hours and would also contradict the design principle the runtime is
built on: Python coordinates, engines move data.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.state.database import transaction

# One statement per entry: asyncpg prepares every statement, and a prepared
# statement cannot carry multiple commands.
SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS public.customers (
        customer_id  bigint PRIMARY KEY,
        email        text NOT NULL,
        region       text,
        created_at   timestamptz NOT NULL,
        source_lsn   bigint NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.orders (
        order_id     bigint PRIMARY KEY,
        customer_id  bigint NOT NULL REFERENCES public.customers(customer_id),
        amount       numeric(12, 2) NOT NULL,
        status       text NOT NULL,
        created_at   timestamptz NOT NULL,
        source_lsn   bigint NOT NULL DEFAULT 0
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_orders_created_at ON public.orders (created_at)",
)


# Customers are seeded at a fixed ratio to orders so the foreign key stays
# satisfiable and the key distribution is realistic rather than uniform.
_CUSTOMERS_SQL = """
INSERT INTO public.customers (customer_id, email, region, created_at, source_lsn)
SELECT g,
       'customer' || g || '@example.com',
       (ARRAY['us-east', 'us-west', 'eu-west', 'ap-south'])[1 + (g % 4)],
       timestamptz '2026-01-01' + (g % 365) * interval '1 day',
       -- Written rather than left to the column default. The seeder is also
       -- used against tables a target adapter created, and those carry the
       -- column as NOT NULL without a default.
       0
  FROM generate_series(CAST(1 AS bigint), CAST(:customers AS bigint)) AS g
    ON CONFLICT (customer_id) DO NOTHING
"""

_ORDERS_SQL = """
INSERT INTO public.orders (order_id, customer_id, amount, status, created_at, source_lsn)
SELECT g,
       1 + (g % :customers),
       round((random() * 500 + 5)::numeric, 2),
       (ARRAY['placed', 'shipped', 'delivered', 'cancelled'])[1 + (g % 4)],
       timestamptz '2026-01-01' + (g % 365) * interval '1 day',
       0
  FROM generate_series(CAST(:lo AS bigint), CAST(:hi AS bigint)) AS g
    ON CONFLICT (order_id) DO NOTHING
"""

# Batched so WAL can recycle between commits. A hundred million rows in one
# transaction holds every byte of WAL until the end, which is how a seed script
# fills a disk.
DEFAULT_BATCH_ROWS = 5_000_000


async def create_schema(engine: AsyncEngine) -> None:
    async with transaction(engine) as connection:
        for statement in SCHEMA_STATEMENTS:
            await connection.execute(text(statement))


async def seed(
    engine: AsyncEngine,
    *,
    orders: int,
    customers: int | None = None,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> int:
    """Seed the source, returning the number of orders requested.

    ANALYZE runs afterwards because profiling reads the planner's statistics,
    and statistics that predate the data describe a table that no longer exists.
    """
    customer_count = customers if customers is not None else max(1, orders // 100)

    await create_schema(engine)
    async with transaction(engine) as connection:
        await connection.execute(text(_CUSTOMERS_SQL), {"customers": customer_count})

    for lo in range(1, orders + 1, batch_rows):
        hi = min(lo + batch_rows - 1, orders)
        async with transaction(engine) as connection:
            await connection.execute(
                text(_ORDERS_SQL), {"lo": lo, "hi": hi, "customers": customer_count}
            )

    async with transaction(engine) as connection:
        await connection.execute(text("ANALYZE public.customers"))
        await connection.execute(text("ANALYZE public.orders"))
    return orders
