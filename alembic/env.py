"""Alembic environment.

The URL comes from the application rather than alembic.ini so migrations and
the runtime cannot disagree about which database they are talking to.
"""

from __future__ import annotations

import asyncio

from alembic import context
from gantry.state.database import create_engine
from gantry.state.tables import metadata
from sqlalchemy.engine import Connection

config = context.config
target_metadata = metadata


def _run(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_online() -> None:
    engine = create_engine()
    async with engine.connect() as connection:
        await connection.run_sync(_run)
        await connection.commit()
    await engine.dispose()


def run_offline() -> None:
    from gantry.state.database import database_url

    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_offline()
else:
    asyncio.run(run_online())
