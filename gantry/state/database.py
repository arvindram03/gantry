# SPDX-License-Identifier: Apache-2.0
"""Async database access for the metadata store."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

DATABASE_URL_ENV = "GANTRY_DATABASE_URL"
DEFAULT_DATABASE_URL = "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"


def database_url() -> str:
    """Resolve the metadata store URL from the environment."""
    return os.environ.get(DATABASE_URL_ENV, DEFAULT_DATABASE_URL)


def create_engine(url: str | None = None) -> AsyncEngine:
    return create_async_engine(url or database_url(), pool_pre_ping=True)


@asynccontextmanager
async def transaction(engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """Run a unit of work in one transaction.

    Checkpoint advances must be transactional with respect to the commit they
    attest to, so the store never exposes an autocommit path.
    """
    async with engine.begin() as connection:
        yield connection
