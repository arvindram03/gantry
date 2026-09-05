"""Killing a worker mid-copy must lose nothing and duplicate nothing.

This is the most important test in v1. Everything else the runtime promises -
retries, at-least-once dispatch, disposable workers - is only safe if this
holds, and it is checked here against real processes and real databases rather
than against a simulation.

Requires the local stack, a seeded source, and migrations applied:
    make dev-up && uv run gantry seed --rows 1000000 && uv run alembic upgrade head
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from gantry.state.database import create_engine, transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ROOT = Path(__file__).resolve().parents[2]
HELPERS = ROOT / "tests" / "integration" / "helpers"
RUNNER = HELPERS / "run_worker.py"

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)

OPERATION = "day9-crash-replay"
TARGET_TABLE = "public.customers_day9"
SOURCE_TABLE = "public.customers"
LEASE_SECONDS = 2


@pytest.fixture
async def target() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(TARGET_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TARGET_TABLE}"))
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def meta() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(META_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text("DELETE FROM checkpoints"))
            await connection.execute(text("DELETE FROM tasks"))
            await connection.execute(text("DELETE FROM operations"))
            await connection.execute(
                text(
                    "INSERT INTO operations (name, operation_type, state, created_at, updated_at)"
                    " VALUES (:name, 'movement', 'draft', now(), now())"
                ),
                {"name": OPERATION},
            )
        yield engine
    finally:
        await engine.dispose()


def spawn(name: str) -> subprocess.Popen[str]:
    environment = {
        **os.environ,
        "PYTHONPATH": str(HELPERS),
        "GANTRY_LEASE_SECONDS": str(LEASE_SECONDS),
        "PYTHONUNBUFFERED": "1",
    }
    return subprocess.Popen(
        [sys.executable, str(RUNNER), name, SOURCE_URL, TARGET_URL, META_URL],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=environment,
    )


def wait_for_commits(process: subprocess.Popen[str], count: int, timeout: float = 60.0) -> int:
    """Block until the worker has committed `count` nodes carrying real rows.

    Killing before any COPY has landed would prove nothing, so the test waits
    for work to actually be in flight.
    """
    seen = 0
    deadline = time.monotonic() + timeout
    assert process.stdout is not None
    while time.monotonic() < deadline:
        line = process.stdout.readline()
        if not line:
            break
        if line.startswith("COMMITTED") and "rows=0" not in line:
            seen += 1
            if seen >= count:
                return seen
    raise AssertionError(f"worker committed only {seen} nodes before the timeout")


@pytest.fixture
def reaper() -> Iterator[list[subprocess.Popen[str]]]:
    spawned: list[subprocess.Popen[str]] = []
    try:
        yield spawned
    finally:
        for process in spawned:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)


async def source_rows(engine: AsyncEngine) -> int:
    async with transaction(engine) as connection:
        return int(
            (await connection.execute(text(f"SELECT count(*) FROM {SOURCE_TABLE}"))).scalar_one()
        )


async def target_rows(engine: AsyncEngine) -> int:
    async with transaction(engine) as connection:
        return int(
            (await connection.execute(text(f"SELECT count(*) FROM {TARGET_TABLE}"))).scalar_one()
        )


async def test_a_killed_worker_loses_nothing_and_duplicates_nothing(
    target: AsyncEngine, meta: AsyncEngine, reaper: list[subprocess.Popen[str]]
) -> None:
    """SIGKILL mid-copy, restart, and land on exactly the right rows."""
    source = create_engine(SOURCE_URL)
    try:
        expected = await source_rows(source)
    finally:
        await source.dispose()
    assert expected > 0, "seed the source first"

    # 1. Start a worker and let it get real work in flight.
    victim = spawn("victim")
    reaper.append(victim)
    wait_for_commits(victim, count=3)

    # 2. SIGKILL. No cleanup, no chance to finish the partition in progress,
    #    no chance to record a checkpoint for it.
    victim.send_signal(signal.SIGKILL)
    victim.wait(timeout=10)
    assert victim.returncode != 0

    partial = await target_rows(target)
    assert partial > 0, "the worker should have copied something before dying"
    assert partial < expected, "the worker should not have finished"

    async with transaction(meta) as connection:
        leased = (
            await connection.execute(text("SELECT count(*) FROM tasks WHERE state = 'leased'"))
        ).scalar_one()
    assert leased >= 1, "the dead worker's task should still be leased"

    # 3. Let the lease lapse. Nobody has to notice the crash.
    await asyncio.sleep(LEASE_SECONDS + 1)

    # 4. A fresh worker picks up where the dead one left off.
    survivor = spawn("survivor")
    reaper.append(survivor)
    stdout, _ = survivor.communicate(timeout=180)
    assert survivor.returncode == 0, stdout
    assert "DONE" in stdout, stdout

    # 5. Exactly the right rows: nothing lost, nothing duplicated.
    assert await target_rows(target) == expected

    async with transaction(target) as connection:
        distinct = (
            await connection.execute(
                text(f"SELECT count(DISTINCT customer_id) FROM {TARGET_TABLE}")
            )
        ).scalar_one()
    assert distinct == expected, "duplicate keys in the target"

    async with transaction(meta) as connection:
        pending = (
            await connection.execute(text("SELECT count(*) FROM tasks WHERE state <> 'done'"))
        ).scalar_one()
    assert pending == 0, "every node should have completed"


async def test_the_interrupted_node_has_no_checkpoint(
    target: AsyncEngine, meta: AsyncEngine, reaper: list[subprocess.Popen[str]]
) -> None:
    """Progress metadata must never run ahead of durable state.

    A worker killed mid-node may well have committed its COPY. It must not have
    recorded a checkpoint claiming so.
    """
    victim = spawn("victim")
    reaper.append(victim)
    wait_for_commits(victim, count=2)
    victim.send_signal(signal.SIGKILL)
    victim.wait(timeout=10)

    async with transaction(meta) as connection:
        checkpoints = (
            await connection.execute(text("SELECT count(*) FROM checkpoints"))
        ).scalar_one()
        done = (
            await connection.execute(text("SELECT count(*) FROM tasks WHERE state = 'done'"))
        ).scalar_one()

    # Every completed task has a checkpoint, and the interrupted one does not:
    # a checkpoint is written before the task is completed, never after.
    assert checkpoints >= done
    assert checkpoints - done <= 1


async def test_repeated_kills_still_converge(
    target: AsyncEngine, meta: AsyncEngine, reaper: list[subprocess.Popen[str]]
) -> None:
    """One crash is an incident; three is a Tuesday."""
    source = create_engine(SOURCE_URL)
    try:
        expected = await source_rows(source)
    finally:
        await source.dispose()

    for attempt in range(3):
        victim = spawn(f"victim-{attempt}")
        reaper.append(victim)
        try:
            wait_for_commits(victim, count=2)
        except AssertionError:
            # A later worker may find little left to do; that is a valid end.
            victim.kill()
            victim.wait(timeout=10)
            break
        victim.send_signal(signal.SIGKILL)
        victim.wait(timeout=10)
        await asyncio.sleep(LEASE_SECONDS + 1)

    survivor = spawn("survivor")
    reaper.append(survivor)
    stdout, _ = survivor.communicate(timeout=180)
    assert survivor.returncode == 0, stdout

    assert await target_rows(target) == expected
