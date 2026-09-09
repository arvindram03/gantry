# SPDX-License-Identifier: Apache-2.0
"""Queries that outlive the request that started them.

**When you want this:** an agent asks for something expensive. You do not want
to hold an HTTP request open for four minutes, and you want the option to give
up on a query that is costing more than the answer is worth.

    pip install "gantry[postgres]"
    GANTRY_DATABASE_URL=postgresql://... python examples/long_running_query.py

`await query(sql)` runs to completion and is the right call most of the time.
When it is not, the same governed path splits into three:

    handle = await db.submit(sql, policy=...)   # returns immediately
    execution = await db.status(handle)         # poll from anywhere
    await db.cancel(handle)                     # stop paying for it

The handle is a value you can store and come back to. Everything the policy
decided still applies — submitting does not skip the checks, it only stops
waiting for the answer.
"""

from __future__ import annotations

import asyncio
import os

import gantry
from gantry.sql import SQLPolicy

URL = os.environ.get("GANTRY_DATABASE_URL", "postgresql://gantry:gantry@localhost:15432/gantry")

# The policy travels with the submission, not with the wait.
POLICY = SQLPolicy(read_only=True, max_rows=100, timeout_seconds=120)


async def main() -> int:
    db = gantry.sql.connect(os.environ.get("GANTRY_PROVIDER", "postgres"), url=URL)

    # 1. The ordinary case, for contrast: run it and wait.
    quick = await db.query(read_only=True, max_rows=5)("SELECT 1 AS answer")
    print(f"await query(...)      -> {quick.status.value}")

    # 2. Submit something slow. Control comes back immediately.
    handle = await db.submit("SELECT pg_sleep(30), 1 AS answer", policy=POLICY)
    print("\nsubmitted, not waiting")
    print(f"  handle:   {handle.gantry_id}")
    print(f"  engine:   {handle.engine} / {handle.target}")
    print(f"  native:   {handle.native_id}")
    print("  A value you can persist and come back to from another process.")

    await asyncio.sleep(1)
    running = await db.status(handle)
    print(f"\nstatus -> {running.state.value}")

    # 3. Decide it is not worth it.
    cancelled = await db.cancel(handle)
    print(f"cancel -> {cancelled.state.value}")

    # 4. A refusal still happens at submit time, before anything runs. Policy
    #    is not something `wait` gets around to checking later.
    refused = await db.execute("DELETE FROM pg_class", policy=POLICY)
    print(f"\nsubmitting a write under a read-only policy -> {refused.status.value}")
    print(f"  {refused.failure.message}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
