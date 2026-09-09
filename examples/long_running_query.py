# SPDX-License-Identifier: Apache-2.0
"""An agent asks for something that will never finish.

**The situation.** A model was asked which regions have overlapping customers
and produced a self-join with no selective predicate. Over 200,000 orders that
is roughly thirteen billion pairs. It is valid SQL, the planner accepts it, and
it will run until something stops it.

This is not a rare case. It is the most common way an agent-written query hurts
you, and the answer is not a better prompt — it is a timeout, a handle, and the
ability to cancel.

    pip install "gantry[postgres]"
    psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
    GANTRY_DATABASE_URL=postgresql://... python examples/long_running_query.py

`await query(sql)` runs to completion and is right most of the time. When it is
not, the same governed path splits into three:

    handle = await db.submit(sql, policy=...)   # returns immediately
    execution = await db.status(handle)         # poll from anywhere
    await db.cancel(handle)                     # stop paying for it

The handle is a value you can store and come back to from another process. The
policy still applies — submitting does not skip the checks, it only stops
waiting for the answer.
"""

from __future__ import annotations

import asyncio
import os

import gantry
from gantry.sql import SQLPolicy

URL = os.environ.get("GANTRY_DATABASE_URL", "postgresql://gantry:gantry@localhost:15432/gantry")

# The policy travels with the submission, not with the wait.
POLICY = SQLPolicy(
    read_only=True,
    allowed_schemas=("analytics",),
    max_rows=200,
    # A server-side statement timeout. The last line of defence, and the one
    # that works even if nothing is watching.
    timeout_seconds=300,
)

# What the agent produced. A self-join on a three-value column.
RUNAWAY = (
    "SELECT a.region, count(*) AS pairs "
    "FROM analytics.orders a "
    "JOIN analytics.orders b ON a.region = b.region "
    "GROUP BY a.region"
)

# What it should have written.
INTENDED = (
    "SELECT region, count(DISTINCT customer_id) AS customers "
    "FROM analytics.orders GROUP BY region ORDER BY customers DESC"
)


async def main() -> int:
    db = gantry.sql.connect(os.environ.get("GANTRY_PROVIDER", "postgres"), url=URL)

    # 1. The query that should have been written: fast, bounded, answered.
    good = await db.query(read_only=True, schemas=("analytics",), max_rows=10)(INTENDED)
    print(f"the intended question -> {good.status.value}")
    if good.inline is not None:
        for row in good.inline.rows:
            print(f"  {row}")

    # 2. The runaway. Submit it and get control straight back.
    print("\nthe query the agent actually wrote")
    handle = await db.submit(RUNAWAY, policy=POLICY)
    print(f"  submitted: {handle.gantry_id}")
    print(f"  native id: {handle.native_id}")
    print("  Control is back immediately. Nothing is waiting on this.")

    await asyncio.sleep(2)
    running = await db.status(handle)
    print(f"  status after 2s: {running.state.value}")

    # 3. Stop paying for it. In a real system this is a button, or a supervisor
    #    that cancels anything still running after N seconds.
    cancelled = await db.cancel(handle)
    print(f"  cancelled -> {cancelled.state.value}")

    # 4. And the checks still happen at submission, before anything runs.
    #    Being asynchronous does not mean being unguarded.
    refused = await db.execute("DELETE FROM analytics.orders", policy=POLICY)
    print(f"\na write submitted under a read-only policy -> {refused.status.value}")
    print(f"  {refused.failure.message}")

    print(
        "\nThree defences, in order of preference: a policy that refuses the\n"
        "query, a timeout that bounds it, and a handle that lets a human end it.\n"
        "The third is the one people forget to build."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
