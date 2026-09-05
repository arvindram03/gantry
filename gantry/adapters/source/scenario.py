# SPDX-License-Identifier: Apache-2.0
"""The checkout-regression scenario, seeded.

The Analysis examples read `public.request_logs` and `public.deploy_events`,
and until now those tables existed only on the machine where they had been
created by hand. That made the Analysis half of the demo unreproducible: the
specs referenced tables a clean clone did not have, and the integration tests
skipped rather than failed, which is the quietest way for a feature to stop
being covered.

The data is the design document's scenario, made deterministic. A deploy at
12:00 makes checkout slower: p95 latency, error rate, database calls and
database wait time all rise against the deploy that preceded it. The numbers
are generated server-side from `generate_series`, so seeding twice produces
exactly the same table and the findings derived from it are stable enough to
assert on.

A second service is deployed in the same window and left alone. Without it the
temporal join has only one candidate on the right and would look correct even
if it were matching everything.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.state.database import transaction

# The window the Analysis examples declare. Fixed rather than relative to now:
# a spec that pins a time range and data that drifts away from it produce an
# empty result and a confusing demo.
WINDOW_START = datetime(2026, 9, 3, tzinfo=UTC)
WINDOW_END = datetime(2026, 9, 4, tzinfo=UTC)

BASELINE_COMMIT = "3a1f00"
REGRESSED_COMMIT = "8f3142"
UNRELATED_COMMIT = "c91b20"

SERVICE = "checkout-api"
OTHER_SERVICE = "search-api"

# One statement per entry: asyncpg prepares every statement, and a prepared
# statement cannot carry multiple commands.
SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS public.request_logs (
        request_id  bigint PRIMARY KEY,
        svc         text NOT NULL,
        latency_ms  numeric(10, 2) NOT NULL,
        status      integer NOT NULL,
        db_calls    integer NOT NULL,
        db_wait_ms  numeric(10, 2) NOT NULL,
        event_time  timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.deploy_events (
        deploy_id     bigint PRIMARY KEY,
        service_name  text NOT NULL,
        commit_sha    text NOT NULL,
        deployed_at   timestamptz NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_request_logs_event_time ON public.request_logs (event_time)",
    "CREATE INDEX IF NOT EXISTS ix_deploy_events_deployed_at ON public.deploy_events (deployed_at)",
)

# `svc` rather than `service`, and `service_name` on the other side: the two
# sources disagree about what to call the same field, which is what the
# Analysis spec's `normalize` block exists to reconcile. Naming them alike
# here would quietly remove the thing being demonstrated.
_DEPLOYS_SQL = """
INSERT INTO public.deploy_events (deploy_id, service_name, commit_sha, deployed_at)
VALUES
    (1, :service, :baseline,  :baseline_at),
    (2, :other,   :unrelated, :unrelated_at),
    (3, :service, :regressed, :regressed_at)
ON CONFLICT (deploy_id) DO NOTHING
"""

# Requests spread evenly across a ten-hour window that straddles the regressing
# deploy. Everything is a deterministic function of the row number: no random(),
# because a demo whose numbers move between runs cannot be asserted on and
# cannot be recognised when it is wrong.
_REQUESTS_SQL = """
INSERT INTO public.request_logs
    (request_id, svc, latency_ms, status, db_calls, db_wait_ms, event_time)
SELECT
    g,
    :service,
    CASE WHEN after_deploy THEN 480 + (g % 200) ELSE 52 + (g % 20) END,
    CASE
        WHEN after_deploy AND g % 40 = 0 THEN 504
        WHEN NOT after_deploy AND g % 200 = 0 THEN 500
        ELSE 200
    END,
    CASE WHEN after_deploy THEN 18 ELSE 3 END,
    CASE WHEN after_deploy THEN 240 + (g % 60) ELSE 6 + (g % 3) END,
    event_time
FROM (
    SELECT
        g,
        start_at + make_interval(secs => (g - 1) * spacing) AS event_time,
        start_at + make_interval(secs => (g - 1) * spacing) >= deployed AS after_deploy
    FROM
        -- Cast the bounds: an untyped bind leaves generate_series ambiguous
        -- between its integer and numeric overloads.
        generate_series(CAST(:lo AS bigint), CAST(:hi AS bigint)) AS g,
        -- Bound once rather than repeated inline. A TIMESTAMPTZ type-literal
        -- prefix applies only to a literal, never to a bind, so the cast has
        -- to be written as CAST. (And nothing in this comment may be written
        -- colon-first: SQLAlchemy scans comments for bind parameters too.)
        (SELECT
            CAST(:window_start AS timestamptz) AS start_at,
            CAST(:regressed_at AS timestamptz) AS deployed,
            -- Cast, or PostgreSQL infers this bind's type from the integer it
            -- is multiplied by and truncates the fractional spacing to zero -
            -- which put every row at the same instant and made the regression
            -- disappear.
            CAST(:spacing AS double precision) AS spacing
        ) AS bounds
) AS spread
ON CONFLICT (request_id) DO NOTHING
"""


async def create_scenario_schema(engine: AsyncEngine) -> None:
    async with transaction(engine) as connection:
        for statement in SCHEMA_STATEMENTS:
            await connection.execute(text(statement))


async def seed_checkout_scenario(
    engine: AsyncEngine,
    *,
    requests: int = 40_000,
    batch_rows: int = 100_000,
) -> int:
    """Seed the checkout-regression scenario, returning the requests written.

    ANALYZE runs afterwards because profiling reads the planner's statistics,
    and statistics that predate the data describe a table that no longer
    exists - the same reason the orders seeder does it.
    """
    await create_scenario_schema(engine)

    # Requests start an hour into the window and finish an hour before it ends,
    # so every row has a preceding deploy to attribute to and none falls outside
    # the range the spec declares.
    first = WINDOW_START.replace(hour=7)
    span_seconds = 10 * 60 * 60
    spacing = span_seconds / max(requests, 1)
    regressed_at = WINDOW_START.replace(hour=12)

    async with transaction(engine) as connection:
        await connection.execute(
            text(_DEPLOYS_SQL),
            {
                "service": SERVICE,
                "other": OTHER_SERVICE,
                "baseline": BASELINE_COMMIT,
                "unrelated": UNRELATED_COMMIT,
                "regressed": REGRESSED_COMMIT,
                "baseline_at": WINDOW_START.replace(hour=6),
                "unrelated_at": WINDOW_START.replace(hour=9),
                "regressed_at": regressed_at,
            },
        )

    for lo in range(1, requests + 1, batch_rows):
        hi = min(lo + batch_rows - 1, requests)
        async with transaction(engine) as connection:
            await connection.execute(
                text(_REQUESTS_SQL),
                {
                    "service": SERVICE,
                    "window_start": first,
                    "regressed_at": regressed_at,
                    "spacing": spacing,
                    "lo": lo,
                    "hi": hi,
                },
            )

    async with transaction(engine) as connection:
        await connection.execute(text("ANALYZE public.request_logs"))
        await connection.execute(text("ANALYZE public.deploy_events"))
    return requests
