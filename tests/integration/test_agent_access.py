"""The access ladder against a real database.

Step 9 of the rehearsal, and the claim it tests is the one that would be
easiest to fake: raw-row access as an agent is denied, and the aggregate over
the same table succeeds. Unit tests prove the gate decides correctly; this
proves the API cannot reach the rows without asking it.

Requires: make dev-up && uv run alembic upgrade head.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from gantry.api.aggregates import Aggregate, AggregateFunction, AggregateQuery
from gantry.api.datasets import Datasets
from gantry.core.dataset import AccessPolicy, AgentAccessPolicy
from gantry.policy.audit import AccessLog, InMemoryAccessLog, PostgresAccessLog
from gantry.policy.gate import AccessDeniedError
from gantry.policy.redaction import REDACTED
from gantry.policy.rules import AccessDefaults, AccessRules, Decision, QueryRules, SampleRules
from gantry.registry.memory import InMemoryDatasetRegistry
from gantry.state.database import create_engine, transaction
from gantry.state.tables import access_log
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)


@pytest.fixture
async def source() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(SOURCE_URL)
    try:
        async with transaction(engine) as connection:
            present = (
                await connection.execute(
                    text("SELECT to_regclass('public.request_logs') IS NOT NULL")
                )
            ).scalar_one()
        if not present:
            pytest.skip("scenario tables absent; seed request_logs and deploy_events")
        yield engine
    finally:
        await engine.dispose()


async def registered(
    source: AsyncEngine, *, sensitive: tuple[str, ...] = (), stance: AgentAccessPolicy | None = None
) -> InMemoryDatasetRegistry:
    """A registry holding the discovered request_logs manifest."""
    from gantry.adapters.source.postgres import PostgresSourceAdapter

    adapter = PostgresSourceAdapter(source)
    manifest = next(m for m in await adapter.discover() if m.name == "public.request_logs")
    manifest = await adapter.profile(manifest)
    updates: dict[str, object] = {"sensitive_fields": sensitive}
    if stance is not None:
        updates["access"] = AccessPolicy(agent_policy=stance)

    registry = InMemoryDatasetRegistry()
    await registry.register(manifest.model_copy(update=updates))
    return registry


def api(
    source: AsyncEngine,
    registry: InMemoryDatasetRegistry,
    rules: AccessRules | None = None,
    audit: AccessLog | None = None,
) -> Datasets:
    return Datasets(registry=registry, engine=source, rules=rules or AccessRules(), audit=audit)


def latency_query() -> AggregateQuery:
    return AggregateQuery(
        group_by=("svc",),
        aggregates=(
            Aggregate(function=AggregateFunction.AVG, field="latency_ms"),
            Aggregate(function=AggregateFunction.COUNT_DISTINCT, field="request_id"),
        ),
    )


async def test_the_default_policy_denies_rows_and_allows_the_aggregate(
    source: AsyncEngine,
) -> None:
    """The demo's step 9, on one Dataset, in one test."""
    datasets = api(source, await registered(source))

    with pytest.raises(AccessDeniedError) as denied:
        await datasets.sample("public.request_logs", limit=5, reason="looking at failures")
    assert "sample on public.request_logs: deny" in str(denied.value)

    outcome = await datasets.query("public.request_logs", latency_query())
    assert outcome.row_count > 0
    assert outcome.decision.decision is Decision.ALLOW
    assert "avg_latency_ms" in outcome.rows[0]


async def test_describe_and_profile_are_permitted_by_default(source: AsyncEngine) -> None:
    """An agent that cannot read a schema cannot form a narrower request."""
    datasets = api(source, await registered(source))

    described = await datasets.describe("public.request_logs")
    assert {field.name for field in described.dataset_schema.fields} >= {"latency_ms", "svc"}

    profiled = await datasets.profile("public.request_logs")
    assert profiled.statistics.row_count is not None


async def test_a_sample_is_permitted_masked_when_rows_are_allowed(source: AsyncEngine) -> None:
    rules = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
    datasets = api(source, await registered(source, sensitive=("request_id",)), rules)

    outcome = await datasets.sample("public.request_logs", limit=5, reason="investigating a spike")
    assert outcome.row_count == 5
    assert outcome.decision.decision is Decision.REDACT
    assert {row["request_id"] for row in outcome.rows} == {REDACTED}
    assert all(row["latency_ms"] is not None for row in outcome.rows)


async def test_a_sample_is_capped_at_the_policy_maximum(source: AsyncEngine) -> None:
    rules = AccessRules(
        default=AccessDefaults(rows=Decision.ALLOW), samples=SampleRules(max_rows=3)
    )
    datasets = api(source, await registered(source), rules)

    outcome = await datasets.sample("public.request_logs", limit=1000, reason="triage")
    assert outcome.row_count == 3, "the cap is applied to the query, not to the caller's request"


async def test_a_sample_without_a_reason_never_reaches_the_database(
    source: AsyncEngine,
) -> None:
    """Denial happens before the query is built, not after the rows come back."""
    rules = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
    datasets = api(source, await registered(source), rules)

    with pytest.raises(AccessDeniedError, match="requires a stated reason"):
        await datasets.sample("public.request_logs", limit=5)


async def test_a_dataset_marked_deny_refuses_even_its_description(source: AsyncEngine) -> None:
    datasets = api(source, await registered(source, stance=AgentAccessPolicy.DENY))

    with pytest.raises(AccessDeniedError):
        await datasets.describe("public.request_logs")


async def test_an_aggregate_over_a_sensitive_field_is_not_masked_when_it_quotes_nothing(
    source: AsyncEngine,
) -> None:
    """Counting a sensitive column is not printing it."""
    datasets = api(source, await registered(source, sensitive=("request_id",)))
    outcome = await datasets.query(
        "public.request_logs",
        AggregateQuery(
            group_by=("svc",),
            aggregates=(Aggregate(function=AggregateFunction.COUNT_DISTINCT, field="request_id"),),
        ),
    )
    assert outcome.decision.decision is Decision.ALLOW
    assert all(row["count_distinct_request_id"] != REDACTED for row in outcome.rows)


async def test_min_and_max_over_a_sensitive_field_are_masked(source: AsyncEngine) -> None:
    """Both return a value stored in the column, one bound at a time."""
    datasets = api(source, await registered(source, sensitive=("request_id",)))
    outcome = await datasets.query(
        "public.request_logs",
        AggregateQuery(
            group_by=("svc",),
            aggregates=(Aggregate(function=AggregateFunction.MIN, field="request_id"),),
        ),
    )
    assert outcome.decision.decision is Decision.REDACT
    assert {row["min_request_id"] for row in outcome.rows} == {REDACTED}


async def test_a_group_under_the_minimum_refuses_the_whole_result(source: AsyncEngine) -> None:
    """An aggregate over a unique key is row access wearing a GROUP BY. The
    whole result is refused rather than the small groups quietly dropped."""
    rules = AccessRules(queries=QueryRules(min_group_size=1_000_000))
    datasets = api(source, await registered(source), rules)

    with pytest.raises(AccessDeniedError, match="under the minimum"):
        await datasets.query("public.request_logs", latency_query())


async def test_a_query_naming_an_unknown_field_is_refused_before_it_is_built(
    source: AsyncEngine,
) -> None:
    """Checking against the manifest is also what stops a field name from
    carrying SQL: an identifier that is not in the schema never reaches one."""
    datasets = api(source, await registered(source))
    with pytest.raises(ValueError, match="has no field"):
        await datasets.query(
            "public.request_logs",
            AggregateQuery(
                group_by=('svc" FROM public.request_logs; --',),
                aggregates=(Aggregate(function=AggregateFunction.COUNT),),
            ),
        )


async def test_the_audit_trail_records_the_denial_and_the_grant(source: AsyncEngine) -> None:
    audit = InMemoryAccessLog(principal="analysis-agent")
    datasets = api(source, await registered(source), audit=audit)

    with pytest.raises(AccessDeniedError):
        await datasets.sample("public.request_logs", limit=5, reason="curiosity")
    await datasets.query("public.request_logs", latency_query())

    events = await audit.events(dataset="public.request_logs")
    assert [(e.rung, e.decision) for e in events] == [("sample", "deny"), ("query", "allow")]
    assert events[0].reason == "curiosity"
    assert events[0].grounds, "a denial with no grounds teaches nothing"
    assert all(e.principal == "analysis-agent" for e in events)


async def test_the_durable_trail_survives_the_process(source: AsyncEngine) -> None:
    meta = create_engine(META_URL)
    try:
        async with transaction(meta) as connection:
            await connection.execute(
                delete(access_log).where(access_log.c.principal == "durability-test")
            )

        audit = PostgresAccessLog(meta, principal="durability-test")
        datasets = api(source, await registered(source), audit=audit)
        with pytest.raises(AccessDeniedError):
            await datasets.records("public.request_logs", keys=["1"], reason="incident")

        # A second reader, as a later review would be.
        events = [
            event
            for event in await PostgresAccessLog(meta).events(dataset="public.request_logs")
            if event.principal == "durability-test"
        ]
        assert [e.decision for e in events] == ["deny"]
        assert events[0].rung == "records"
    finally:
        async with transaction(meta) as connection:
            await connection.execute(
                delete(access_log).where(access_log.c.principal == "durability-test")
            )
        await meta.dispose()
