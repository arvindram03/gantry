# SPDX-License-Identifier: Apache-2.0
"""The PostgreSQL adapter against a real PostgreSQL.

Every other test in this suite substitutes a stub adapter, which is the right
way to test the governance layer and no way at all to test SQL. `describe()`
shipped selecting `table_type` from `information_schema.columns`, where that
column does not exist, and nothing noticed because nothing ran it.

Skipped unless a database is reachable, so a clone without one still passes.
Point it somewhere with `GANTRY_TEST_POSTGRES_URL` — including at Neon or
Supabase, where the same tests are a useful check that the provider preset and
TLS settings are right. Set `GANTRY_REQUIRE_LIVE=1` in a job that is supposed
to have a database up, so an unreachable one fails loudly instead of skipping.
"""

from __future__ import annotations

import os

import gantry
import pytest

from _live import require_live_or_skip

URL = os.environ.get("GANTRY_TEST_POSTGRES_URL", "postgresql://gantry:gantry@localhost:5432/gantry")
PROVIDER = os.environ.get("GANTRY_TEST_POSTGRES_PROVIDER", "postgres")


def _connect() -> gantry.sql.SQLConnection:
    pytest.importorskip("asyncpg")
    return gantry.sql.connect(PROVIDER, url=URL)


async def _reachable() -> bool:
    """Is there actually a database there?

    Imported dynamically, the way the adapter itself does it: asyncpg ships no
    type information, and this test file is type-checked.
    """
    import importlib

    asyncpg = importlib.import_module("asyncpg")
    try:
        connection = await asyncpg.connect(URL, timeout=5)
    except Exception:
        return False
    await connection.close()
    return True


@pytest.fixture
async def db() -> gantry.sql.SQLConnection:
    connection = _connect()
    if not await _reachable():
        require_live_or_skip(f"no PostgreSQL at {URL.rsplit('@', 1)[-1]}")
    return connection


async def test_describe_reads_a_real_information_schema(
    db: gantry.sql.SQLConnection,
) -> None:
    """The query has to be valid against the engine, not merely plausible.

    A brand-new database legitimately has no user tables, and that is not a
    failure of the query — so it skips rather than asserting something about
    the database it was pointed at. Run `examples/seed.sql` first for the
    interesting version of this test.
    """
    schema = await db.describe()

    if not schema.tables:
        pytest.skip("no user tables here; run examples/seed.sql against this database")

    table = schema.tables[0]
    assert schema.schemas, "a table implies the schema it lives in"
    assert table.columns, "a table must come back with its columns"
    assert table.kind, "and its kind, which is what the broken query was reaching for"


async def test_a_read_only_query_returns_bounded_rows(
    db: gantry.sql.SQLConnection,
) -> None:
    query = db.query(read_only=True, max_rows=3, timeout=15)
    result = await query("SELECT generate_series(1, 100) AS n")

    assert result.inline is not None
    assert len(result.inline.rows) <= 3
    assert result.inline.truncated, "the row bound must be reported, not silently applied"


async def test_a_write_is_refused_before_it_reaches_the_database(
    db: gantry.sql.SQLConnection,
) -> None:
    """The refusal an agent will actually meet."""
    query = db.query(read_only=True)
    result = await query("CREATE TABLE gantry_should_not_exist (id int)")

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "read-only" in result.failure.message


async def test_explain_runs_against_the_engine(db: gantry.sql.SQLConnection) -> None:
    plan = await db.explain("SELECT 1")
    assert plan is not None


def test_the_transaction_pooler_rule_needs_no_database() -> None:
    """The statement-cache rule, checked without connecting to anything.

    It has to be unit-tested precisely because it cannot be trusted to a live
    check: whether the bug appears depends on which backend the pooler hands
    you, so a passing connection proves nothing about the rule being right.
    """
    from gantry.sql.adapters.postgres import _apply_transaction_pooling

    pooled: dict[str, object] = {}
    _apply_transaction_pooling(
        "supabase", "postgresql://u:p@aws-0-us-west-2.pooler.supabase.com:6543/postgres", pooled
    )
    assert pooled == {"statement_cache_size": 0}

    for provider, url in (
        # Session pooler and direct: a backend per client connection.
        ("supabase", "postgresql://u:p@aws-0-us-west-2.pooler.supabase.com:5432/postgres"),
        ("supabase", "postgresql://u:p@db.ref.supabase.co:5432/postgres"),
        # Neon's pooler carries prepared statements; measured, and not the same
        # question despite the identical shape.
        ("neon", "postgresql://u:p@ep-x-pooler.region.aws.neon.tech:6543/db"),
        ("postgres", "postgresql://u:p@localhost:6543/db"),
    ):
        untouched: dict[str, object] = {}
        _apply_transaction_pooling(provider, url, untouched)
        assert untouched == {}, f"{provider} {url} should not have been changed"

    explicit: dict[str, object] = {"statement_cache_size": 100}
    _apply_transaction_pooling(
        "supabase", "postgresql://u:p@aws-0-us-west-2.pooler.supabase.com:6543/postgres", explicit
    )
    assert explicit == {"statement_cache_size": 100}, "an explicit setting must win"
