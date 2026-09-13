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
from gantry.runs.status import RunStatus

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


async def _raw_execute(*statements: str) -> None:
    """Arrange and clean up with a connection Gantry knows nothing about.

    Not through `db.query`: `DROP TABLE IF EXISTS x` classifies its table as
    `IF` — the object regex reads the word after `TABLE` — so any allow-list
    refuses it. That is the conservative classifier over-refusing, which is the
    safe direction, but it makes the governed path the wrong tool for setting
    up a test.
    """
    import importlib

    asyncpg = importlib.import_module("asyncpg")
    connection = await asyncpg.connect(URL, timeout=5)
    try:
        for statement in statements:
            await connection.execute(statement)
    finally:
        await connection.close()


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
    assert len(result.rows) <= 3
    assert result.truncated, "the row bound must be reported, not silently applied"


async def test_a_write_is_refused_before_it_reaches_the_database(
    db: gantry.sql.SQLConnection,
) -> None:
    """The refusal an agent will actually meet."""
    query = db.query(read_only=True)
    result = await query("CREATE TABLE gantry_should_not_exist (id int)")

    assert result.status is RunStatus.POLICY_REJECTED
    assert result.failure is not None
    assert "read-only" in result.failure.message


async def test_explain_runs_against_the_engine(db: gantry.sql.SQLConnection) -> None:
    plan = await db.explain("SELECT 1")
    assert plan is not None


async def test_materialize_creates_verifies_and_refuses_to_repeat(
    db: gantry.sql.SQLConnection,
) -> None:
    """Materialization for PostgreSQL, which was "not yet enabled" until now.

    Nothing about the engine prevented it — `CREATE TABLE AS` is explainable
    and its DDL is transactional. The adapter simply never declared the
    capabilities or implemented `inspect_table`, so every attempt was refused
    with "adapter does not support CREATE TABLE AS".
    """
    import gantry.verify

    schema = await db.describe()
    if not any(table.schema == "analytics" for table in schema.tables):
        pytest.skip("no analytics schema here; run examples/seed.sql first")

    await _raw_execute("DROP TABLE IF EXISTS reporting.live_rollup")

    build = db.materialize(
        sources=["analytics.*"],
        destinations=["reporting.*"],
        verify=[gantry.verify.destination_exists(), gantry.verify.row_count(min=1)],
    )
    sql = (
        "CREATE TABLE reporting.live_rollup AS "
        "SELECT status, count(*) AS orders FROM analytics.orders GROUP BY status"
    )

    result = await build(sql)

    assert result.status is RunStatus.ACCEPTED, result.failure
    assert result.uri == "postgres://reporting/live_rollup"
    checks = {
        check.name: check for check in (result.verification.checks if result.verification else ())
    }
    assert checks["destination_exists"].ok
    # Reads `rows` from the table metadata, which is the key `RowCount` uses.
    assert checks["row_count"].ok and isinstance(checks["row_count"].actual, int)

    repeat = await build(sql)
    assert repeat.status is RunStatus.POLICY_REJECTED
    assert "already exists" in (repeat.failure.message if repeat.failure else "")

    await _raw_execute("DROP TABLE IF EXISTS reporting.live_rollup")


async def test_a_view_is_validated_by_dry_run_and_leaves_nothing_behind(
    db: gantry.sql.SQLConnection,
) -> None:
    """`EXPLAIN CREATE VIEW` is a syntax error in PostgreSQL.

    So a view is validated by running it in a transaction and rolling back,
    which resolves every name in the definition. This asserts both halves: a
    view over a missing table is refused at admission, and the refused
    definition is not left behind by the dry run itself.
    """
    await _raw_execute("DROP VIEW IF EXISTS reporting.live_view")

    build = db.materialize(sources=["analytics.*"], destinations=["reporting.*"])

    refused = await build(
        "CREATE VIEW reporting.live_view AS SELECT * FROM analytics.no_such_table"
    )
    assert refused.status is RunStatus.POLICY_REJECTED
    assert "no_such_table" in (refused.failure.message if refused.failure else "")

    absent = db.query(schemas=["information_schema"])
    found = await absent(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'reporting' AND table_name = 'live_view'"
    )
    assert found.rows[0][0] == 0, "the dry run left the view behind"

    created = await build("CREATE VIEW reporting.live_view AS SELECT status FROM analytics.orders")
    assert created.status is RunStatus.ACCEPTED, created.failure
    assert created.uri == "postgres://reporting/live_view"

    await _raw_execute("DROP VIEW IF EXISTS reporting.live_view")


async def test_inspect_table_reports_a_missing_destination_as_missing(
    db: gantry.sql.SQLConnection,
) -> None:
    """Create-only rests on this telling absence from emptiness."""
    from gantry.sql.adapters.postgres import PostgresAdapter
    from gantry.sql.materialization import TableRef
    from gantry.sql.target import SQLTarget

    target = SQLTarget(PROVIDER, "postgres", "postgres", {"url": URL})
    adapter = PostgresAdapter(target)

    assert await adapter.inspect_table(TableRef("definitely_not_here", "reporting"), target) is None

    orders = await adapter.inspect_table(
        TableRef("orders", "analytics"), target, include_row_count=True
    )
    if orders is None:
        pytest.skip("no analytics.orders here; run examples/seed.sql first")
    assert orders.schema == "analytics"
    assert orders.kind == "base table"
    assert isinstance(orders.metadata["rows"], int)


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


async def test_validating_a_create_table_as_does_not_execute_its_body(
    db: gantry.sql.SQLConnection,
) -> None:
    """`EXPLAIN` for a CTAS, not a dry run — the difference is doing the work twice.

    Validating a `CREATE TABLE AS` by running and rolling it back would produce
    a correct answer and copy every row for nothing, silently doubling the cost
    of every materialization. Nothing about the result distinguishes the two, so
    this measures a side effect instead.

    A sequence is the instrument: `nextval` is not rolled back in PostgreSQL, so
    it still advances inside an aborted transaction, while `EXPLAIN` never calls
    it at all. If the sequence moves, the body ran.
    """
    await _raw_execute(
        "DROP TABLE IF EXISTS reporting.seq_probe",
        "DROP SEQUENCE IF EXISTS reporting.validate_probe CASCADE",
        "CREATE SEQUENCE reporting.validate_probe",
        # Primed, because `last_value` on a never-called sequence reads 1 —
        # the same as after one call — so an unprimed baseline cannot tell one
        # execution from none.
        "SELECT nextval('reporting.validate_probe')",
    )
    reader = db.query(schemas=["reporting"])

    async def sequence_value() -> int:
        result = await reader("SELECT last_value FROM reporting.validate_probe")
        assert result.inline is not None, result.failure
        value = result.rows[0][0]
        assert isinstance(value, int)
        return value

    before = await sequence_value()
    build = db.materialize(sources=["reporting.*"], destinations=["reporting.*"])

    await build(
        "CREATE TABLE reporting.seq_probe AS SELECT nextval('reporting.validate_probe') AS n"
    )

    # The statement itself runs once, so the sequence advances once. Twice means
    # validation executed the body as well.
    assert await sequence_value() - before <= 1, (
        "validation executed the CREATE TABLE AS body; it should be EXPLAINed"
    )
    await _raw_execute(
        "DROP TABLE IF EXISTS reporting.seq_probe",
        "DROP SEQUENCE IF EXISTS reporting.validate_probe CASCADE",
    )


def test_only_views_are_validated_by_dry_run() -> None:
    """Which statements skip EXPLAIN, without needing a database.

    Getting this wrong in the permissive direction means `EXPLAIN CREATE VIEW`
    and a rejected materialization; getting it wrong in the other means
    dry-running a `CREATE TABLE AS`, which copies every row twice.
    """
    from gantry.sql.adapters.postgres import _explainable

    assert _explainable("CREATE TABLE reporting.x AS SELECT 1")
    assert _explainable("SELECT 1")
    assert _explainable("  select * from t")
    assert not _explainable("CREATE VIEW reporting.v AS SELECT 1")
    assert not _explainable("create or replace view v as select 1")
    assert not _explainable("\n  CREATE RECURSIVE VIEW v(a) AS SELECT 1")
    # A materialized view is explainable in PostgreSQL, unlike a plain one.
    assert _explainable("CREATE MATERIALIZED VIEW m AS SELECT 1")


def test_a_materialization_is_reported_as_a_reference_to_its_destination() -> None:
    from gantry.output import OutputKind
    from gantry.sql.adapters.postgres import _destination_output

    handle = gantry.ExecutionHandle(
        "run_1",
        "sql",
        "postgres",
        "postgres_1",
        metadata={
            "gantry.sql.materialization.destination": "reporting.rollup",
            "gantry.sql.materialization.operation": "CREATE_VIEW_AS",
        },
    )

    outputs = _destination_output(handle)

    assert len(outputs) == 1
    assert outputs[0].kind is OutputKind.TABLE
    # Slashes: `_materialized_output` matches the destination in that form.
    assert outputs[0].uri == "postgres://reporting/rollup"
    assert outputs[0].metadata["object_kind"] == "view"


def test_an_ordinary_query_reports_no_destination() -> None:
    from gantry.sql.adapters.postgres import _destination_output

    assert _destination_output(gantry.ExecutionHandle("r", "sql", "postgres", "p")) == ()


def test_metadata_lookups_quote_and_escape_what_they_interpolate() -> None:
    from gantry.sql.adapters.postgres import _escaped, _quoted, _schema_filter
    from gantry.sql.materialization import TableRef

    assert _escaped("o'brien") == "o''brien"
    assert _quoted('we"ird') == '"we""ird"'
    assert _schema_filter(TableRef("t")) == ""
    assert _schema_filter(TableRef("t", "analytics")) == "AND c.table_schema = 'analytics'"
    assert _schema_filter(TableRef("t", "an'alytics")) == "AND c.table_schema = 'an''alytics'"


async def test_null_rate_catches_a_join_that_row_count_calls_healthy(
    db: gantry.sql.SQLConnection,
) -> None:
    """The failure `row_count` cannot see.

    A query that runs, produces the expected number of rows, and joins wrongly,
    so the column everything downstream keys on is null in most of them. The
    count says the table is fine. Measured against the real engine, because the
    null rate has to be observed at the destination rather than reported by the
    statement that wrote it.
    """
    import gantry.verify

    await _raw_execute("DROP TABLE IF EXISTS reporting.null_probe")
    build = db.materialize(
        sources=["analytics.*"],
        destinations=["reporting.*"],
        verify=[
            gantry.verify.row_count(min=1),
            gantry.verify.null_rate(column="customer_id", max=0.01),
        ],
    )

    result = await build(
        "CREATE TABLE reporting.null_probe AS "
        "SELECT o.order_id, c.customer_id FROM analytics.orders o "
        "LEFT JOIN analytics.customers c ON c.customer_id = o.order_id"
    )

    assert result.status is RunStatus.REJECTED
    checks = {
        check.name: check for check in (result.verification.checks if result.verification else ())
    }
    assert checks["row_count"].ok, "the row count is fine, which is the point"
    assert not checks["null_rate"].ok
    assert checks["null_rate"].supported, "the provider measured it; it simply failed"
    observed = checks["null_rate"].actual
    assert isinstance(observed, dict) and observed["value"] > 0.5
    await _raw_execute("DROP TABLE IF EXISTS reporting.null_probe")


async def test_a_run_is_readable_from_another_process_with_only_its_id(
    db: gantry.sql.SQLConnection, tmp_path: object
) -> None:
    """Criterion 9: understood without the agent conversation that produced it.

    The store is opened twice over the same file, the second time after the
    first is closed, because "it is still in memory" is not the property being
    tested.
    """
    import gantry.verify
    from gantry.runs.store import SQLiteRunStore

    await _raw_execute("DROP TABLE IF EXISTS reporting.evidence_probe")
    build = db.materialize(
        sources=["analytics.*"],
        destinations=["reporting.*"],
        verify=[gantry.verify.destination_exists(), gantry.verify.row_count(min=1)],
    )

    result = await build(
        "CREATE TABLE reporting.evidence_probe AS SELECT customer_id FROM analytics.customers"
    )
    assert result.status is RunStatus.ACCEPTED, result.failure
    assert result.evidence is not None

    path = f"{tmp_path}/runs.db"
    writer = SQLiteRunStore(path)
    writer.create(result)
    writer.close()

    reader = SQLiteRunStore(path)
    try:
        run = reader.get(result.id)
        assert run is not None
        assert run.status is RunStatus.ACCEPTED
        assert [r.resource for r in run.inputs] == ["analytics.customers"]
        assert run.uri == "postgres://reporting/evidence_probe"
        assert run.execution is not None and run.execution.native_id is not None
        assert run.proposal is not None and run.proposal.hash
        rendered = run.render()
        assert "✓ row_count" in rendered
        assert "analytics.customers" in rendered
    finally:
        reader.close()
    await _raw_execute("DROP TABLE IF EXISTS reporting.evidence_probe")


async def _tables(schema: str) -> set[str]:
    """What PostgreSQL itself says exists, through a connection Gantry never saw."""
    import importlib

    asyncpg = importlib.import_module("asyncpg")
    connection = await asyncpg.connect(URL, timeout=5)
    try:
        rows = await connection.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = $1", schema
        )
    finally:
        await connection.close()
    return {str(row["tablename"]) for row in rows}


async def test_a_policy_refusal_never_reaches_postgres() -> None:
    """A denied materialization leaves no table, and a denied read runs nothing.

    Asserted against `pg_tables` rather than against the run: the run could be
    wrong about this in a way no assertion on the run would catch, and the
    table either exists in the database or it does not.
    """
    from gantry.actor import actor, context

    pytest.importorskip("asyncpg")
    if not await _reachable():
        require_live_or_skip(f"no PostgreSQL at {URL}")
    await _raw_execute(
        "CREATE SCHEMA IF NOT EXISTS policy_raw",
        "CREATE SCHEMA IF NOT EXISTS policy_scratch",
        "CREATE SCHEMA IF NOT EXISTS policy_prod",
        "DROP TABLE IF EXISTS policy_raw.invoices",
        "DROP TABLE IF EXISTS policy_scratch.totals",
        "DROP TABLE IF EXISTS policy_prod.totals",
        "CREATE TABLE policy_raw.invoices (customer_id int, balance int)",
        "INSERT INTO policy_raw.invoices VALUES (1, 10), (2, 20)",
    )

    policy = gantry.Policy(
        name="agent-materialization",
        rules=[
            gantry.allow.query(actors=["etl-agent"], sources=["policy_raw.*"]),
            gantry.allow.materialize(
                actors=["etl-agent"],
                sources=["policy_raw.*"],
                destinations=["policy_scratch.*"],
            ),
            gantry.deny.materialize(destinations=["policy_prod.*"]),
        ],
    )
    db = gantry.sql.connect(PROVIDER, url=URL, policy=policy)
    query = db.query(schemas=["policy_raw", "policy_prod"], max_rows=10, timeout=15)
    materialize = db.materialize(
        sources=["policy_raw.*"],
        destinations=["policy_scratch.*", "policy_prod.*"],
        timeout=60,
    )
    body = (
        "SELECT customer_id, SUM(balance) AS balance FROM policy_raw.invoices GROUP BY customer_id"
    )

    with context(actor=actor("agent", "etl-agent"), environment="prod"):
        read = await query("SELECT customer_id FROM policy_raw.invoices")
        allowed = await materialize(f"CREATE TABLE policy_scratch.totals AS {body}")
        refused = await materialize(f"CREATE TABLE policy_prod.totals AS {body}")
    with context(actor=actor("agent", "rogue-agent")):
        wrong_actor = await query("SELECT customer_id FROM policy_raw.invoices")

    # The engine's own state first: it is the claim the rest of the run only
    # describes, and a record can be wrong about it in a way no assertion on the
    # record would catch.
    assert await _tables("policy_scratch") == {"totals"}
    assert await _tables("policy_prod") == set(), "a denied write must leave nothing behind"

    assert read.status is RunStatus.ACCEPTED
    assert read.admission is not None
    assert read.admission.policy == "agent-materialization"
    assert read.admission.policy_hash == policy.version

    assert allowed.status is RunStatus.ACCEPTED
    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.execution is None
    assert refused.admission is not None
    assert refused.admission.codes == ("DESTINATION_DENIED",)

    assert wrong_actor.status is RunStatus.POLICY_REJECTED
    assert wrong_actor.execution is None
    assert wrong_actor.admission is not None
    assert wrong_actor.admission.codes == ("ACTOR_DENIED",)


async def test_confirmation_parks_a_production_write_until_the_host_answers() -> None:
    """Policy allows it; a rule asks that the user be told. Asked of `pg_tables`.

    The three states have to be distinguishable in the database, not just in the
    run: nothing while parked, the table once confirmed, and nothing at all for
    the one that was declined.
    """
    from gantry.actor import actor, context
    from gantry.confirmation import ConfirmationStatus

    pytest.importorskip("asyncpg")
    if not await _reachable():
        require_live_or_skip(f"no PostgreSQL at {URL}")
    await _raw_execute(
        "CREATE SCHEMA IF NOT EXISTS confirm_raw",
        "CREATE SCHEMA IF NOT EXISTS confirm_prod",
        "DROP TABLE IF EXISTS confirm_raw.invoices",
        "DROP TABLE IF EXISTS confirm_prod.totals",
        "DROP TABLE IF EXISTS confirm_prod.declined",
        "CREATE TABLE confirm_raw.invoices (customer_id int, balance int)",
        "INSERT INTO confirm_raw.invoices VALUES (1, 10), (2, 20)",
    )

    policy = gantry.Policy(
        name="prod-data-agents",
        rules=[
            gantry.allow.materialize(
                sources=["confirm_raw.*"],
                destinations=["confirm_prod.*"],
                require_confirmation=True,
                confirmation_code="PRODUCTION_WRITE",
                confirmation_message="This will write to production data.",
            )
        ],
    )
    db = gantry.sql.connect(PROVIDER, url=URL, policy=policy)
    materialize = db.materialize(
        sources=["confirm_raw.*"],
        destinations=["confirm_prod.*"],
        timeout=60,
        checks=[gantry.verify.destination_exists(), gantry.verify.row_count(min=1)],
    )
    body = (
        "SELECT customer_id, SUM(balance) AS balance FROM confirm_raw.invoices GROUP BY customer_id"
    )

    with context(actor=actor("agent", "migration-agent"), environment="prod"):
        parked = await materialize(f"CREATE TABLE confirm_prod.totals AS {body}")
        to_decline = await materialize(f"CREATE TABLE confirm_prod.declined AS {body}")
    while_waiting = await _tables("confirm_prod")

    confirmed = await gantry.runs.confirm(parked.id, metadata={"channel": "cli"})
    declined = await gantry.runs.decline(to_decline.id)
    after = await _tables("confirm_prod")

    assert while_waiting == set(), "nothing may exist in PostgreSQL while confirmation is pending"
    assert after == {"totals"}

    assert parked.status is RunStatus.AWAITING_CONFIRMATION
    assert parked.execution is None
    assert parked.admission is not None and parked.admission.allowed
    assert parked.confirmation is not None
    assert parked.confirmation.codes == ("PRODUCTION_WRITE",)

    assert confirmed.id == parked.id
    assert confirmed.status is RunStatus.ACCEPTED
    assert confirmed.confirmation is not None
    assert confirmed.confirmation.status is ConfirmationStatus.CONFIRMED
    assert confirmed.verification is not None and confirmed.verification.ok

    assert declined.status is RunStatus.CONFIRMATION_DECLINED
    assert declined.execution is None
