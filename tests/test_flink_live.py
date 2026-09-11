# SPDX-License-Identifier: Apache-2.0
"""The Flink adapter against a real Flink cluster.

Every other Flink test substitutes a fake HTTP transport, which is the right way
to test the control-plane logic and no way at all to test what the SQL Gateway
accepts. Gantry sent `executionTimeout` on every statement, and Flink throws
`SqlGatewayService doesn't support timeout mechanism now` for any positive
value — so nothing worked against a real gateway while every fake-transport
test passed.

Skipped unless a gateway is reachable, so a clone without one still passes.
Bring one up with:

    docker compose -f examples/stack/docker-compose.yml up -d --wait

Set GANTRY_TEST_FLINK_GATEWAY / GANTRY_TEST_FLINK_JOBMANAGER to point
elsewhere. Set GANTRY_REQUIRE_LIVE=1 in a job that is supposed to have a
gateway up, so an unreachable one fails loudly instead of skipping.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import gantry
import pytest
from gantry.batch import BatchConnection
from gantry.flink.operation import FlinkBatchJob

from _live import require_live_or_skip

GATEWAY = os.environ.get("GANTRY_TEST_FLINK_GATEWAY", "http://localhost:8083")
JOBMANAGER = os.environ.get("GANTRY_TEST_FLINK_JOBMANAGER", "http://localhost:8081")
CATALOG = os.environ.get("GANTRY_TEST_FLINK_CATALOG", "pg")
DATABASE = os.environ.get("GANTRY_TEST_FLINK_DATABASE", "gantry")
DATABASE_SCHEMA = os.environ.get("GANTRY_TEST_FLINK_SCHEMA", "analytics")
REPORTING_SCHEMA = os.environ.get("GANTRY_TEST_FLINK_REPORTING", "reporting")

# The tables examples/seed.sql creates. A JDBC catalog exposes a PostgreSQL
# table as one identifier containing a dot, so the schema is quoted inside the
# name rather than as a separate part.
SOURCE = f"`{DATABASE_SCHEMA}.orders`"
SINK = f"`{REPORTING_SCHEMA}.orders_replica`"


def _reachable(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return bool(200 <= int(response.status) < 300)
    except (urllib.error.URLError, OSError, ValueError):
        return False


def _catalogs() -> set[str]:
    """Ask the gateway which catalogs it has, over its own REST API.

    The gateway accepts a statement immediately and finishes it later, so the
    status has to be polled before the result is read. Reading straight away
    returns an empty payload, which is indistinguishable from a gateway with no
    catalogs — and skips every test in this file for the wrong reason.
    """
    try:
        request = urllib.request.Request(
            f"{GATEWAY}/v2/sessions",
            method="POST",
            data=b"{}",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            session = json.load(response)["sessionHandle"]
        statement = urllib.request.Request(
            f"{GATEWAY}/v2/sessions/{session}/statements",
            method="POST",
            data=json.dumps({"statement": "SHOW CATALOGS"}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(statement, timeout=10) as response:
            operation = json.load(response)["operationHandle"]

        status_url = f"{GATEWAY}/v2/sessions/{session}/operations/{operation}/status"
        for _ in range(40):
            with urllib.request.urlopen(status_url, timeout=10) as response:
                state = json.load(response).get("status")
            if state in {"FINISHED", "ERROR"}:
                break
            time.sleep(0.25)

        url = f"{GATEWAY}/v2/sessions/{session}/operations/{operation}/result/0"
        with urllib.request.urlopen(url, timeout=10) as response:
            payload = json.load(response)
        return {row["fields"][0] for row in payload.get("results", {}).get("data", [])}
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        # Deliberately narrow. A blanket `except Exception` here turned a
        # NameError in this very function into "no catalogs", which skipped
        # every test in the file and looked exactly like a cluster that was not
        # running.
        return set()


@pytest.fixture
def flink() -> BatchConnection:
    if not _reachable(f"{GATEWAY}/info") or not _reachable(f"{JOBMANAGER}/overview"):
        require_live_or_skip(f"no Flink at {GATEWAY} / {JOBMANAGER}")
    if CATALOG not in _catalogs():
        require_live_or_skip(f"catalog {CATALOG!r} is not registered on the gateway")
    return gantry.batch.connect(
        "flink",
        endpoint=GATEWAY,
        jobmanager_endpoint=JOBMANAGER,
        default_catalog=CATALOG,
        default_database=DATABASE,
        # A batch job takes longer to deploy than the 30s default allows.
        submission_timeout=180.0,
        request_timeout=180.0,
        validation_timeout=120.0,
    )


@pytest.fixture
def job(flink: BatchConnection) -> FlinkBatchJob:
    return flink.job(
        inputs=[f"{DATABASE_SCHEMA}.orders"],
        outputs=[f"{REPORTING_SCHEMA}.orders_replica"],
        poll_interval=0.25,
        timeout=300,
    )


def _insert() -> str:
    return f"INSERT INTO {SINK} SELECT order_id, customer_id, region, amount FROM {SOURCE}"


async def test_validation_uses_the_real_planner(
    job: FlinkBatchJob,
) -> None:
    """The statement has to be one the gateway accepts, not merely one that
    looks right. This is what the `executionTimeout` bug broke."""
    result = await job.validate(_insert())
    assert result.ok, f"the real planner rejected it: {result.errors}"


async def test_the_planner_rejects_sql_it_cannot_plan(
    job: FlinkBatchJob,
) -> None:
    """A failing validation must come back as errors, not as an exception."""
    broken = f"INSERT INTO {SINK} SELECT no_such_column FROM {SOURCE}"
    result = await job.validate(broken)
    assert not result.ok
    assert result.errors


async def test_a_batch_job_runs_on_the_cluster(
    job: FlinkBatchJob,
) -> None:
    """A real job, submitted to a real JobManager, moving real rows.

    Everything below the API — session, statement, operation polling, job
    submission over the JobManager's REST endpoint — has to be right for this
    to pass, and none of it is exercised by a fake transport.
    """
    result = await job(_insert())
    assert result.status.value == "ACCEPTED", (
        f"the job did not succeed: {result.failure.message if result.failure else result.status}"
    )


async def test_the_v0_boundary_is_enforced_before_the_gateway_sees_it(
    job: FlinkBatchJob,
) -> None:
    """Multiple statements are refused by Gantry, not by Flink — so the refusal
    does not depend on a cluster being reachable at all."""
    result = await job.validate(f"INSERT INTO {SINK} SELECT id, label FROM {SOURCE}; SELECT 1;")
    assert not result.ok
    assert any("exactly one SQL statement" in error for error in result.errors)


# --------------------------------------------------------------------------
# The paths only a real engine proves. Each of these covers a bug that shipped
# because a fake transport answered whatever it was asked.


def _runtime() -> object:
    from gantry.flink.api import FlinkRuntime
    from gantry.flink.target import FlinkTarget

    return FlinkRuntime(
        FlinkTarget(
            GATEWAY,
            {
                "jobmanager_endpoint": JOBMANAGER,
                "default_catalog": CATALOG,
                "default_database": DATABASE,
                "request_timeout": 120.0,
            },
        )
    )


async def test_a_result_is_read_past_its_first_page(flink: BatchConnection) -> None:
    """`SELECT COUNT(*)` arrives on the second page, not the first.

    Page 0 comes back NOT_READY, the rows follow on page 1, EOS on page 2 —
    and the rows are a changelog, so the count builds up 1, 2, 3 … rather than
    arriving whole. Reading one page and taking the first row returned 1 for
    any non-empty table: a plausible number, and always wrong.
    """
    runtime = _runtime()
    table = await runtime.inspect_table(  # type: ignore[attr-defined]
        f"{DATABASE_SCHEMA}.orders", include_row_count=True
    )
    assert table is not None, f"{DATABASE_SCHEMA}.orders should exist; run examples/seed.sql"
    rows = table.metadata.get("rows")
    assert isinstance(rows, int)
    assert rows > 1, "a paged changelog read as one page reports 1"


async def test_a_schema_qualified_table_can_be_inspected(
    flink: BatchConnection,
) -> None:
    """A JDBC catalog exposes `analytics.orders` as one identifier containing a
    dot. Quoting it as two named a database that does not exist, so every table
    outside `public` was uninspectable — and `row_count` on it could never
    pass."""
    runtime = _runtime()
    table = await runtime.inspect_table(f"{DATABASE_SCHEMA}.orders")  # type: ignore[attr-defined]
    assert table is not None
    assert {column.name for column in table.columns} >= {"order_id", "status", "amount"}


async def test_a_missing_table_is_absent_rather_than_an_error(
    flink: BatchConnection,
) -> None:
    """Trying both spellings must not turn "no such table" into a raised
    exception, or a verification check cannot distinguish a missing
    destination from a broken connection."""
    runtime = _runtime()
    assert await runtime.inspect_table("analytics.no_such_table_here") is None  # type: ignore[attr-defined]


async def test_running_job_metrics_reach_the_health_checks(
    flink: BatchConnection,
) -> None:
    """Restart count is derived by the adapter and appears only on the metrics
    it returns. The health path rebuilt them from the execution's generic
    metrics instead, found nothing, and failed every check that needed one on a
    perfectly healthy job."""
    stream = gantry.stream.connect(
        "flink",
        endpoint=GATEWAY,
        jobmanager_endpoint=JOBMANAGER,
        default_catalog=CATALOG,
        default_database=DATABASE,
        submission_timeout=180.0,
        request_timeout=180.0,
    )
    job = stream.job(
        inputs=[f"{DATABASE_SCHEMA}.orders"],
        outputs=[f"{REPORTING_SCHEMA}.orders_replica"],
        checks=[gantry.verify.running()],
        timeout=180,
    )
    result = await job(
        f"INSERT INTO `{REPORTING_SCHEMA}.orders_replica` "
        f"SELECT order_id, customer_id, region, amount FROM `{DATABASE_SCHEMA}.orders`"
    )
    if result.execution is None or result.execution.state.value != "RUNNING":
        pytest.skip("the job finished before it could be observed running")
    if not result.metrics.native.get("job"):
        # Flink registers job metrics a moment after the job reaches RUNNING,
        # so the earliest observation can legitimately have none. Skipping is
        # honest here; asserting would make this fail about one run in three
        # for a reason that is not the one under test.
        pytest.skip("Flink had not registered job metrics yet")
    assert result.metrics.restart_count is not None, (
        "a running job must report a restart count; None means the health "
        "checks are reading metrics from the wrong object"
    )
