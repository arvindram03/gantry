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

    docker compose -f examples/flink/docker-compose.yml up -d

Set GANTRY_TEST_FLINK_GATEWAY / GANTRY_TEST_FLINK_JOBMANAGER to point elsewhere.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import gantry
import pytest
from gantry.flink import FlinkMode, FlinkSQLArtifact

GATEWAY = os.environ.get("GANTRY_TEST_FLINK_GATEWAY", "http://localhost:18084")
JOBMANAGER = os.environ.get("GANTRY_TEST_FLINK_JOBMANAGER", "http://localhost:18081")
CATALOG = os.environ.get("GANTRY_TEST_FLINK_CATALOG", "pg")
DATABASE = os.environ.get("GANTRY_TEST_FLINK_DATABASE", "gantry")

SOURCE = f"`{CATALOG}`.`{DATABASE}`.`flink_src`"
SINK = f"`{CATALOG}`.`{DATABASE}`.`flink_sink`"


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
def flink() -> gantry.flink.FlinkConnection:
    if not _reachable(f"{GATEWAY}/info") or not _reachable(f"{JOBMANAGER}/overview"):
        pytest.skip(f"no Flink at {GATEWAY} / {JOBMANAGER}")
    if CATALOG not in _catalogs():
        pytest.skip(f"catalog {CATALOG!r} is not registered on the gateway")
    return gantry.flink.connect(
        GATEWAY,
        jobmanager_endpoint=JOBMANAGER,
        default_catalog=CATALOG,
        default_database=DATABASE,
        # A batch job takes longer to deploy than the 30s default allows.
        submission_timeout=180.0,
        request_timeout=180.0,
        validation_timeout=120.0,
    )


def _insert() -> FlinkSQLArtifact:
    return FlinkSQLArtifact(
        f"INSERT INTO {SINK} SELECT id, label FROM {SOURCE}",
        mode=FlinkMode.BATCH,
        declared_inputs=(f"{CATALOG}.{DATABASE}.flink_src",),
        declared_outputs=(f"{CATALOG}.{DATABASE}.flink_sink",),
    )


async def test_validation_uses_the_real_planner(
    flink: gantry.flink.FlinkConnection,
) -> None:
    """The statement has to be one the gateway accepts, not merely one that
    looks right. This is what the `executionTimeout` bug broke."""
    result = await flink.validate(_insert())
    assert result.ok, f"the real planner rejected it: {result.errors}"


async def test_the_planner_rejects_sql_it_cannot_plan(
    flink: gantry.flink.FlinkConnection,
) -> None:
    """A failing validation must come back as errors, not as an exception."""
    broken = FlinkSQLArtifact(
        f"INSERT INTO {SINK} SELECT no_such_column FROM {SOURCE}",
        mode=FlinkMode.BATCH,
        declared_outputs=(f"{CATALOG}.{DATABASE}.flink_sink",),
    )
    result = await flink.validate(broken)
    assert not result.ok
    assert result.errors


async def test_a_batch_job_runs_on_the_cluster(
    flink: gantry.flink.FlinkConnection,
) -> None:
    """A real job, submitted to a real JobManager, moving real rows.

    Everything below the API — session, statement, operation polling, job
    submission over the JobManager's REST endpoint — has to be right for this
    to pass, and none of it is exercised by a fake transport.
    """
    result = await flink.run(_insert(), timeout_seconds=300)
    assert result.status.value == "ACCEPTED", (
        f"the job did not succeed: {result.failure.message if result.failure else result.status}"
    )


async def test_the_v0_boundary_is_enforced_before_the_gateway_sees_it(
    flink: gantry.flink.FlinkConnection,
) -> None:
    """Multiple statements are refused by Gantry, not by Flink — so the refusal
    does not depend on a cluster being reachable at all."""
    result = await flink.validate(
        FlinkSQLArtifact(
            f"INSERT INTO {SINK} SELECT id, label FROM {SOURCE}; SELECT 1;",
            mode=FlinkMode.BATCH,
            declared_outputs=(f"{CATALOG}.{DATABASE}.flink_sink",),
        )
    )
    assert not result.ok
    assert any("exactly one SQL statement" in error for error in result.errors)
