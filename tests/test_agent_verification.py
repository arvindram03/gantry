# SPDX-License-Identifier: Apache-2.0
"""Trusted and agent-proposed verification form one additive contract."""

from __future__ import annotations

import inspect
import json
from pathlib import Path

import duckdb
import gantry
from gantry.failure import FailureKind
from gantry.result import ResultStatus
from gantry.runs.store import MemoryRunStore, _bundle_from_dict
from gantry.verifier import CheckSource


def _seeded(tmp_path: Path) -> gantry.sql.SQLConnection:
    path = tmp_path / "agent-verification.duckdb"
    native = duckdb.connect(str(path))
    native.execute("CREATE TABLE events AS SELECT 1 AS id UNION ALL SELECT 2")
    native.close()
    return gantry.sql.connect("duckdb", path=str(path), read_only=True)


async def test_runtime_checks_are_agent_commitments_and_are_recorded(tmp_path: Path) -> None:
    store = MemoryRunStore()
    gantry.runs.configure(store)
    query = _seeded(tmp_path).query(
        checks=[gantry.verify.row_count(max=10)],
    )

    result = await query(
        "SELECT id FROM events",
        verify=[gantry.verify.not_empty(), gantry.verify.required_columns(["id"])],
    )

    assert result.status is ResultStatus.ACCEPTED
    assert result.verification is not None
    assert [check.source for check in result.verification.checks] == [
        CheckSource.TRUSTED,
        CheckSource.AGENT,
        CheckSource.AGENT,
    ]
    assert result.evidence is not None
    assert result.evidence.proposal["agent_verification"] == [
        {"type": "not_empty"},
        {"type": "required_columns", "columns": ["id"]},
    ]
    assert store.get(result.evidence.run_id) is not None
    restored = _bundle_from_dict(json.loads(result.evidence.to_json()))
    assert len(restored.trusted_checks) == 1
    assert len(restored.agent_checks) == 2
    assert restored.agent_checks[0].source is CheckSource.AGENT


async def test_conflicting_count_contract_stops_before_execution(tmp_path: Path) -> None:
    query = _seeded(tmp_path).query(checks=[gantry.verify.row_count(max=1)])

    result = await query("SELECT id FROM events", verify=[gantry.verify.row_count(min=2)])

    assert result.status is ResultStatus.VERIFICATION_CONFLICT
    assert result.failure is not None
    assert result.failure.kind is FailureKind.VERIFICATION_CONFLICT
    assert result.handle is None


async def test_agent_cannot_spoof_provenance_or_request_provider_incompatible_check(
    tmp_path: Path,
) -> None:
    tool = _seeded(tmp_path).query().tool()

    spoofed = await tool.invoke(
        sql="SELECT id FROM events",
        verify=[{"type": "not_empty", "source": "trusted"}],
    )
    unsupported = await tool.invoke(sql="SELECT id FROM events", verify=[{"type": "running"}])

    assert spoofed.status is ResultStatus.REJECTED
    assert unsupported.status is ResultStatus.VERIFICATION_UNSUPPORTED
    assert unsupported.failure is not None
    assert unsupported.failure.kind is FailureKind.VERIFICATION_UNSUPPORTED


async def test_agent_check_that_cannot_be_measured_fails_as_unsupported(
    tmp_path: Path,
) -> None:
    query = _seeded(tmp_path).query(max_rows=1)

    result = await query("SELECT id FROM events", verify=[gantry.verify.row_count(min=1)])

    assert result.status is ResultStatus.VERIFICATION_UNSUPPORTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.VERIFICATION_UNSUPPORTED
    assert result.verification is not None
    assert result.verification.unsupported_checks[0].source is CheckSource.AGENT


def test_public_check_constructors_do_not_expose_source() -> None:
    for constructor in (
        gantry.verify.not_empty,
        gantry.verify.row_count,
        gantry.verify.required_columns,
        gantry.verify.null_rate,
    ):
        assert "source" not in inspect.signature(constructor).parameters
