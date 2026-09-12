# SPDX-License-Identifier: Apache-2.0
"""The evidence model: bounded, serializable, and able to come back.

Evidence that cannot be read back is a log line. These tests are mostly about
the round trip, because that is the property the rest of the design rests on —
a run has to be understandable later, by someone else, without the process that
produced it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from gantry.actor import actor
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.runs.model import OperationKind, ResourceRef, Run, render
from gantry.runs.sqlite import SQLiteRunStore
from gantry.runs.sqlite import evidence_from_dict as _bundle_from_dict
from gantry.runs.status import RunStatus
from gantry.runs.store import MemoryRunStore
from gantry.verifier import CheckResult, VerificationResult

from _runs import make_run


def _bundle(**overrides: Any) -> EvidenceBundle:
    started = datetime(2026, 9, 12, 3, 0, tzinfo=UTC)
    defaults: dict[str, Any] = {
        "run_id": "run_123",
        "engine": "sql",
        "operation": "CREATE_TABLE_AS",
        "decision": "ACCEPTED",
        "native_execution_id": "job_xyz",
        "proposal_hash": "a" * 64,
        "inputs": ("raw.orders",),
        "outputs": ("postgres://analytics/customer_metrics",),
        "started_at": started,
        "finished_at": started + timedelta(milliseconds=18342),
        "execution": {"status": "SUCCEEDED"},
        "observations": (
            Observation("rows_read", 1_190_432, ObservationSource.ENGINE, unit="rows"),
            Observation("row_count", 1_190_432, ObservationSource.OUTPUT, unit="rows"),
        ),
        "checks": (
            CheckResult(
                "row_count", True, {"min": 1_000_000}, {"value": 1_190_432}, source="postgres"
            ),
        ),
    }
    return EvidenceBundle(**{**defaults, **overrides})


def _run(**overrides: Any) -> Run:
    """A run carrying the bundle above, for the store tests."""
    defaults: dict[str, Any] = {
        "id": "run_123",
        "actor": actor("agent", "test-agent"),
        "kind": OperationKind.MATERIALIZE,
        "provider": "postgres",
        "inputs": (ResourceRef(system="postgres", resource="raw.orders"),),
        "outputs": (ResourceRef(system="postgres", resource="analytics.customer_metrics"),),
        "evidence": _bundle(),
        "verification": VerificationResult(ok=True, checks=_bundle().checks),
    }
    return make_run(**{**defaults, **overrides})


def test_a_bundle_is_json_and_carries_both_sides_of_every_check() -> None:
    """Structured, not prose: something downstream has to compare the numbers."""
    payload = json.loads(_bundle().to_json())

    assert payload["run_id"] == "run_123"
    assert payload["duration_ms"] == 18342
    check = payload["checks"][0]
    assert check == {
        "check": "row_count",
        "passed": True,
        "expected": {"min": 1_000_000},
        "observed": {"value": 1_190_432},
        "source": "postgres",
    }


def test_every_observation_keeps_the_source_that_produced_it() -> None:
    """An engine's word for its own runtime is weaker than a measured count."""
    payload = json.loads(_bundle().to_json())

    sources = {item["name"]: item["source"] for item in payload["observations"]}
    assert sources == {"rows_read": "engine", "row_count": "output"}


def test_a_bundle_survives_a_round_trip_through_json() -> None:
    original = _bundle()

    restored = _bundle_from_dict(json.loads(original.to_json()))
    assert restored is not None

    assert restored.run_id == original.run_id
    assert restored.inputs == original.inputs
    assert restored.outputs == original.outputs
    assert restored.started_at == original.started_at
    assert restored.duration_ms == original.duration_ms
    assert restored.proposal_hash == original.proposal_hash
    assert [c.name for c in restored.checks] == [c.name for c in original.checks]
    assert restored.checks[0].expected == original.checks[0].expected
    assert restored.checks[0].source == "postgres"
    assert [o.source for o in restored.observations] == [
        ObservationSource.ENGINE,
        ObservationSource.OUTPUT,
    ]


def test_an_unserializable_value_is_degraded_rather_than_lost() -> None:
    """Evidence that raises on `json.dumps` is not evidence.

    A value this model does not recognise is rendered as its string form, which
    is worse than a typed value and much better than an exception at the moment
    someone tries to record why a run was accepted.
    """

    class Odd:
        def __str__(self) -> str:
            return "odd-value"

    bundle = _bundle(observations=(Observation("thing", Odd(), ObservationSource.GANTRY),))

    payload = json.loads(bundle.to_json())

    assert payload["observations"][0]["value"] == "odd-value"


def test_the_newest_observation_of_a_name_wins() -> None:
    earlier = Observation("row_count", 1, ObservationSource.OUTPUT)
    later = Observation("row_count", 2, ObservationSource.OUTPUT)

    bundle = _bundle(observations=(earlier, later))

    assert bundle.observation("row_count") is later
    assert bundle.observation("absent") is None


def test_an_observation_needs_a_name() -> None:
    with pytest.raises(ValueError, match="name"):
        Observation("  ", 1, ObservationSource.ENGINE)


def test_duration_needs_both_ends() -> None:
    assert _bundle(finished_at=None).duration_ms is None
    assert _bundle(started_at=None).duration_ms is None


def test_a_run_renders_for_a_person_without_losing_the_numbers() -> None:
    """The check block is the point: expected beside observed.

    A reader who can only see a verdict can disagree with the verdict. A reader
    who can see the bound and the measurement can disagree with the bound.
    """
    text = render(_run())

    assert "Run run_123" in text
    assert "agent:test-agent" in text
    assert "materialize" in text
    assert "raw.orders" in text
    assert "analytics.customer_metrics" in text
    assert "✓ row_count" in text
    assert "expected: min 1000000" in text
    assert "observed: value 1190432" in text


def test_an_unsupported_check_renders_differently_from_a_failed_one() -> None:
    """ "Could not measure" and "measured and it was wrong" are different facts."""
    failed = _run(
        status=RunStatus.VERIFICATION_UNSUPPORTED,
        verification=VerificationResult(
            ok=False,
            checks=(
                CheckResult("row_count", False, {"min": 1}, {"value": 0}, "too few rows"),
                CheckResult(
                    "null_rate", False, {"max": 0.01}, None, "unavailable", supported=False
                ),
            ),
        ),
    )

    text = render(failed)

    assert "✗ row_count" in text
    assert "? null_rate" in text


def test_a_memory_store_returns_what_it_was_given() -> None:
    store = MemoryRunStore()

    store.create(_run())

    assert store.get("run_123") is not None
    assert store.get("absent") is None
    assert [run.id for run in store.recent()] == ["run_123"]


def test_a_memory_store_filters_by_decision() -> None:
    store = MemoryRunStore()
    store.create(_run())
    store.create(_run(id="run_456", status=RunStatus.REJECTED))

    rejected = store.recent(status="REJECTED")

    assert [run.id for run in rejected] == ["run_456"]


def test_a_sqlite_store_outlives_the_object_that_wrote_it(tmp_path: object) -> None:
    """The invariant the whole feature rests on.

    Not "the store works" but "the evidence is still there once everything that
    produced it is gone", which is why this opens a second store over the same
    file rather than reusing the first.
    """
    path = f"{tmp_path}/runs.db"
    writer = SQLiteRunStore(path)
    writer.create(_run())
    writer.close()

    reader = SQLiteRunStore(path)
    try:
        run = reader.get("run_123")
        assert run is not None
        assert run.status is RunStatus.ACCEPTED
        assert [r.resource for r in run.inputs] == ["raw.orders"]
        assert run.verification is not None
        assert "✓ row_count" in run.render()
    finally:
        reader.close()


def test_recording_the_same_run_twice_updates_rather_than_duplicates(
    tmp_path: object,
) -> None:
    """A stream is observed more than once, and is one run each time."""
    store = SQLiteRunStore(f"{tmp_path}/runs.db")
    try:
        store.create(_run())
        store.create(_run(status=RunStatus.REJECTED))

        assert len(store.recent()) == 1
        run = store.get("run_123")
        assert run is not None and run.status is RunStatus.REJECTED
    finally:
        store.close()


def test_a_run_without_evidence_still_records() -> None:
    """A refused proposal has no evidence and is still a run worth keeping."""
    store = MemoryRunStore()

    store.create(_run(evidence=None, status=RunStatus.POLICY_REJECTED))

    recorded = store.get("run_123")
    assert recorded is not None
    assert recorded.evidence is None
    assert recorded.status is RunStatus.POLICY_REJECTED


async def test_a_provider_that_cannot_measure_a_check_fails_closed(tmp_path: object) -> None:
    """Spec §18: never silently skip a required check.

    DuckDB implements materialization but not `column_null_rates`, so this is
    the real "not every provider supports every check" case rather than a stub
    of one. The requirement is two-part: the run is rejected, and it is
    rejected as *unsupported* — "I could not measure this" is a gap in the
    provider, while "I measured it and it was wrong" is a problem with the
    data, and they call for different fixes.
    """
    import gantry
    import gantry.verify
    from gantry.failure import FailureKind
    from gantry.runs.status import RunStatus

    pytest.importorskip("duckdb")
    db = gantry.sql.connect("duckdb", path=f"{tmp_path}/probe.duckdb")
    seed = db.query(read_only=False, schemas=["main"])
    await seed("CREATE TABLE main.source AS SELECT 1 AS customer_id")

    build = db.materialize(
        sources=["main.*"],
        destinations=["main.*"],
        verify=[
            gantry.verify.row_count(min=1),
            gantry.verify.null_rate(column="customer_id", max=0.01),
        ],
    )

    result = await build("CREATE TABLE main.derived AS SELECT customer_id FROM main.source")

    assert result.status is RunStatus.VERIFICATION_UNSUPPORTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.UNSUPPORTED_VERIFICATION, (
        "an unmeasurable check must be distinguishable from a failed one"
    )
    checks = {
        check.name: check for check in (result.verification.checks if result.verification else ())
    }
    assert checks["row_count"].ok, "the checks it can evaluate still run"
    assert not checks["null_rate"].supported
    assert not checks["null_rate"].ok, "unsupported fails closed rather than passing"


def _seeded(tmp_path: object, *statements: str, name: str = "probe") -> str:
    """A DuckDB file arranged outside Gantry, and its path.

    Seeded with the driver rather than through a governed connection for two
    reasons. Tests should not arrange state through the thing under test, and
    DuckDB refuses a second connection to the same file with a different
    configuration — so a writable Gantry connection held open would stop the
    read-only one these tests need.
    """
    duckdb = pytest.importorskip("duckdb")
    path = f"{tmp_path}/{name}.duckdb"
    connection = duckdb.connect(path)
    try:
        for statement in statements:
            connection.execute(statement)
    finally:
        connection.close()
    return path


async def test_the_same_checks_serve_a_query_and_a_materialization(tmp_path: object) -> None:
    """One library, both paths — the point of the change.

    `row_count` against a materialization asks about the destination it built;
    against a query it asks about the rows that came back. Same object, same
    meaning, and the caller does not have to know which protocol they are in.
    """
    import gantry
    import gantry.verify

    read_path = _seeded(
        tmp_path, "CREATE TABLE main.people AS SELECT 1 AS id UNION ALL SELECT 2", name="read"
    )
    write_path = _seeded(
        tmp_path, "CREATE TABLE main.people AS SELECT 1 AS id UNION ALL SELECT 2", name="write"
    )
    db = gantry.sql.connect("duckdb", path=read_path, read_only=True)
    writable = gantry.sql.connect("duckdb", path=write_path)
    checks: list[gantry.verify.MaterializationCheck] = [
        gantry.verify.row_count(min=1, max=10),
        gantry.verify.required_columns(["id"]),
    ]

    queried = await db.query(schemas=["main"], verify=checks)("SELECT id FROM main.people")
    built = await writable.materialize(sources=["main.*"], destinations=["main.*"], verify=checks)(
        "CREATE TABLE main.copy AS SELECT id FROM main.people"
    )

    assert queried.status is RunStatus.ACCEPTED, queried.failure
    assert built.status is RunStatus.ACCEPTED, built.failure
    for result in (queried, built):
        assert result.verification is not None
        assert [c.name for c in result.verification.checks] == ["row_count", "required_columns"]
    # Both emit evidence, in the same shape.
    for result in (queried, built):
        assert result.evidence is not None
        assert result.status is RunStatus.ACCEPTED
        assert result.verification is not None
        assert json.loads(result.to_json())["id"] == result.id


async def test_a_check_that_cannot_mean_anything_on_a_query_is_unsupported(
    tmp_path: object,
) -> None:
    """The first thing that cannot be the same on both paths.

    `destination_exists` asks about something a query never creates. Answering
    it against the result set would make it trivially true, which is worse than
    refusing: a caller would believe a destination was checked.
    """
    import gantry
    import gantry.verify
    from gantry.failure import FailureKind

    db = gantry.sql.connect(
        "duckdb", path=_seeded(tmp_path, "CREATE TABLE main.t AS SELECT 1 AS a"), read_only=True
    )

    result = await db.query(schemas=["main"], verify=[gantry.verify.destination_exists()])(
        "SELECT a FROM main.t"
    )

    assert result.status is RunStatus.VERIFICATION_UNSUPPORTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.UNSUPPORTED_VERIFICATION
    assert result.verification is not None
    check = result.verification.checks[0]
    assert not check.supported and not check.ok
    assert "does not create one" in (check.message or "")


async def test_a_truncated_result_cannot_have_its_rows_counted(tmp_path: object) -> None:
    """The second, and the subtler one.

    `max_rows` clips the answer. Counting what came back would measure the
    policy rather than the data, and a caller asserting `row_count(min=1000)`
    against a result capped at 10 would be told something untrue either way it
    landed. So a truncated result makes the count unsupported.
    """
    import gantry
    import gantry.verify

    db = gantry.sql.connect(
        "duckdb",
        path=_seeded(
            tmp_path,
            "CREATE TABLE main.many AS SELECT unnest(generate_series(1, 100)) AS id",
        ),
        read_only=True,
    )

    result = await db.query(schemas=["main"], max_rows=5, verify=[gantry.verify.row_count(min=1)])(
        "SELECT id FROM main.many"
    )

    assert result.inline is not None and result.truncated
    assert result.status is RunStatus.VERIFICATION_UNSUPPORTED
    assert result.verification is not None
    check = result.verification.checks[0]
    assert not check.supported
    assert "truncated" in (check.message or "")


async def test_a_query_result_carries_the_same_evidence_shape(tmp_path: object) -> None:
    """Spec §7: one model across surfaces, not one per surface."""
    import gantry
    import gantry.verify

    db = gantry.sql.connect(
        "duckdb", path=_seeded(tmp_path, "CREATE TABLE main.t AS SELECT 1 AS id"), read_only=True
    )

    result = await db.query(schemas=["main"], verify=[gantry.verify.row_count(min=1)])(
        "SELECT id FROM main.t"
    )

    evidence = result.evidence
    assert evidence is not None
    assert evidence.operation == "query"
    assert evidence.proposal_hash is not None
    names = {item.name for item in evidence.observations}
    assert {"rows_returned", "truncated", "row_count"} <= names
    # Whether the answer was the whole answer changes what every other
    # observation means, so it is recorded rather than implied.
    truncated = evidence.observation("truncated")
    assert truncated is not None and truncated.value is False
    assert json.loads(evidence.to_json())["operation"] == "query"


async def test_execution_failure_is_not_overridden_by_result_set_checks(
    tmp_path: object,
) -> None:
    """Re-deciding can only take acceptance away, never grant it."""
    import gantry
    import gantry.verify

    db = gantry.sql.connect(
        "duckdb", path=_seeded(tmp_path, "CREATE TABLE main.t AS SELECT 1 AS id"), read_only=True
    )

    result = await db.query(schemas=["main"], verify=[gantry.verify.row_count(min=0)])(
        "SELECT * FROM main.no_such_table"
    )

    assert result.status is not RunStatus.ACCEPTED
    assert result.status is not RunStatus.REJECTED


async def test_a_null_rate_over_no_rows_is_not_a_null_rate_of_zero(tmp_path: object) -> None:
    """An empty result has nothing to be null in.

    Reporting zero would read as "this column is fully populated" when the
    truth is that nothing was measured — the most misleading direction a check
    can fail in, because it looks like a pass.
    """
    import gantry
    import gantry.verify

    db = gantry.sql.connect(
        "duckdb",
        path=_seeded(tmp_path, "CREATE TABLE main.t AS SELECT 1 AS id WHERE false"),
        read_only=True,
    )

    result = await db.query(
        schemas=["main"], verify=[gantry.verify.null_rate(column="id", max=0.01)]
    )("SELECT id FROM main.t")

    assert result.inline is not None and result.rows == ()
    assert result.status is RunStatus.VERIFICATION_UNSUPPORTED
    assert result.verification is not None
    check = result.verification.checks[0]
    assert not check.supported, "no rows measured is not a measurement of zero"
    assert not check.ok
