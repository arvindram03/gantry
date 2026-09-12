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

import pytest
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.runs.model import render
from gantry.runs.store import MemoryRunStore, SQLiteRunStore, _bundle_from_dict
from gantry.verifier import CheckResult


def _bundle(**overrides: object) -> EvidenceBundle:
    started = datetime(2026, 9, 12, 3, 0, tzinfo=UTC)
    defaults: dict[str, object] = {
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
    return EvidenceBundle(**{**defaults, **overrides})  # type: ignore[arg-type]


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
    text = render(_bundle())

    assert "Run run_123" in text
    assert "Status       ACCEPTED" in text
    assert "raw.orders" in text
    assert "postgres://analytics/customer_metrics" in text
    assert "job: job_xyz" in text
    assert "runtime: 18.3s" in text
    assert "✓ row_count" in text
    assert "expected: min 1000000" in text
    assert "observed: value 1190432" in text


def test_an_unsupported_check_renders_differently_from_a_failed_one() -> None:
    """ "Could not measure" and "measured and it was wrong" are different facts."""
    failed = _bundle(
        decision="VERIFICATION_FAILED",
        checks=(
            CheckResult("row_count", False, {"min": 1}, {"value": 0}, "too few rows"),
            CheckResult("null_rate", False, {"max": 0.01}, None, "unavailable", supported=False),
        ),
    )

    text = render(failed)

    assert "✗ row_count" in text
    assert "? null_rate" in text


def test_a_memory_store_returns_what_it_was_given() -> None:
    store = MemoryRunStore()

    stored = store.record(_bundle())

    assert store.get("run_123") is stored
    assert store.get("absent") is None
    assert [run.run_id for run in store.list()] == ["run_123"]


def test_a_memory_store_filters_by_decision() -> None:
    store = MemoryRunStore()
    store.record(_bundle())
    store.record(_bundle(run_id="run_456", decision="VERIFICATION_FAILED"))

    rejected = store.list(decision="VERIFICATION_FAILED")

    assert [run.run_id for run in rejected] == ["run_456"]


def test_a_sqlite_store_outlives_the_object_that_wrote_it(tmp_path: object) -> None:
    """The invariant the whole feature rests on.

    Not "the store works" but "the evidence is still there once everything that
    produced it is gone", which is why this opens a second store over the same
    file rather than reusing the first.
    """
    path = f"{tmp_path}/runs.db"
    writer = SQLiteRunStore(path)
    writer.record(_bundle())
    writer.close()

    reader = SQLiteRunStore(path)
    try:
        run = reader.get("run_123")
        assert run is not None
        assert run.decision == "ACCEPTED"
        assert run.evidence.inputs == ("raw.orders",)
        assert run.evidence.checks[0].expected == {"min": 1_000_000}
        assert "✓ row_count" in run.render()
    finally:
        reader.close()


def test_recording_the_same_run_twice_updates_rather_than_duplicates(
    tmp_path: object,
) -> None:
    """A stream is observed more than once, and is one run each time."""
    store = SQLiteRunStore(f"{tmp_path}/runs.db")
    try:
        store.record(_bundle(decision="ACCEPTED"))
        store.record(_bundle(decision="VERIFICATION_FAILED"))

        assert len(store.list()) == 1
        run = store.get("run_123")
        assert run is not None and run.decision == "VERIFICATION_FAILED"
    finally:
        store.close()


def test_recording_nothing_is_not_an_error() -> None:
    """Results without evidence exist; callers should not need a guard."""
    from gantry import runs

    assert runs.record(None) is None


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
    from gantry.result import ResultStatus

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

    assert result.status is ResultStatus.VERIFICATION_FAILED
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
