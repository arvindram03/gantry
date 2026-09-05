"""Change event parsing and semantics."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from gantry.adapters.cdc.kafka import MalformedEventError, parse_debezium
from gantry.core.changes import ChangeEvent, ChangeOperation, StreamPosition
from pydantic import ValidationError

TOPIC = "gantry.public.customers"


def envelope(op: str = "c", **overrides: object) -> bytes:
    body: dict[str, object] = {
        "op": op,
        "before": {"customer_id": 1, "region": "old"} if op in ("u", "d") else None,
        "after": {"customer_id": 1, "region": "new"} if op in ("c", "u", "r") else None,
        "source": {
            "schema": "public",
            "table": "customers",
            "lsn": 32673093328,
            "ts_ms": 1788580575943,
            "txId": 785,
        },
    }
    body.update(overrides)
    return json.dumps(body).encode()


def key(value: int = 1) -> bytes:
    return json.dumps({"customer_id": value}).encode()


@pytest.mark.parametrize(
    ("code", "operation"),
    [
        ("c", ChangeOperation.INSERT),
        ("u", ChangeOperation.UPDATE),
        ("d", ChangeOperation.DELETE),
        ("r", ChangeOperation.SNAPSHOT_READ),
    ],
)
def test_operations_are_mapped(code: str, operation: ChangeOperation) -> None:
    event = parse_debezium(envelope(code), key(), TOPIC, 0, 7)
    assert event is not None
    assert event.operation is operation


def test_snapshot_reads_are_distinct_from_inserts() -> None:
    """Treating a snapshot read as an insert double-counts the snapshot phase."""
    event = parse_debezium(envelope("r"), key(), TOPIC, 0, 0)
    assert event is not None
    assert event.is_from_snapshot
    assert event.operation is not ChangeOperation.INSERT


def test_the_source_lsn_survives_parsing() -> None:
    """Every ordering decision downstream depends on this field."""
    event = parse_debezium(envelope(), key(), TOPIC, 0, 0)
    assert event is not None
    assert event.source_lsn == 32673093328


def test_the_dataset_comes_from_the_source_not_the_topic() -> None:
    event = parse_debezium(envelope(), key(), TOPIC, 0, 0)
    assert event is not None
    assert event.dataset == "public.customers"


def test_the_stream_position_is_recorded() -> None:
    event = parse_debezium(envelope(), key(), TOPIC, 3, 42)
    assert event is not None
    assert str(event.stream_position) == f"{TOPIC}:3:42"


def test_a_tombstone_is_not_an_event() -> None:
    """A null value marks a key for log compaction; it is not a change."""
    assert parse_debezium(None, key(), TOPIC, 0, 0) is None


def test_an_unknown_operation_is_an_error_not_a_skip() -> None:
    with pytest.raises(MalformedEventError, match="unknown operation"):
        parse_debezium(envelope("z"), key(), TOPIC, 0, 0)


def test_a_non_json_payload_is_an_error() -> None:
    with pytest.raises(MalformedEventError, match="not JSON"):
        parse_debezium(b"not json", key(), TOPIC, 0, 0)


def test_a_missing_timestamp_is_an_error() -> None:
    with pytest.raises(MalformedEventError, match="no source timestamp"):
        parse_debezium(
            envelope(source={"schema": "public", "table": "c", "lsn": 1}), key(), TOPIC, 0, 0
        )


# --- event invariants ------------------------------------------------------


def event(**overrides: object) -> ChangeEvent:
    base: dict[str, object] = {
        "dataset": "public.customers",
        "operation": ChangeOperation.INSERT,
        "key": {"customer_id": "1"},
        "after": {"customer_id": 1},
        "source_lsn": 100,
        "source_timestamp": datetime(2026, 9, 22, tzinfo=UTC),
        "stream_position": StreamPosition(topic=TOPIC, partition=0, offset=0),
    }
    base.update(overrides)
    return ChangeEvent.model_validate(base)


def test_an_insert_without_an_after_image_is_rejected() -> None:
    with pytest.raises(ValidationError, match="no after image"):
        event(after=None)


def test_a_delete_without_a_before_image_is_rejected() -> None:
    with pytest.raises(ValidationError, match="no before image"):
        event(operation=ChangeOperation.DELETE, after=None, before=None)


def test_an_event_without_a_key_is_rejected() -> None:
    """Ordering and deduplication are per key; an event without one has neither."""
    with pytest.raises(ValidationError, match="carries no key"):
        event(key={})


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        event(source_timestamp=datetime(2026, 9, 22))


def test_key_text_is_stable_across_orderings() -> None:
    """Routing and deduplication depend on one key rendering the same way."""
    first = event(key={"a": "1", "b": "2"}).key_text
    second = event(key={"b": "2", "a": "1"}).key_text
    assert first == second == "a=1|b=2"


# --- stream positions ------------------------------------------------------


def test_stream_positions_round_trip() -> None:
    position = StreamPosition(topic="a.b.c", partition=2, offset=99)
    assert StreamPosition.parse(str(position)) == position


def test_stream_positions_survive_dotted_topics() -> None:
    """Topic names contain dots and colons must still split correctly."""
    parsed = StreamPosition.parse("gantry.public.customers:0:17")
    assert parsed.topic == "gantry.public.customers"
    assert parsed.partition == 0
    assert parsed.offset == 17
