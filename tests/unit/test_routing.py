"""Key routing and ordering."""

from __future__ import annotations

import random
import subprocess
import sys
from datetime import UTC, datetime

import pytest
from gantry.core.changes import ChangeEvent, ChangeOperation, StreamPosition
from gantry.movement.routing import lane_for, latest_per_key, order_by_key, route

AT = datetime(2026, 9, 24, tzinfo=UTC)


def event(key: int, lsn: int) -> ChangeEvent:
    return ChangeEvent(
        dataset="public.orders",
        operation=ChangeOperation.UPDATE,
        key={"id": str(key)},
        after={"id": key},
        source_lsn=lsn,
        source_timestamp=AT,
        stream_position=StreamPosition(topic="t", partition=0, offset=lsn),
    )


def test_a_key_always_lands_in_the_same_lane() -> None:
    """Two workers must agree, or they race on one row."""
    assert lane_for("id=42", 8) == lane_for("id=42", 8)


def test_lanes_are_stable_across_processes() -> None:
    """Python's hash() is randomised per process; this must not be."""
    script = "from gantry.movement.routing import lane_for; print(lane_for('id=42', 8))"
    first = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    ).stdout.strip()
    second = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert first == second == str(lane_for("id=42", 8))


def test_lanes_are_within_range() -> None:
    assert all(0 <= lane_for(f"id={index}", 4) < 4 for index in range(200))


def test_at_least_one_lane_is_required() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        lane_for("id=1", 0)


def test_routing_keeps_a_key_together() -> None:
    """Per-key ordering is only enforceable if one key has one lane."""
    events = [event(1, 10), event(2, 11), event(1, 12), event(3, 13), event(1, 14)]
    routed = route(events, lanes=4)

    lanes_holding_key_one = [
        lane for lane, batch in routed.items() if any(e.key_text == "id=1" for e in batch)
    ]
    assert len(lanes_holding_key_one) == 1


def test_routing_preserves_order_within_a_lane() -> None:
    events = [event(1, 10), event(1, 20), event(1, 30)]
    routed = route(events, lanes=4)
    only_lane = next(iter(routed.values()))
    assert [e.source_lsn for e in only_lane] == [10, 20, 30]


def test_ordering_is_by_key_then_source_position() -> None:
    shuffled = [event(2, 20), event(1, 30), event(2, 10), event(1, 10)]
    ordered = order_by_key(shuffled)
    assert [(e.key_text, e.source_lsn) for e in ordered] == [
        ("id=1", 10),
        ("id=1", 30),
        ("id=2", 10),
        ("id=2", 20),
    ]


def test_ordering_is_deterministic_under_shuffling() -> None:
    """A shuffled batch and an ordered one must produce the same write sequence."""
    events = [event(key, lsn) for key in range(1, 6) for lsn in (10, 20, 30)]
    baseline = [(e.key_text, e.source_lsn) for e in order_by_key(events)]

    for seed in range(5):
        scrambled = list(events)
        random.Random(seed).shuffle(scrambled)
        assert [(e.key_text, e.source_lsn) for e in order_by_key(scrambled)] == baseline


def test_collapsing_keeps_the_newest_per_key() -> None:
    """Safe because apply is last-writer-wins; the intermediates reach the same place."""
    collapsed = latest_per_key([event(1, 10), event(1, 30), event(1, 20), event(2, 5)])
    assert [(e.key_text, e.source_lsn) for e in collapsed] == [("id=1", 30), ("id=2", 5)]


def test_collapsing_an_empty_batch_is_empty() -> None:
    assert latest_per_key([]) == []
