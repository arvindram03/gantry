"""Core vocabulary: sizes, windows, positions, checkpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from gantry.core import (
    Checkpoint,
    CheckpointScope,
    PositionKind,
    SourcePosition,
    TimeWindow,
    format_byte_size,
    parse_byte_size,
)
from gantry.core.sizes import ByteSizeError
from pydantic import ValidationError


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("14.2TB", 14_200_000_000_000),
        ("512MB", 512_000_000),
        ("1KiB", 1024),
        ("1 GiB", 1024**3),
        ("900", 900),
        ("900B", 900),
    ],
)
def test_parse_byte_size(text: str, expected: int) -> None:
    assert parse_byte_size(text) == expected


def test_si_and_iec_units_are_not_conflated() -> None:
    assert parse_byte_size("1GB") != parse_byte_size("1GiB")


@pytest.mark.parametrize("text", ["", "TB", "12XB", "-5", "1.2.3MB"])
def test_reject_unparseable_sizes(text: str) -> None:
    with pytest.raises(ByteSizeError):
        parse_byte_size(text)


def test_format_byte_size_round_trips_through_parse() -> None:
    assert parse_byte_size(format_byte_size(14_200_000_000_000)) == 14_200_000_000_000


def test_time_window_is_half_open() -> None:
    start = datetime(2026, 9, 3, tzinfo=UTC)
    window = TimeWindow(start=start, end=start + timedelta(days=1))
    assert window.duration == timedelta(days=1)
    assert window.contains(start)
    assert not window.contains(window.end)


def test_time_window_requires_tz_aware_bounds() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        TimeWindow(start=datetime(2026, 9, 3), end=datetime(2026, 9, 4))


def test_time_window_rejects_inverted_bounds() -> None:
    start = datetime(2026, 9, 4, tzinfo=UTC)
    with pytest.raises(ValidationError, match="must be after"):
        TimeWindow(start=start, end=start - timedelta(hours=1))


def test_positions_are_only_comparable_within_a_kind() -> None:
    lsn = SourcePosition(kind=PositionKind.LSN, value="0/16B3748")
    offset = SourcePosition(kind=PositionKind.OFFSET, value="4711")
    assert lsn.comparable_with(SourcePosition(kind=PositionKind.LSN, value="0/16B37AA"))
    assert not lsn.comparable_with(offset)


def test_checkpoint_requires_tz_aware_commit_time() -> None:
    position = SourcePosition(kind=PositionKind.LSN, value="0/16B3748")
    with pytest.raises(ValidationError, match="timezone-aware"):
        Checkpoint(
            scope=CheckpointScope.PARTITION,
            scope_id="orders/0",
            position=position,
            committed_at=datetime(2026, 9, 3),
        )


def test_core_types_are_immutable() -> None:
    position = SourcePosition(kind=PositionKind.OFFSET, value="1")
    with pytest.raises(ValidationError):
        position.value = "2"  # type: ignore[misc]  # runtime immutability check
