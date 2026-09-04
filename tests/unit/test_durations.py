"""Duration parsing."""

from __future__ import annotations

from datetime import timedelta

import pytest
from gantry.core.durations import DurationError, format_duration, parse_duration


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2s", timedelta(seconds=2)),
        ("30m", timedelta(minutes=30)),
        ("24h", timedelta(hours=24)),
        ("7d", timedelta(days=7)),
        ("1w", timedelta(weeks=1)),
        ("100ms", timedelta(milliseconds=100)),
        ("1.5h", timedelta(minutes=90)),
    ],
)
def test_parse_duration(text: str, expected: timedelta) -> None:
    assert parse_duration(text) == expected


def test_minutes_and_milliseconds_are_distinguished() -> None:
    assert parse_duration("5m") != parse_duration("5ms")


@pytest.mark.parametrize("text", ["", "30", "5 fortnights", "-2s", "s"])
def test_reject_unparseable_durations(text: str) -> None:
    with pytest.raises(DurationError):
        parse_duration(text)


def test_bare_number_is_rejected_rather_than_guessed() -> None:
    with pytest.raises(DurationError, match="expected a number and one of"):
        parse_duration("30")


def test_timedelta_passes_through() -> None:
    assert parse_duration(timedelta(seconds=5)) == timedelta(seconds=5)


@pytest.mark.parametrize("text", ["2s", "30m", "24h", "1w"])
def test_format_round_trips(text: str) -> None:
    assert parse_duration(format_duration(parse_duration(text))) == parse_duration(text)
