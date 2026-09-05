"""Failure classification."""

from __future__ import annotations

import pytest
from gantry.scheduler.failures import FailureClass, classify, is_retryable


class FakeDatabaseError(Exception):
    pass


@pytest.mark.parametrize(
    "message",
    [
        "deadlock detected",
        "could not serialize access due to concurrent update",
        "connection was closed in the middle of operation",
        "server closed the connection unexpectedly",
        "canceling statement due to statement timeout",
        "too many connections for role",
    ],
)
def test_transient_conditions_are_retryable(message: str) -> None:
    assert classify(FakeDatabaseError(message)) is FailureClass.RETRYABLE


@pytest.mark.parametrize(
    "message",
    [
        'relation "public.orders" does not exist',
        "dataset 'orders' has no field 'created_at'",
        "dataset 'orders' has no key; idempotent writes need a stable key",
    ],
)
def test_a_plan_describing_a_vanished_world_needs_replanning(message: str) -> None:
    """Retrying reproduces the same error; the plan has to change."""
    assert classify(FakeDatabaseError(message)) is FailureClass.NEEDS_REPLAN


@pytest.mark.parametrize(
    "message",
    ["permission denied for table orders", "unsafe identifier: 'x\"; DROP'"],
)
def test_refusals_are_fatal(message: str) -> None:
    """Neither retrying nor replanning can help."""
    assert classify(FakeDatabaseError(message)) is FailureClass.FATAL


def test_an_unrecognised_error_is_retried() -> None:
    """The runtime assumes retries happen; a bounded attempt count is the guard.

    Guessing retryable turns a wrong guess into a quarantined task. Guessing
    fatal turns a transient blip into a stalled migration.
    """
    assert classify(FakeDatabaseError("something nobody has seen before")) is (
        FailureClass.RETRYABLE
    )
    assert is_retryable(FakeDatabaseError("mystery"))


def test_classification_reads_the_exception_type_too() -> None:
    """Driver errors often carry their meaning in the class name, not the text.

    Matching is on the lowercased "Type: message" string, so a marker only
    catches a type name when it is written without spaces.
    """

    class UndefinedTableError(Exception):
        pass

    assert classify(UndefinedTableError("")) is FailureClass.NEEDS_REPLAN
