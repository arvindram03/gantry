"""Target SQL generation and commit accounting.

These are pure functions, so the properties that make writes idempotent can be
checked without a database.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from gantry.adapters.target.postgres import (
    UnpreparedTargetError,
    _conflict_action,
    _qualified,
    _quote,
    _staging_name,
    _upsert_sql,
)
from gantry.core.commit import CommitResult

NAMES = ["order_id", "status", "amount"]
TYPES = ["bigint", "text", "numeric(12, 2)"]
KEYS = ["order_id"]


def test_upsert_skips_rows_that_are_already_identical() -> None:
    """The WHERE clause is what makes a replay a no-op."""
    sql = _upsert_sql("public.orders", NAMES, TYPES, KEYS)
    assert "ON CONFLICT" in sql
    assert "IS DISTINCT FROM" in sql


def test_upsert_reports_insert_versus_update() -> None:
    """xmax = 0 distinguishes a fresh insert from an update."""
    assert "RETURNING (xmax = 0)" in _upsert_sql("public.orders", NAMES, TYPES, KEYS)


def test_upsert_passes_column_arrays_not_rows() -> None:
    """One statement over arrays, rather than one statement per row."""
    sql = _upsert_sql("public.orders", NAMES, TYPES, KEYS)
    assert "unnest(" in sql
    assert sql.count(":v") == len(NAMES)
    assert "CAST(:v0 AS bigint[])" in sql


def test_a_key_only_table_does_nothing_on_conflict() -> None:
    assert _conflict_action("public.t", ["id"], ["id"]) == "DO NOTHING"


def test_keys_are_never_overwritten() -> None:
    action = _conflict_action("public.orders", NAMES, KEYS)
    assert '"order_id" = EXCLUDED' not in action
    assert '"status" = EXCLUDED."status"' in action


@pytest.mark.parametrize(
    "identifier", ['orders"; DROP TABLE x', "orders-1", "", "1orders", "or ders"]
)
def test_unsafe_identifiers_are_rejected(identifier: str) -> None:
    with pytest.raises(ValueError, match="unsafe identifier"):
        _quote(identifier)


def test_qualified_names_quote_each_part() -> None:
    """Quoting a dotted name whole produces one identifier that names nothing."""
    assert _qualified("public.orders") == '"public"."orders"'


def test_staging_names_are_derived_safely() -> None:
    assert _staging_name("public.orders") == "gantry_staging_public_orders"
    with pytest.raises(ValueError, match="unsafe target name"):
        _staging_name("public.orders; DROP TABLE x")


def test_unsupported_column_types_are_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported column type"):
        _upsert_sql("public.orders", ["a"], ["bigint); DROP TABLE x --"], ["a"])


# --- commit accounting -----------------------------------------------------


def now() -> datetime:
    return datetime(2026, 9, 16, tzinfo=UTC)


def test_a_replay_is_a_noop() -> None:
    result = CommitResult(rows_unchanged=1000, committed_at=now())
    assert result.is_noop
    assert result.rows_changed == 0
    assert result.rows_seen == 1000


def test_a_first_write_is_not_a_noop() -> None:
    result = CommitResult(rows_inserted=1000, committed_at=now())
    assert not result.is_noop
    assert result.rows_changed == 1000


def test_commit_time_must_be_tz_aware() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        CommitResult(committed_at=datetime(2026, 9, 16))


def test_unprepared_target_error_exists_for_missing_key() -> None:
    """A dataset without a stable key cannot be written idempotently."""
    assert issubclass(UnpreparedTargetError, Exception)
