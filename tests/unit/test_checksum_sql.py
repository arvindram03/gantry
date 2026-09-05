"""Checksum SQL generation and normalisation rules.

Pure string generation, so the normalisation contract can be checked without a
database. The rules themselves are documented in docs/guarantees.md.
"""

from __future__ import annotations

import pytest
from gantry.core.dataset import DatasetManifest, PhysicalRef
from gantry.core.schema import DatasetSchema, FieldSchema
from gantry.verification.checksum import (
    NULL_SENTINEL,
    ChunkChecksum,
    checksum_expression,
    normalize_column,
    row_expression,
)


def field(name: str, declared: str) -> FieldSchema:
    return FieldSchema(name=name, type=declared)


@pytest.mark.parametrize(
    ("declared", "expected"),
    [
        ("numeric(12, 2)", "trim_scale"),
        ("double precision", "round"),
        ("timestamp with time zone", "AT TIME ZONE 'UTC'"),
        ("date", "YYYY-MM-DD"),
        ("boolean", "::int::text"),
        ("bytea", "encode"),
        ("text", "::text"),
    ],
)
def test_types_are_normalised_before_hashing(declared: str, expected: str) -> None:
    assert expected in normalize_column(field("c", declared))


def test_nulls_get_a_sentinel() -> None:
    """A NULL rendered as nothing is an empty string; inside a concatenation it
    makes the whole row NULL. Both are silent."""
    rendered = normalize_column(field("c", "text"))
    assert rendered.startswith("coalesce(")
    assert NULL_SENTINEL in rendered


def manifest(*fields: FieldSchema) -> DatasetManifest:
    """A manifest whose key is always present, as the schema model requires."""
    declared = (field("order_id", "bigint"), *fields) if fields else ()
    return DatasetManifest(
        name="public.orders",
        physical=PhysicalRef(adapter="postgres", reference="public.orders"),
        dataset_schema=DatasetSchema(keys=("order_id",) if declared else (), fields=declared),
    )


def test_columns_are_separated_unambiguously() -> None:
    """("ab", "c") and ("a", "bc") must not render identically."""
    rendered = row_expression(manifest(field("a", "text"), field("b", "text")))
    assert "\x1f" in rendered


def test_a_checksum_needs_discovered_fields() -> None:
    with pytest.raises(ValueError, match="no discovered fields"):
        row_expression(manifest())


def test_the_checksum_sums_rather_than_xors() -> None:
    """XOR cancels duplicates, so a row written twice would be invisible to
    exactly the check meant to catch it."""
    expression = checksum_expression(manifest(field("a", "text")))
    assert "sum(" in expression
    assert "bit_xor" not in expression


def test_the_checksum_is_order_independent() -> None:
    """No sort, so no dependence on the order rows come back in."""
    expression = checksum_expression(manifest(field("a", "text")))
    assert "ORDER BY" not in expression
    assert "string_agg" not in expression


def test_an_empty_scope_checksums_to_zero_not_null() -> None:
    assert "coalesce(sum(" in checksum_expression(manifest(field("a", "text")))


# --- comparison semantics --------------------------------------------------


def test_checksums_compare_on_hash_and_row_count() -> None:
    """A checksum alone cannot tell an empty range from one that cancelled."""
    assert ChunkChecksum("abc", 10) == ChunkChecksum("abc", 10)
    assert ChunkChecksum("abc", 10) != ChunkChecksum("abc", 11)
    assert ChunkChecksum("abc", 10) != ChunkChecksum("abd", 10)


def test_checksums_describe_themselves_readably() -> None:
    assert ChunkChecksum("abc", 1234).describe() == "abc over 1,234 rows"
