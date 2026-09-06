# SPDX-License-Identifier: Apache-2.0
"""What an Iceberg target refuses, and when.

The refusing is the feature. A column Iceberg cannot hold must fail in Prepare
with the column named, not at row forty million inside a Java writer.
"""

from __future__ import annotations

import pytest
from gantry.adapters.target.iceberg import (
    UnsupportedTypeError,
    iceberg_schema,
    iceberg_type,
)
from gantry.core import DatasetManifest, DatasetSchema, FieldSchema, PhysicalRef


def field(name: str, declared: str) -> FieldSchema:
    return FieldSchema(name=name, type=declared)


def manifest(*fields: FieldSchema) -> DatasetManifest:
    return DatasetManifest.model_validate(
        {
            "name": "public.orders",
            "physical": PhysicalRef(adapter="postgres", reference="public.orders"),
            "dataset_schema": DatasetSchema(keys=(fields[0].name,), fields=fields),
        }
    )


class TestWhatItAccepts:
    @pytest.mark.parametrize(
        ("declared", "expected"),
        [
            ("bigint", "long"),
            ("integer", "int"),
            ("boolean", "boolean"),
            ("text", "string"),
            ("character varying(64)", "string"),
            ("double precision", "double"),
            ("date", "date"),
            ("bytea", "binary"),
            ("uuid", "uuid"),
            ("timestamp with time zone", "timestamptz"),
            ("timestamp without time zone", "timestamp"),
            ("numeric(12,2)", "decimal(12, 2)"),
            ("numeric(38,9)", "decimal(38, 9)"),
        ],
    )
    def test_a_representable_type_maps(self, declared: str, expected: str) -> None:
        assert iceberg_type(field("c", declared)) == expected

    def test_a_length_limit_does_not_travel(self) -> None:
        """Iceberg strings are unbounded; varchar(64) is a source-side
        constraint, and pretending otherwise would invent a check the target
        does not perform."""
        assert iceberg_type(field("c", "varchar(64)")) == "string"


class TestWhatItRefuses:
    @pytest.mark.parametrize("declared", ["json", "jsonb", "interval", "money", "tsvector", "xml"])
    def test_a_type_with_no_iceberg_equivalent_is_refused(self, declared: str) -> None:
        with pytest.raises(UnsupportedTypeError, match="c "):
            iceberg_type(field("c", declared))

    def test_the_message_says_why_and_not_merely_no(self) -> None:
        """An operator reading this has to decide what to do next."""
        with pytest.raises(UnsupportedTypeError, match="no JSON type"):
            iceberg_type(field("payload", "jsonb"))

    def test_a_decimal_beyond_icebergs_precision_is_refused(self) -> None:
        with pytest.raises(UnsupportedTypeError, match="38 digits"):
            iceberg_type(field("amount", "numeric(40,2)"))

    def test_an_unconstrained_numeric_is_refused_rather_than_bounded(self) -> None:
        """PostgreSQL's bare `numeric` is unbounded. Choosing a precision for
        the user would silently store something other than what they have."""
        with pytest.raises(UnsupportedTypeError, match="unconstrained numeric"):
            iceberg_type(field("amount", "numeric"))

    def test_an_unknown_type_is_refused_rather_than_guessed(self) -> None:
        with pytest.raises(UnsupportedTypeError, match="Refused rather"):
            iceberg_type(field("shape", "geometry(Point,4326)"))


class TestTheWholeSchema:
    def test_every_problem_is_reported_at_once(self) -> None:
        """One round trip per bad column is a bad way to fix a schema."""
        with pytest.raises(UnsupportedTypeError) as raised:
            iceberg_schema(
                manifest(
                    field("id", "bigint"),
                    field("payload", "jsonb"),
                    field("window", "interval"),
                    field("amount", "numeric"),
                )
            )
        message = str(raised.value)
        assert "payload" in message
        assert "window" in message
        assert "amount" in message

    def test_a_representable_schema_maps_completely(self) -> None:
        mapped = iceberg_schema(
            manifest(
                field("id", "bigint"), field("label", "text"), field("amount", "numeric(12,2)")
            )
        )
        assert mapped == {"id": "long", "label": "string", "amount": "decimal(12, 2)"}
