# SPDX-License-Identifier: Apache-2.0
"""Whether a target can hold what the source will send it.

Every rule here is **directional**, and that is the whole reason the module
exists. A target wider than the source is fine; the reverse truncates. A target
more permissive about nulls is fine; the reverse rejects rows the source
considers valid. A check written symmetrically would refuse half the migrations
that are safe and permit half that are not.
"""

from __future__ import annotations

import pytest
from gantry.core import DatasetManifest, DatasetSchema, FieldSchema, PhysicalRef
from gantry.migration.compatibility import (
    CompatibilityCheck,
    CompatibilityRepair,
    check_field,
    check_manifest,
)


def field(name: str = "amount", type_: str = "numeric(12,2)", nullable: bool = True) -> FieldSchema:
    return FieldSchema(name=name, type=type_, nullable=nullable)


def manifest(
    *fields: FieldSchema, keys: tuple[str, ...] | None = None, name: str = "orders"
) -> DatasetManifest:
    # The key defaults to the first field, so a manifest built for a type test
    # does not also have to declare an id column it never looks at.
    resolved = keys if keys is not None else (fields[0].name,)
    return DatasetManifest(
        name=name,
        physical=PhysicalRef(adapter="postgres", reference=f"public.{name}"),
        dataset_schema=DatasetSchema(keys=resolved, fields=fields),
    )


def failures(source: FieldSchema, target: FieldSchema) -> list[str]:
    return [f.check.value for f in check_field("orders", source, target)]


class TestWidening:
    @pytest.mark.parametrize(
        ("source", "target"),
        [
            ("integer", "bigint"),
            ("smallint", "integer"),
            ("real", "double precision"),
            ("date", "timestamp with time zone"),
            ("varchar", "text"),
            ("uuid", "text"),
        ],
    )
    def test_a_wider_target_is_fine(self, source: str, target: str) -> None:
        assert failures(field(type_=source), field(type_=target)) == []

    @pytest.mark.parametrize(
        ("source", "target"),
        [
            ("bigint", "integer"),
            ("integer", "smallint"),
            ("double precision", "real"),
            ("timestamp with time zone", "date"),
            ("text", "uuid"),
        ],
    )
    def test_a_narrower_target_is_refused(self, source: str, target: str) -> None:
        """The direction that loses data. `bigint` into `integer` overflows;
        symmetry here would let it through."""
        assert failures(field(type_=source), field(type_=target)) == ["type_compatible"]

    def test_identical_types_agree(self) -> None:
        assert failures(field(), field()) == []

    @pytest.mark.parametrize(
        ("alias", "canonical"),
        [
            ("int4", "integer"),
            ("int8", "bigint"),
            ("timestamptz", "timestamp with time zone"),
            ("varchar", "character varying"),
            ("bool", "boolean"),
            ("decimal", "numeric"),
        ],
    )
    def test_catalog_aliases_are_the_same_type(self, alias: str, canonical: str) -> None:
        """The catalog and hand-written specs spell these differently; a
        refusal over spelling would be a false alarm every time."""
        assert failures(field(type_=alias), field(type_=canonical)) == []


class TestPrecision:
    def test_a_narrower_target_truncates(self) -> None:
        assert failures(field(type_="numeric(12,2)"), field(type_="numeric(6,2)")) == [
            "width_sufficient"
        ]

    def test_a_wider_target_is_fine(self) -> None:
        assert failures(field(type_="numeric(6,2)"), field(type_="numeric(12,2)")) == []

    def test_less_scale_truncates_too(self) -> None:
        """`numeric(12,4)` into `numeric(12,2)` silently rounds every value."""
        assert failures(field(type_="numeric(12,4)"), field(type_="numeric(12,2)")) == [
            "width_sufficient"
        ]

    def test_a_shorter_varchar_truncates(self) -> None:
        assert failures(field(type_="varchar(255)"), field(type_="varchar(64)")) == [
            "width_sufficient"
        ]

    def test_an_unbounded_target_is_always_sufficient(self) -> None:
        """`text` has no declared width, so nothing can be too long for it."""
        assert failures(field(type_="varchar(255)"), field(type_="text")) == []


class TestNullability:
    def test_a_nullable_source_into_a_not_null_target_is_refused(self) -> None:
        """The target would reject rows the source considers valid."""
        assert failures(field(nullable=True), field(nullable=False)) == ["nullability"]

    def test_a_not_null_source_into_a_nullable_target_is_fine(self) -> None:
        """A nullable target simply never sees a null."""
        assert failures(field(nullable=False), field(nullable=True)) == []


class TestWholeManifests:
    def test_a_missing_table_asks_to_create_it_rather_than_refusing(self) -> None:
        report = check_manifest(manifest(field()), None, target_name="public.orders")
        assert not report.compatible
        assert report.repairs == (CompatibilityRepair.CREATE_TARGET,)

    def test_a_missing_column_is_named(self) -> None:
        report = check_manifest(
            manifest(field("id", "bigint"), field("amount")),
            manifest(field("id", "bigint")),
            target_name="public.orders",
        )
        assert [f.column for f in report.failures] == ["amount"]
        assert report.failures[0].check is CompatibilityCheck.COLUMN_PRESENT

    def test_a_missing_key_column_is_refused_for_a_stated_reason(self) -> None:
        """Idempotent writes detect a conflict on the key. Without it a
        replayed batch duplicates instead of upserting — the guarantee the
        whole runtime rests on."""
        report = check_manifest(
            manifest(field("id", "bigint"), keys=("id",)),
            manifest(field("other", "bigint"), keys=("other",)),
            target_name="public.orders",
        )
        checks = {f.check for f in report.failures}
        assert CompatibilityCheck.KEY_PRESENT in checks
        assert any("conflict" in f.problem for f in report.failures)

    def test_extra_target_columns_are_reported_but_not_refused(self) -> None:
        """A target may carry its own bookkeeping. A column nobody remembers
        adding is worth seeing before a cutover, not after."""
        report = check_manifest(
            manifest(field("id", "bigint")),
            manifest(field("id", "bigint"), field("loaded_at", "timestamptz")),
            target_name="public.orders",
        )
        assert report.compatible
        assert report.extra_columns == ("loaded_at",)

    def test_an_undiscovered_source_asks_for_rediscovery(self) -> None:
        bare = DatasetManifest(
            name="orders",
            physical=PhysicalRef(adapter="postgres", reference="public.orders"),
            dataset_schema=DatasetSchema(keys=("id",)),
        )
        report = check_manifest(bare, manifest(field()), target_name="public.orders")
        assert report.repairs == (CompatibilityRepair.REDISCOVER,)

    def test_every_offending_column_is_reported_not_only_the_first(self) -> None:
        """An operator fixing one column at a time across six round trips is
        the failure mode a structured report exists to avoid."""
        report = check_manifest(
            manifest(
                field("id", "bigint"),
                field("amount", "numeric(12,2)"),
                field("status", "text"),
                field("region", "text", nullable=True),
            ),
            manifest(
                field("id", "bigint"),
                field("amount", "numeric(6,2)"),
                field("region", "text", nullable=False),
            ),
            target_name="public.orders",
        )
        assert {f.column for f in report.failures} == {"amount", "status", "region"}

    def test_a_compatible_pair_says_so(self) -> None:
        report = check_manifest(
            manifest(field("id", "bigint"), field("amount", "numeric(12,2)")),
            manifest(field("id", "bigint"), field("amount", "numeric(12,2)")),
            target_name="public.orders",
        )
        assert report.compatible
        assert "compatible" in report.describe()


def test_a_failure_description_never_uses_square_brackets() -> None:
    """These lines are rendered through Rich, which reads `[...]` as markup
    and silently swallows it. A refusal that loses the name of what it
    refused is worse than no refusal."""
    report = check_manifest(
        manifest(field("id", "bigint"), field("amount", "numeric(12,2)")),
        manifest(field("id", "bigint"), field("amount", "numeric(6,2)")),
        target_name="public.orders",
    )
    for failure in report.failures:
        assert "[" not in failure.describe()
