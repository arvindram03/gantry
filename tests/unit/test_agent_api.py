"""The aggregate vocabulary, and what it refuses to express.

The API takes a structured aggregate rather than SQL so that "is this an
aggregate" is decidable. These tests pin the two things that makes possible:
a field name cannot carry SQL, and the columns that expose a value are known
before the query runs.
"""

from __future__ import annotations

import pytest
from gantry.api.aggregates import (
    GROUP_SIZE_COLUMN,
    Aggregate,
    AggregateFunction,
    AggregateQuery,
)
from gantry.core import DatasetManifest, DatasetSchema, FieldSchema, PhysicalRef
from gantry.policy.redaction import REDACTED, redact_columns
from pydantic import ValidationError

FIELDS = (
    FieldSchema(name="order_id", type="bigint"),
    FieldSchema(name="email", type="text"),
    FieldSchema(name="total", type="numeric"),
)


def manifest() -> DatasetManifest:
    return DatasetManifest(
        name="orders",
        physical=PhysicalRef(adapter="postgres", reference="public.orders"),
        dataset_schema=DatasetSchema(keys=("order_id",), fields=FIELDS),
        sensitive_fields=("email",),
    )


def count() -> Aggregate:
    return Aggregate(function=AggregateFunction.COUNT)


class TestAggregateShape:
    def test_a_query_must_compute_something(self) -> None:
        with pytest.raises(ValidationError, match="at least one aggregate"):
            AggregateQuery(group_by=("total",))

    def test_count_takes_no_field(self) -> None:
        with pytest.raises(ValidationError, match="count takes no field"):
            Aggregate(function=AggregateFunction.COUNT, field="total")

    def test_every_other_function_needs_one(self) -> None:
        with pytest.raises(ValidationError, match="needs a field"):
            Aggregate(function=AggregateFunction.SUM)

    def test_two_aggregates_may_not_share_a_name(self) -> None:
        """Colliding names would silently drop one measure from the output."""
        with pytest.raises(ValidationError, match="share a name"):
            AggregateQuery(
                aggregates=(
                    Aggregate(function=AggregateFunction.SUM, field="total", alias="t"),
                    Aggregate(function=AggregateFunction.AVG, field="total", alias="t"),
                )
            )

    def test_the_group_size_column_is_reserved(self) -> None:
        """It is what the minimum-group check reads; an alias shadowing it
        would make that check read the caller's number instead."""
        with pytest.raises(ValidationError, match="reserved"):
            AggregateQuery(
                aggregates=(
                    Aggregate(
                        function=AggregateFunction.SUM, field="total", alias=GROUP_SIZE_COLUMN
                    ),
                )
            )


class TestFieldChecking:
    def test_an_unknown_field_is_refused(self) -> None:
        query = AggregateQuery(group_by=("nonexistent",), aggregates=(count(),))
        with pytest.raises(ValueError, match="has no field 'nonexistent'"):
            query.check_against(manifest())

    def test_a_field_name_carrying_sql_is_refused_as_an_unknown_field(self) -> None:
        """Injection and typos fail the same way, which is why this check is
        the whole defence rather than an assist to escaping."""
        query = AggregateQuery(
            group_by=('total" FROM public.orders; DROP TABLE orders; --',),
            aggregates=(count(),),
        )
        with pytest.raises(ValueError, match="has no field"):
            query.check_against(manifest())

    def test_fields_in_a_filter_are_checked_too(self) -> None:
        query = AggregateQuery(aggregates=(count(),), where={"missing": "x"})
        with pytest.raises(ValueError, match="has no field 'missing'"):
            query.check_against(manifest())

    def test_an_undiscovered_schema_checks_nothing(self) -> None:
        """Before discovery a manifest lists no fields. Refusing every query
        until then would make the API useless on a freshly declared Dataset."""
        bare = DatasetManifest(
            name="orders",
            physical=PhysicalRef(adapter="postgres", reference="public.orders"),
            dataset_schema=DatasetSchema(keys=("order_id",)),
        )
        AggregateQuery(group_by=("anything",), aggregates=(count(),)).check_against(bare)


class TestWhatIsExposed:
    def test_a_grouping_key_returns_its_values(self) -> None:
        query = AggregateQuery(group_by=("email",), aggregates=(count(),))
        assert query.fields_returned() == ("email",)

    def test_counting_a_sensitive_field_does_not_return_it(self) -> None:
        """Counting a column is not printing it, and masking the count would
        withhold a number that reveals nothing."""
        query = AggregateQuery(
            aggregates=(Aggregate(function=AggregateFunction.COUNT_DISTINCT, field="email"),)
        )
        assert query.fields_returned() == ()
        assert query.fields_touched() == ("email",)

    @pytest.mark.parametrize("function", [AggregateFunction.MIN, AggregateFunction.MAX])
    def test_min_and_max_do_return_it(self, function: AggregateFunction) -> None:
        query = AggregateQuery(aggregates=(Aggregate(function=function, field="email"),))
        assert query.fields_returned() == ("email",)

    def test_exposing_columns_uses_the_output_alias_not_the_field_name(self) -> None:
        """The bug this exists to prevent: masking by source field name against
        an aliased output matches nothing, while the decision still says
        REDACT. A mask that is not applied is worse than one never promised."""
        query = AggregateQuery(
            group_by=("total",),
            aggregates=(Aggregate(function=AggregateFunction.MIN, field="email"),),
        )
        assert query.columns_exposing(("email",)) == ("min_email",)

        rows = redact_columns(
            [{"total": 10, "min_email": "a@b.c"}], query.columns_exposing(("email",))
        )
        assert rows == [{"total": 10, "min_email": REDACTED}]

    def test_a_grouping_key_is_masked_under_its_own_name(self) -> None:
        query = AggregateQuery(group_by=("email",), aggregates=(count(),))
        assert query.columns_exposing(("email",)) == ("email",)

    def test_an_alias_is_honoured_when_masking(self) -> None:
        query = AggregateQuery(
            aggregates=(
                Aggregate(function=AggregateFunction.MAX, field="email", alias="latest_contact"),
            )
        )
        assert query.columns_exposing(("email",)) == ("latest_contact",)

    def test_nothing_is_exposed_when_nothing_is_sensitive(self) -> None:
        query = AggregateQuery(group_by=("total",), aggregates=(count(),))
        assert query.columns_exposing(()) == ()
