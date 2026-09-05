# SPDX-License-Identifier: Apache-2.0
"""The aggregate vocabulary an agent may ask for over a Dataset.

The RFC sketches `dataset.query(sql, params)`. This takes a structured request
instead, and the reason is the policy: `rows: deny, aggregates: allow` is only
enforceable if "is this an aggregate" is decidable. Over arbitrary SQL it is
not - deciding it would mean parsing every dialect Gantry dispatches to, and
being wrong once means an agent read raw rows through a rule that said it
could not.

Built from a fixed vocabulary, the answer is decidable by construction. Every
projection is either a grouping key or one of the functions below, every field
is checked against the manifest, and identifiers are quoted rather than
interpolated - so a field name cannot carry SQL.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator

from gantry.core.dataset import DatasetManifest
from gantry.core.names import FieldName

# Always projected alongside the requested aggregates. Without it the minimum
# group size cannot be checked, and that check is what stops an aggregate over
# a unique key from being row access wearing a GROUP BY.
GROUP_SIZE_COLUMN = "row_count"


class AggregateFunction(StrEnum):
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    NULL_RATE = "null_rate"


# Functions that reduce a column to one number without reproducing a value from
# it. MIN and MAX are absent on purpose: both return an actual stored value.
_NON_QUOTING = frozenset(
    {
        AggregateFunction.COUNT,
        AggregateFunction.COUNT_DISTINCT,
        AggregateFunction.SUM,
        AggregateFunction.AVG,
        AggregateFunction.NULL_RATE,
    }
)

_NEEDS_FIELD = frozenset(set(AggregateFunction) - {AggregateFunction.COUNT})


class Aggregate(BaseModel):
    """One measure to compute."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    function: AggregateFunction
    field: FieldName | None = None
    alias: str | None = None

    @model_validator(mode="after")
    def _check_aggregate(self) -> Aggregate:
        if self.function in _NEEDS_FIELD and not self.field:
            raise ValueError(f"{self.function.value} needs a field")
        if self.function is AggregateFunction.COUNT and self.field:
            raise ValueError("count takes no field; use count_distinct for a column")
        return self

    @property
    def name(self) -> str:
        return self.alias or (
            self.function.value if self.field is None else f"{self.function.value}_{self.field}"
        )

    @property
    def quotes_a_value(self) -> bool:
        """Whether this measure can return a value stored in the column.

        `min` and `max` do. Over a sensitive field that makes them a way to
        read the data one bound at a time, so they are treated as revealing it.
        """
        return self.function not in _NON_QUOTING

    def expression(self, quote: Callable[[str], str]) -> str:
        column = "" if self.field is None else quote(self.field)
        match self.function:
            case AggregateFunction.COUNT:
                return "count(*)"
            case AggregateFunction.COUNT_DISTINCT:
                return f"count(DISTINCT {column})"
            case AggregateFunction.NULL_RATE:
                return f"avg(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END)"
            case _:
                return f"{self.function.value}({column})"


class AggregateQuery(BaseModel):
    """A grouped aggregate over one Dataset."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    group_by: tuple[FieldName, ...] = ()
    aggregates: tuple[Aggregate, ...] = ()
    # Equality filters only. A structured filter cannot smuggle an expression,
    # and anything richer belongs in an Analysis, which is verified.
    where: dict[FieldName, str] = {}
    limit: int | None = None

    @model_validator(mode="after")
    def _check_query(self) -> AggregateQuery:
        if not self.aggregates:
            raise ValueError("an aggregate query must compute at least one aggregate")
        names = [aggregate.name for aggregate in self.aggregates]
        if len(set(names)) != len(names):
            raise ValueError(f"two aggregates share a name: {sorted(names)}")
        if GROUP_SIZE_COLUMN in set(names) | set(self.group_by):
            raise ValueError(f"{GROUP_SIZE_COLUMN!r} is reserved for the group size")
        return self

    def fields_touched(self) -> tuple[str, ...]:
        """Every field this query reads, whether or not it is returned."""
        touched = set(self.group_by) | set(self.where)
        touched |= {a.field for a in self.aggregates if a.field is not None}
        return tuple(sorted(touched))

    def fields_returned(self) -> tuple[str, ...]:
        """Fields whose values can appear in the output.

        A grouping key is returned verbatim. An aggregate is returned only if
        it reproduces a stored value - counting a sensitive column is not the
        same as printing it, and treating them alike would mask numbers that
        reveal nothing.
        """
        returned = set(self.group_by)
        returned |= {a.field for a in self.aggregates if a.quotes_a_value and a.field is not None}
        return tuple(sorted(returned))

    def columns_exposing(self, fields: Sequence[str]) -> tuple[str, ...]:
        """Output columns through which these fields' values can be read.

        Redaction masks output columns, and an aggregate's output is aliased -
        `min_request_id`, not `request_id`. Masking by source field name would
        match nothing and mask nothing, while the decision still said REDACT.
        """
        wanted = set(fields)
        columns = {field for field in self.group_by if field in wanted}
        columns |= {
            aggregate.name
            for aggregate in self.aggregates
            if aggregate.quotes_a_value and aggregate.field in wanted
        }
        return tuple(sorted(columns))

    def check_against(self, manifest: DatasetManifest) -> None:
        """Refuse anything naming a field the Dataset does not have."""
        known = {field.name for field in manifest.dataset_schema.fields}
        if not known:
            return
        unknown = sorted(set(self.fields_touched()) - known)
        if unknown:
            raise ValueError(f"{manifest.name} has no field {', '.join(repr(u) for u in unknown)}")
