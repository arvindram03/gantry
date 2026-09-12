# SPDX-License-Identifier: Apache-2.0
"""Describe a query's rows as a table, so one set of checks serves both paths.

A materialization is verified against the destination it created. A query has
no destination — but it has columns, a row count and values, which is what the
checks actually read. Describing the result set as a `Table` lets
`row_count`, `required_columns` and `null_rate` mean the same thing on both
paths without a second library.

Two things cannot be the same, and both fail closed rather than pretending:

`destination_exists` and `output_exists` ask about something a query does not
produce, so they report themselves unsupported here.

A truncated result describes the rows that came back, not the rows the query
matched. Asserting a row count against `max_rows` worth of a larger answer
would be measuring the policy rather than the data, so a bounded result makes
the count-based checks unsupported too.
"""

from __future__ import annotations

from collections.abc import Sequence

from gantry.sql.output import InlineRows
from gantry.sql.schema import Column, Table
from gantry.verifier import CheckResult

RESULT_SET = "(result set)"


def result_set_table(inline: InlineRows | None, *, null_rate_columns: Sequence[str] = ()) -> Table:
    """A `Table` describing the rows a query returned.

    Types are unknown: the inline form carries values, not a column schema, and
    inventing one would be worse than admitting it. `required_columns` reads
    names, which is what this can honestly supply.
    """
    if inline is None:
        return Table(RESULT_SET, None, None, ())
    columns = tuple(Column(name, "unknown", nullable=True) for name in inline.columns)
    metadata: dict[str, object] = {"rows": len(inline.rows), "truncated": inline.truncated}
    if null_rate_columns:
        metadata["null_rates"] = _null_rates(inline, null_rate_columns)
    return Table(RESULT_SET, None, None, columns, kind="result set", metadata=metadata)


def _null_rates(inline: InlineRows, columns: Sequence[str]) -> dict[str, float]:
    """How often each requested column is null, over the returned rows."""
    rates: dict[str, float] = {}
    if not inline.rows:
        # Nothing to be null in. A rate of zero would claim a measurement that
        # no row supports, so the column is left out and the check reports
        # itself unsupported.
        return rates
    for column in columns:
        if column not in inline.columns:
            continue
        index = inline.columns.index(column)
        nulls = sum(1 for row in inline.rows if row[index] is None)
        rates[column] = nulls / len(inline.rows)
    return rates


def unsupported_for_query(check: object, reason: str) -> CheckResult:
    """A check that cannot mean anything against a query's rows."""
    return CheckResult(
        name=_check_name(check),
        ok=False,
        message=reason,
        supported=False,
        source="gantry",
    )


def applies_to_result_set(check: object, *, truncated: bool) -> str | None:
    """Why this check cannot be evaluated against a result set, if it cannot."""
    if getattr(check, "requires_destination", False):
        return "this check needs a destination, and a query does not create one"
    if (
        truncated
        and getattr(check, "requires_row_count", False)
        and not getattr(check, "allows_truncated", False)
    ):
        return (
            "the result was truncated by max_rows, so its row count describes "
            "the rows returned rather than the rows matched"
        )
    if truncated and tuple(getattr(check, "null_rate_columns", ())):
        return (
            "the result was truncated by max_rows, so a null rate over it "
            "describes the rows returned rather than the rows matched"
        )
    return None


def _check_name(check: object) -> str:
    mapping = {
        "DestinationExists": "destination_exists",
        "OutputExists": "output_exists",
        "RowCount": "row_count",
        "RequiredColumns": "required_columns",
        "NullRate": "null_rate",
    }
    name = type(check).__name__
    return mapping.get(name, name)


__all__ = [
    "RESULT_SET",
    "applies_to_result_set",
    "result_set_table",
    "unsupported_for_query",
]
