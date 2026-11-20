# SPDX-License-Identifier: Apache-2.0
"""Stats only: numbers about groups, never rows, never a group too small to hide in.

`db.query(stats_only=StatsOnly(min_group_size=10))` lets an agent write its
own SQL, joins, subqueries and CTEs included, and answers only what describes
groups. `check` reads the statement before it runs; `guard` adds, for every
group the answer has, how many distinct records of each table it came from;
after it runs, the whole answer is refused if any group came from fewer
records than the minimum. Nothing is returned from a refused answer, not even
which group was small.

The one rule all of it serves: **every number in the answer is computed over
at least `min_group_size` distinct records of every table in the outer FROM.**

- **What comes out is aggregated.** Each column of the outer SELECT is a group
  key or an aggregate: `count`, `sum`, `avg`, `stddev` or `variance` of a plain
  column. `min`, `max`, `string_agg`, percentiles and the like return a row's
  own value and are refused. So is `sum(CASE WHEN id = 5 THEN salary END)`,
  one row's value summed over many: a condition goes in WHERE or GROUP BY,
  where it shrinks the group the minimum is checked against.
- **Records, not rows.** A join can repeat one record many times, so that a
  group of one counts as ten thousand. With more than one table in the outer
  FROM, each one's records are counted apart by its primary key, read with
  `describe()`. A single table needs no key: its rows are its records.
- **Anything as a filter.** WHERE and ON take any condition, subqueries and CTEs
  included (`NOT EXISTS`, `IN (SELECT ...)`): a filter only takes records out
  of a group, and the group left is what is counted. What is counted, the
  outer FROM, is real tables; a subquery or CTE there could repeat rows that
  cannot be seen into.

What it does not stop: two answers over overlapping groups, each large
enough, can still differ by a small group. Every query is a recorded run, so a
run of near-identical questions is there to be seen.

A minimum of 0 checks the statement and no group's size. Needs the
`redaction` extra, which brings the parser.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from gantry.sql.redaction import _read_dialect

try:  # the `redaction` extra
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ParseError
except ImportError:  # pragma: no cover - exercised only without the extra
    sqlglot = None  # type: ignore[assignment]

#: The columns `guard` adds start with this, and are taken out before the answer is returned.
COUNTED = "gantry_records_"

#: Aggregates that summarise a group without returning any one row's value.
ALLOWED: tuple[type, ...] = ()
if sqlglot is not None:
    ALLOWED = (
        exp.Count,
        exp.Sum,
        exp.Avg,
        exp.Stddev,
        exp.StddevPop,
        exp.StddevSamp,
        exp.Variance,
        exp.VariancePop,
    )
#: Said by name when refused, so the agent knows what to use instead.
OFFERED = "count, count(DISTINCT ...), sum, avg, stddev and variance"


@dataclass(frozen=True, slots=True)
class StatsOnly:
    """Answer only aggregates, and refuse an answer with a group under `min_group_size` records."""

    min_group_size: int = 10

    def __post_init__(self) -> None:
        if isinstance(self.min_group_size, bool) or not isinstance(self.min_group_size, int):
            raise TypeError("min_group_size must be an integer")
        if self.min_group_size < 0:
            raise ValueError("min_group_size must not be negative")
        if sqlglot is None:  # pragma: no cover - exercised only without the extra
            raise ImportError("stats only needs the parser: pip install 'data-gantry[redaction]'")


class StatsError(ValueError):
    """A statement a stats-only connection will not run. Said to the agent as it is."""


@dataclass(frozen=True, slots=True)
class Source:
    """A table in the outer FROM: what it is called there, and what it is."""

    alias: str
    name: str
    schema: str | None


@dataclass(frozen=True, slots=True)
class Checked:
    select: exp.Select
    sources: tuple[Source, ...]


# ------------------------------------------------------------------ checking


def check(sql: str, dialect: str) -> Checked:
    """The statement, parsed, if everything it returns is an aggregate."""
    try:
        statements = [found for found in sqlglot.parse(sql, read=_read_dialect(dialect)) if found]
    except ParseError as error:
        raise StatsError(f"the statement could not be read: {_first_line(error)}") from error
    if len(statements) != 1:
        raise StatsError("one statement at a time")
    [select] = statements
    if not isinstance(select, exp.Select):
        raise StatsError(
            "only a SELECT of counts and aggregates is answered here; "
            "for UNION, ask each part on its own"
        )
    for clause in ("into", "locks"):
        if select.args.get(clause):
            raise StatsError("a plain SELECT only")

    sources = _sources(select)
    returned = [*select.expressions]
    group = select.args.get("group")
    having = select.args.get("having")
    for part in (*returned, group, having):
        if part is None:
            continue
        for node in part.walk():
            if isinstance(node, exp.Select):
                raise StatsError(
                    "a subquery belongs in WHERE (IN, EXISTS), not in what is returned"
                )
            if isinstance(node, exp.Window):
                raise StatsError("window functions return a value per row; use GROUP BY")
            if isinstance(node, exp.Filter):
                raise StatsError(
                    "an aggregate's FILTER picks rows out of a group; use WHERE or GROUP BY"
                )
            if isinstance(node, exp.Star) and not isinstance(node.parent, exp.Count):
                raise StatsError("`*` is only for count(*); name the aggregates you want")

    keys = _group_keys(select)
    if not returned:
        raise StatsError("the SELECT list is empty")
    for projection in returned:
        target = projection.unalias()
        if _key(target) in keys:
            continue
        _aggregate_only(target, "the SELECT list", keys)
    if having is not None:
        _aggregate_only(having.this, "HAVING", keys)
    return Checked(select=select, sources=sources)


def _sources(select: exp.Select) -> tuple[Source, ...]:
    """The tables in the outer FROM and its joins. Each must be a real table."""
    named = select.args.get("with_") or select.args.get("with")
    ctes = {cte.alias_or_name.lower() for cte in named.expressions} if named else set()
    start = select.args.get("from_") or select.args.get("from")
    if start is None:
        raise StatsError("FROM names the table to count")
    found: list[Source] = []
    for item in [start.this, *(join.this for join in select.args.get("joins") or ())]:
        if not isinstance(item, exp.Table) or not isinstance(item.this, exp.Identifier):
            raise StatsError(
                "what is counted (the outer FROM and its joins) is tables; a subquery or "
                "function there goes in WHERE instead (IN, EXISTS)"
            )
        if not item.db and item.name.lower() in ctes:
            raise StatsError(
                f"{item.name} is a CTE: use it in WHERE (IN, EXISTS), and count a table"
            )
        found.append(Source(alias=item.alias_or_name, name=item.name, schema=item.db or None))
    aliases = [source.alias.lower() for source in found]
    if len(set(aliases)) != len(aliases):
        raise StatsError("give each table in FROM its own alias")
    return tuple(found)


def _group_keys(select: exp.Select) -> set[str]:
    """Each GROUP BY expression, as SQL, with positions and aliases resolved."""
    group = select.args.get("group")
    if group is None:
        return set()
    if any(group.args.get(kind) for kind in ("rollup", "cube", "grouping_sets")):
        raise StatsError("GROUP BY a list of expressions; ROLLUP, CUBE and GROUPING SETS are not")
    named = {projection.alias: projection.unalias() for projection in select.expressions}
    keys: set[str] = set()
    for item in group.expressions:
        if isinstance(item, exp.Literal) and not item.is_string:
            position = int(item.name)
            if not 1 <= position <= len(select.expressions):
                raise StatsError(f"GROUP BY {position} is not a column of the SELECT list")
            item = select.expressions[position - 1].unalias()
        elif isinstance(item, exp.Column) and not item.table and item.name in named:
            item = named[item.name]
        if item.find(exp.AggFunc):
            raise StatsError("GROUP BY cannot hold an aggregate")
        keys.add(_key(item))
    return keys


def _aggregate_only(node: exp.Expression, where: str, keys: set[str]) -> None:
    """Every column in `node` is a group key or an allowed aggregate's plain argument."""
    for aggregate in node.find_all(exp.AggFunc, exp.WithinGroup):
        if not isinstance(aggregate, ALLOWED):
            name = aggregate.sql(dialect="postgres").split("(")[0].lower()
            raise StatsError(
                f"{name} returns a row's own value, so it is not offered; use {OFFERED}"
            )
        argument = aggregate.this
        if isinstance(argument, exp.Distinct):
            if len(argument.expressions) != 1:
                raise StatsError("count(DISTINCT ...) takes one column")
            argument = argument.expressions[0]
        if not isinstance(argument, exp.Column | exp.Star):
            raise StatsError(
                "an aggregate takes a plain column, not an expression: "
                f"{aggregate.sql(dialect='postgres')}. Put a condition in WHERE or "
                "GROUP BY instead, so the group it picks is the one counted"
            )
    for column in node.find_all(exp.Column):
        if column.find_ancestor(exp.AggFunc) is not None:
            continue
        if any(_key(ancestor) in keys for ancestor in _up_to(column, node)):
            continue
        raise StatsError(
            f"{column.sql(dialect='postgres')} in {where} is neither grouped nor aggregated; "
            f"group by it, or wrap it in one of {OFFERED}"
        )


def _up_to(node: exp.Expression, top: exp.Expression) -> Sequence[exp.Expression]:
    chain = [node]
    while chain[-1] is not top:
        parent = chain[-1].parent
        if not isinstance(parent, exp.Expression):
            break
        chain.append(parent)
    return chain


def _key(node: exp.Expression) -> str:
    return node.sql(dialect="postgres", normalize=True)


def _first_line(error: Exception) -> str:
    return str(error).splitlines()[0][:200]


# ------------------------------------------------------------- guarding


def guard(
    checked: Checked,
    dialect: str,
    primary_keys: Mapping[Source, Sequence[str]] | None = None,
) -> str:
    """The checked statement, with how many records each group came from, last.

    One table: its rows are its records, so `count(*)`. Several: each table's
    records are counted apart by its primary key, which `primary_keys` gives;
    a table without one cannot be told apart in a join and is refused.
    """
    counted = checked.select.copy()
    if len(checked.sources) == 1:
        counted.select(exp.alias_(exp.Count(this=exp.Star()), f"{COUNTED}0"), copy=False)
        return counted.sql(dialect=_read_dialect(dialect))
    for index, source in enumerate(checked.sources):
        key = list((primary_keys or {}).get(source) or ())
        if not key:
            raise StatsError(
                f"{source.name} has no primary key, so its records cannot be counted apart "
                "in a join; ask about it on its own, or use it in WHERE (IN, EXISTS)"
            )
        columns: list[exp.Expression] = [exp.column(part, table=source.alias) for part in key]
        if len(columns) == 1 or _read_dialect(dialect) == "mysql":
            # MySQL counts distinct combinations of several columns as they are.
            distinct = exp.Distinct(expressions=columns)
        else:
            distinct = exp.Distinct(expressions=[exp.Tuple(expressions=columns)])
        counted.select(exp.alias_(exp.Count(this=distinct), f"{COUNTED}{index}"), copy=False)
    return counted.sql(dialect=_read_dialect(dialect))


def too_small(
    columns: Sequence[str], rows: Sequence[Sequence[object]], *, min_group_size: int
) -> bool:
    """Whether any group in the answer came from fewer records than the minimum."""
    if min_group_size <= 0:
        return False
    counts = [index for index, column in enumerate(columns) if str(column).startswith(COUNTED)]
    return any(_count(row[index]) < min_group_size for row in rows for index in counts)


def _count(value: object) -> int:
    """A record count as the driver gave it (int, Decimal, text); unreadable counts as none."""
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return 0


def without_counts(
    columns: Sequence[str], rows: Sequence[Sequence[object]]
) -> tuple[tuple[str, ...], tuple[tuple[object, ...], ...]]:
    """The answer as the agent sees it: the record counts taken out."""
    keep = [index for index, column in enumerate(columns) if not str(column).startswith(COUNTED)]
    return (
        tuple(columns[index] for index in keep),
        tuple(tuple(row[index] for index in keep) for row in rows),
    )


def primary_keys(
    sources: Sequence[Source], tables: Sequence[Any], default_schema: str | None
) -> dict[Source, tuple[str, ...]]:
    """Each source's primary key, from the connection's own schema listing."""
    found: dict[Source, tuple[str, ...]] = {}
    for source in sources:
        schema = (source.schema or default_schema or "").lower()
        matches = [
            table
            for table in tables
            if table.name.lower() == source.name.lower() and (table.schema or "").lower() == schema
        ]
        found[source] = tuple(matches[0].primary_key) if matches else ()
    return found


__all__ = [
    "COUNTED",
    "Checked",
    "Source",
    "StatsError",
    "StatsOnly",
    "check",
    "guard",
    "primary_keys",
    "too_small",
    "without_counts",
]
