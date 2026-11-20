# SPDX-License-Identifier: Apache-2.0
"""Column redaction: rows come back, a sensitive column's real value does not.

A `Redaction` names columns (`public.users.email`, `*.*.phone`) and how each
is masked:

- `hide`: the column reads as `[redacted]` where it is returned, and as NULL
  wherever else the statement uses it.
- `hash`: the column reads as a stable token, `h_` and 16 hex digits, keyed
  with a secret the agent never sees. The same value gives the same token, so
  joins, `count(DISTINCT ...)` and grouping by person still work; the value
  does not come back, and without the key a guess cannot be hashed to check
  it. Meant for identifiers: on a column with few values (a plan, a zip code)
  a token is no better than the value.

**Masked at the source, not in the result.** Masking a result by column name
is undone by an alias, a function or a WHERE clause: `SELECT email AS e`,
`lower(email)`, `WHERE email LIKE 'a%'`. So a query is rewritten before it
runs: every column the statement reads is resolved to the table it comes
from, `*` included, through CTEs, derived tables, joins and subqueries, and
each redacted one is replaced by its mask wherever it appears. The column then
*is* the mask everywhere in the statement, and a WHERE on it tests the mask.

**What cannot be resolved is refused.** A statement the parser cannot read,
a table the connection's schema does not list (`pg_stats`), a reference to a
whole row (`row_to_json(u)`, `u::text`), and any function the parser does not
model (`query_to_xml`, which runs SQL from a string) are refused rather than
guessed at.

Writes are checked, not rewritten: a write may set a redacted column but not
read one (`SET notes = email` would copy it out), and returned rows show the
column as `[redacted]`. A materialization that reads a redacted column is
refused, since what it creates would have to hold the mask, and a view would
hold the key.

What it does not cover: a view in the database that exposes a redacted column
under another name is a different column, and has to be named too.

Needs the `redaction` extra (`pip install data-gantry[redaction]`), which
brings the parser.
"""

from __future__ import annotations

import fnmatch
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast

try:  # the `redaction` extra; checked when a Redaction is made
    import sqlglot
    from sqlglot import exp
    from sqlglot.optimizer.qualify import qualify
    from sqlglot.optimizer.scope import Scope, traverse_scope
except ImportError:  # pragma: no cover - exercised only without the extra
    sqlglot = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from gantry.sql.schema import Table

#: How a hidden column reads where it is returned.
HIDDEN = "[redacted]"

_PART = re.compile(r"^[A-Za-z0-9_*?$-]+$")
_KEY = re.compile(r"^[A-Za-z0-9_-]{16,128}$")


class RedactionMode(StrEnum):
    HIDE = "hide"
    HASH = "hash"


class RedactionError(ValueError):
    """A statement redaction cannot vouch for. Refused, and said as it is."""


@dataclass(frozen=True, slots=True)
class Redaction:
    """Which columns are masked, and how.

    `columns` maps `schema.table.column` patterns, with `*` and `?`
    wildcards, to a mode. Matching ignores case. `key` keys `hash` and is
    required when any column is hashed: 16 to 128 letters, digits, `-` or `_`.
    """

    columns: Mapping[str, RedactionMode | str]
    key: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        normalized: dict[str, RedactionMode] = {}
        for pattern, mode in self.columns.items():
            parts = pattern.strip().lower().split(".")
            if len(parts) != 3 or not all(_PART.match(part) for part in parts):
                raise ValueError(f"a redacted column is schema.table.column: {pattern!r}")
            normalized[".".join(parts)] = RedactionMode(mode)
        if RedactionMode.HASH in normalized.values() and not _KEY.match(self.key):
            raise ValueError("hashing needs a key of 16 to 128 letters, digits, - or _")
        object.__setattr__(self, "columns", normalized)
        _parser()  # fail at configuration, not at the first query

    def mode_for(self, schema: str | None, table: str, column: str) -> RedactionMode | None:
        name = f"{schema or ''}.{table}.{column}".lower()
        for pattern, mode in self.columns.items():
            if fnmatch.fnmatchcase(name, pattern):
                return mode  # type: ignore[return-value]
        return None

    def masked_in(self, table: Table) -> dict[str, RedactionMode]:
        """The table's redacted columns, by name."""
        found = {}
        for column in table.columns:
            mode = self.mode_for(table.schema, table.name, column.name)
            if mode is not None:
                found[column.name] = mode
        return found


@dataclass(frozen=True, slots=True)
class Rewritten:
    """The statement to run, and which columns it masked."""

    sql: str
    masked: tuple[str, ...]


# ------------------------------------------------------------------ queries


def rewrite(
    sql: str,
    *,
    dialect: str,
    tables: Sequence[Table],
    redaction: Redaction,
    default_schema: str | None,
) -> Rewritten:
    """`sql` with every redacted column replaced by its mask, or a refusal."""
    read = _read_dialect(dialect)
    tree = _one_statement(sql, read)
    if not isinstance(tree, exp.Query):
        raise RedactionError("only a SELECT can be read here")
    _refuse_unmodelled(tree)

    known = _known(tables)
    _listed_only(tree, known, default_schema)
    types = {
        ((table.schema or "").lower(), table.name.lower(), column.name.lower()): column.type
        for table in tables
        for column in table.columns
    }
    try:
        qualified = qualify(
            tree,
            schema=_schema_of(tables),
            db=default_schema,
            dialect=read,
            validate_qualify_columns=True,
            quote_identifiers=False,
        )
    except Exception as error:
        raise RedactionError(
            f"the statement's columns could not be resolved: {_short(error)}"
        ) from error

    for whole in qualified.find_all(exp.TableColumn):
        raise RedactionError(f"{whole.sql(read)} reads a whole row; name its columns")

    masked: dict[str, None] = {}
    replacements: list[tuple[exp.Column, exp.Expression]] = []
    for scope in traverse_scope(qualified):
        for column in scope.columns:
            source = _source_of(scope, column.table) if column.table else None
            if source is None:
                raise RedactionError(f"{column.sql(read)} is not a column of any table here")
            if not isinstance(source, exp.Table):
                continue  # a CTE or derived table: its own columns were rewritten at their source
            table = _table_of(source, known, default_schema)
            mode = redaction.mode_for(table[0], source.name, column.name)
            if mode is None:
                continue
            masked[f"{table[0]}.{source.name}.{column.name}"] = None
            kind = types.get((table[0].lower(), source.name.lower(), column.name.lower()))
            replacements.append((column, _mask(column, mode, read, redaction.key, kind)))
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                _table_of(source, known, default_schema)

    for column, mask in replacements:
        parent = column.parent
        if isinstance(parent, exp.Alias) or not _is_output(column):
            column.replace(mask)
        else:
            column.replace(exp.alias_(mask, column.name))
    _restore_outputs(qualified, sql)
    return Rewritten(sql=qualified.sql(dialect=read), masked=tuple(masked))


_GENERATED = re.compile(r"^_col_\d+$")


def _restore_outputs(tree: exp.Expression, original: str) -> None:
    """Take off the `_col_N` names the resolver gave unnamed outputs.

    Only on the outermost SELECT, whose columns the caller sees: there the
    database names an unnamed output itself (`lower`, `count`), as it would
    have without the rewrite. An inner one may be referred to by that name.
    """
    selects: list[exp.Expression] = [tree]
    while selects:
        node = selects.pop()
        if isinstance(node, exp.SetOperation):
            selects.extend(cast("list[exp.Expression]", [node.left, node.right]))
            continue
        if not isinstance(node, exp.Select):
            continue
        for projection in node.expressions:
            if not _GENERATED.match(projection.alias) or projection.alias in original:
                continue
            if isinstance(projection, exp.Alias):
                projection.replace(projection.this)
            elif isinstance(projection, exp.Subquery):
                projection.set("alias", None)


def _source_of(scope: Scope | None, alias: str) -> Any:
    """What `alias` names in this scope, or in the one enclosing it (a correlated column)."""
    while scope is not None:
        if alias in scope.sources:
            return scope.sources[alias]
        scope = scope.parent
    return None


def _mask(
    column: exp.Column, mode: RedactionMode, dialect: str, key: str, kind: str | None
) -> exp.Expression:
    target = column.sql(dialect)
    if mode is RedactionMode.HIDE:
        if _is_output(column) or isinstance(column.parent, exp.Alias):
            return exp.Literal.string(HIDDEN)
        return _null(dialect, kind)
    if dialect == "mysql":
        template = f"CONCAT('h_', LEFT(MD5(CONCAT('{key}', {target})), 16))"
    else:
        template = f"('h_' || LEFT(MD5('{key}' || CAST({target} AS TEXT)), 16))"
    return cast("exp.Expression", sqlglot.parse_one(template, read=dialect))


def _null(dialect: str, kind: str | None) -> exp.Expression:
    """A NULL standing in for a hidden column inside the statement.

    Typed as the column on PostgreSQL, so a comparison, a function or an
    ORDER BY on it still type-checks (a bare `ORDER BY NULL` is refused
    there). MySQL takes a bare NULL everywhere, and its CAST names few types.
    """
    if dialect == "mysql" or not kind:
        return exp.Null()
    try:
        to = exp.DataType.build(kind, dialect=dialect)
    except Exception:
        return exp.Null()  # a type the parser does not know: an enum, an array
    if to.this == exp.DataType.Type.USERDEFINED:
        return exp.Null()
    return exp.Cast(this=exp.Null(), to=to)


def _is_output(column: exp.Column) -> bool:
    """Whether the column is itself one of a SELECT's outputs, aliased or not."""
    node: Any = column
    if isinstance(node.parent, exp.Alias):
        node = node.parent
    parent = node.parent
    return isinstance(parent, exp.Select) and node in parent.expressions


# ------------------------------------------------------------------- writes


def reads_redacted(
    sql: str,
    *,
    dialect: str,
    tables: Sequence[Table],
    redaction: Redaction,
    default_schema: str | None,
) -> tuple[str, ...]:
    """The redacted columns a write or materialization reads, as `table.column`.

    Conservative: a column read by name is matched against every table the
    statement names, so an unqualified `email` counts if any of them has a
    redacted `email`, and `*` counts every redacted column of every table it
    could cover. A column being set (`SET email = ...`, `INSERT (email)`) is
    not a read.
    """
    read = _read_dialect(dialect)
    tree = _one_statement(sql, read)
    _refuse_unmodelled(tree)
    known = _known(tables)
    named: dict[str, tuple[str, dict[str, RedactionMode]]] = {}
    for source in tree.find_all(exp.Table):
        if not isinstance(source.this, exp.Identifier):
            continue
        schema, columns = _table_of(source, known, default_schema, strict=False)
        modes = {
            name: mode
            for name in columns
            if (mode := redaction.mode_for(schema, source.name, name)) is not None
        }
        named[source.alias_or_name.lower()] = (f"{schema}.{source.name}", modes)
        named.setdefault(source.name.lower(), named[source.alias_or_name.lower()])

    set_targets = _set_targets(tree)
    found: dict[str, None] = {}
    for column in tree.find_all(exp.Column):
        if id(column) in set_targets:
            continue
        candidates = (
            [named[column.table.lower()]]
            if column.table and column.table.lower() in named
            else named.values()
        )
        for table, modes in candidates:
            for name in modes:
                if name.lower() == column.name.lower():
                    found[f"{table}.{name}"] = None
            if column.name.lower() in named and not column.table:
                found.update({f"{table}.{name}": None for name in modes})  # a whole row
    for star in tree.find_all(exp.Star):
        if isinstance(star.parent, exp.Count) or star.find_ancestor(exp.Returning):
            continue  # `RETURNING *` comes back masked: see `mask_rows`
        qualifier = star.parent.table if isinstance(star.parent, exp.Column) else ""
        candidates = (
            [named[qualifier.lower()]]
            if qualifier and qualifier.lower() in named
            else named.values()
        )
        for table, modes in candidates:
            found.update({f"{table}.{name}": None for name in modes})
    return tuple(found)


def _set_targets(tree: exp.Expression) -> set[int]:
    """The columns a write assigns to, which it does not read."""
    targets: set[int] = set()
    if isinstance(tree, exp.Update):
        for assignment in tree.expressions:
            if isinstance(assignment, exp.EQ) and isinstance(assignment.this, exp.Column):
                targets.add(id(assignment.this))
    if isinstance(tree, exp.Insert) and isinstance(tree.this, exp.Schema):
        targets.update(id(column) for column in tree.this.find_all(exp.Column))
        targets.update(id(column) for column in tree.this.expressions)
    return targets


def mask_rows(
    columns: Sequence[str], rows: Iterable[Sequence[object]], hidden: Iterable[str]
) -> list[list[object]]:
    """Rows with each named column shown as `[redacted]`, for what a write returns."""
    wanted = {name.lower() for name in hidden}
    positions = [index for index, name in enumerate(columns) if name.lower() in wanted]
    masked = []
    for row in rows:
        copy = list(row)
        for index in positions:
            if copy[index] is not None:
                copy[index] = HIDDEN
        masked.append(copy)
    return masked


def scrub(message: str, redaction: Redaction) -> str:
    """An error as the agent may see it: the key never appears in one."""
    return message.replace(redaction.key, "[key]") if redaction.key else message


# ------------------------------------------------------------------ helpers


def _parser() -> None:
    if sqlglot is None:  # pragma: no cover - exercised only without the extra
        raise ImportError("column redaction needs the parser: pip install 'data-gantry[redaction]'")


def _read_dialect(dialect: str) -> str:
    return {
        "postgresql": "postgres",
        "postgres": "postgres",
        "mysql": "mysql",
        "duckdb": "duckdb",
    }.get(dialect.lower(), "postgres")


def _one_statement(sql: str, dialect: str) -> exp.Expression:
    try:
        statements = [found for found in sqlglot.parse(sql, read=dialect) if found is not None]
    except Exception as error:
        raise RedactionError(f"the statement could not be read: {_short(error)}") from error
    if len(statements) != 1:
        raise RedactionError("one statement at a time")
    return cast("exp.Expression", statements[0])


def _refuse_unmodelled(tree: exp.Expression) -> None:
    for function in tree.find_all(exp.Anonymous):
        raise RedactionError(
            f"{function.name} is not a function redaction can follow, so it is not run here"
        )


def _listed_only(
    tree: exp.Expression,
    known: Mapping[tuple[str, str], tuple[str, list[str]]],
    default_schema: str | None,
) -> None:
    """Refuse a table the connection does not list, such as a system catalogue."""
    ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for source in tree.find_all(exp.Table):
        if not isinstance(source.this, exp.Identifier):
            raise RedactionError(f"{source.sql()} is not a table redaction can follow")
        if not source.db and source.name.lower() in ctes:
            continue
        _table_of(source, known, default_schema)


def _known(tables: Sequence[Table]) -> dict[tuple[str, str], tuple[str, list[str]]]:
    return {
        ((table.schema or "").lower(), table.name.lower()): (
            table.schema or "",
            [column.name for column in table.columns],
        )
        for table in tables
    }


def _table_of(
    source: exp.Table,
    known: Mapping[tuple[str, str], tuple[str, list[str]]],
    default_schema: str | None,
    *,
    strict: bool = True,
) -> tuple[str, list[str]]:
    schema = (source.db or default_schema or "").lower()
    found = known.get((schema, source.name.lower()))
    if found is None:
        if strict:
            raise RedactionError(f"{source.name} is not a table this connection lists")
        return schema, []
    return found


def _schema_of(tables: Sequence[Table]) -> dict[str, object]:
    shaped: dict[str, Any] = {}
    for table in tables:
        shaped.setdefault(table.schema or "", {})[table.name] = {
            column.name: "TEXT" for column in table.columns
        }
    return shaped


def _short(error: Exception) -> str:
    return str(error).splitlines()[0][:200] if str(error) else type(error).__name__


__all__ = [
    "HIDDEN",
    "Redaction",
    "RedactionError",
    "RedactionMode",
    "Rewritten",
    "mask_rows",
    "reads_redacted",
    "rewrite",
    "scrub",
]
