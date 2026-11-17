# SPDX-License-Identifier: Apache-2.0
"""Rows handed to a write as values, inserted as parameters.

`SQLWrite.insert_rows` puts rows a caller already has (copied from another
database, say) into a table. The values never become SQL text: they travel to
the adapter beside the statement, and the adapter binds them as parameters, so
no value can change what the statement does, whatever it contains.

The table and column names do become SQL text, so they are held to plain
identifiers here (letters, digits and underscores, not starting with a digit)
and quoted by the adapter in its own dialect. A name outside that is refused
rather than escaped: escaping identifiers by hand is exactly the kind of code
that is correct until it is not.

What the run records is the statement with `?` where the values go, and a
digest of the values, so two runs can be compared without the record holding
the rows themselves.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

#: The context key that carries a `RowLoad` to the adapter.
ROW_LOAD = "gantry.sql.row_load"

#: A name that is safe to put in a statement once quoted.
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

#: The value types a load accepts. Anything else is refused before anything
#: runs, so a value the driver would turn into something surprising is never
#: sent at all.
VALUE_TYPES: tuple[type, ...] = (
    bool,
    int,
    float,
    str,
    bytes,
    Decimal,
    datetime,
    date,
    time,
    timedelta,
    UUID,
)


@dataclass(frozen=True, slots=True)
class RowLoad:
    """Rows to insert into one table, by column, as parameters."""

    table: str
    """`name` or `schema.name`, each part a plain identifier."""
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]

    @property
    def parts(self) -> tuple[str, ...]:
        return tuple(self.table.split("."))

    def statement(self, quote: Callable[[str], str], placeholder: Callable[[int], str]) -> str:
        """The insert as one adapter runs it: its quoting, its placeholders."""
        table = ".".join(quote(part) for part in self.parts)
        columns = ", ".join(quote(column) for column in self.columns)
        marks = ", ".join(placeholder(index) for index in range(1, len(self.columns) + 1))
        return f"INSERT INTO {table} ({columns}) VALUES ({marks})"

    @property
    def proposal(self) -> str:
        """The statement as the run records it and the policy reads it."""
        return self.statement(lambda name: name, lambda _index: "?")

    @property
    def digest(self) -> str:
        """A digest of the values, in order: the same rows give the same digest."""
        return digest(self.rows)


def problem(table: str, columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str | None:
    """Why these rows cannot be loaded as given, or `None` if they can."""
    parts = table.split(".")
    if len(parts) > 2 or not all(IDENTIFIER.match(part) for part in parts):
        return (
            f"{table!r} cannot name a table here: use name or schema.name, each made of "
            "letters, digits and underscores"
        )
    if not columns:
        return "name at least one column"
    for column in columns:
        if not isinstance(column, str) or not IDENTIFIER.match(column):
            return (
                f"{column!r} cannot name a column here: use letters, digits and underscores, "
                "not starting with a digit"
            )
    lowered = [column.lower() for column in columns]
    if len(set(lowered)) != len(lowered):
        return "a column is named twice"
    if not rows:
        return "there are no rows to insert"
    for number, row in enumerate(rows, start=1):
        if len(row) != len(columns):
            return f"row {number} has {len(row)} values for {len(columns)} columns"
        for value in row:
            if value is not None and not isinstance(value, VALUE_TYPES):
                return (
                    f"row {number} has a {type(value).__name__}, which cannot be inserted as a "
                    "value here"
                )
    return None


def digest(rows: Sequence[Sequence[object]]) -> str:
    """SHA-256 over each value's type and text, row by row, in order."""
    hashed = sha256()
    for row in rows:
        for value in row:
            hashed.update(f"{type(value).__name__}:{value!r}\x1f".encode())
        hashed.update(b"\x1e")
    return hashed.hexdigest()


def load_of(metadata: Mapping[str, object]) -> RowLoad | None:
    """The load an execution context carries, if it carries one."""
    load = metadata.get(ROW_LOAD)
    return load if isinstance(load, RowLoad) else None


__all__ = ["IDENTIFIER", "ROW_LOAD", "VALUE_TYPES", "RowLoad", "digest", "load_of", "problem"]
