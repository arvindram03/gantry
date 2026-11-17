# SPDX-License-Identifier: Apache-2.0
"""Everything a database's schemas hold, read whole: for moving them elsewhere.

`SQLConnection.snapshot(schemas)` reads every object in the named schemas
(extensions they use, types, sequences, tables with their columns,
constraints, indexes, views, functions, triggers and row-level security
policies) as the engine itself describes them. Where the engine can say how
to create an object, `definition` holds that statement in its own dialect, so
moving between two databases of the same engine loses nothing.

It reads structure only, never rows, and only in the schemas asked for. It is
what a migration plans from, and what one checks the destination against once
it has run.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum


class ObjectKind(StrEnum):
    """What a schema object is, in the order a schema is built."""

    EXTENSION = "extension"
    SCHEMA = "schema"
    ENUM = "enum"
    SEQUENCE = "sequence"
    FUNCTION = "function"
    TABLE = "table"
    CONSTRAINT = "constraint"
    INDEX = "index"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    TRIGGER = "trigger"
    POLICY = "policy"


@dataclass(frozen=True, slots=True)
class Column:
    """One column of a table, as the engine describes it."""

    name: str
    data_type: str
    """As the engine writes it: `character varying(64)`, `numeric(10,2)`, `jsonb`."""
    nullable: bool
    default: str | None = None
    """The default expression, as the engine writes it; `None` when there is none."""
    identity: str | None = None
    """`always` or `by default` for an identity column; `None` otherwise."""
    generated: str | None = None
    """The expression of a stored generated column; `None` otherwise."""


@dataclass(frozen=True, slots=True)
class SchemaObject:
    """One object, as the engine describes it.

    `name` is the object's own name; `table` is the table it belongs to, for
    a constraint, index, trigger or policy. `definition` is how the engine
    would create it, in its own dialect, where it can say; `details` carries
    what a planner needs besides (a constraint's type, a function's language,
    a table's columns, the column that owns a sequence).
    """

    kind: ObjectKind
    schema: str
    name: str
    definition: str | None = None
    table: str | None = None
    columns: tuple[Column, ...] = ()
    details: Mapping[str, object] = field(default_factory=dict)

    @property
    def qualified_name(self) -> str:
        """`schema.name`, or `schema.table.name` for something that belongs to a table."""
        if self.kind is ObjectKind.EXTENSION:
            return self.name
        if self.kind is ObjectKind.SCHEMA:
            return self.name
        if self.table is not None:
            return f"{self.schema}.{self.table}.{self.name}"
        return f"{self.schema}.{self.name}"

    @property
    def target(self) -> str:
        """What a change to it changes, as policy names it: its schema and table or name."""
        if self.kind in (ObjectKind.EXTENSION, ObjectKind.SCHEMA):
            return f"{self.name}.*" if self.kind is ObjectKind.SCHEMA else self.name
        return f"{self.schema}.{self.table or self.name}"


@dataclass(frozen=True, slots=True)
class SchemaSnapshot:
    """The objects in some schemas of one database, in the order they are built."""

    engine: str
    schemas: tuple[str, ...]
    objects: tuple[SchemaObject, ...]

    def of(self, kind: ObjectKind) -> tuple[SchemaObject, ...]:
        return tuple(item for item in self.objects if item.kind is kind)

    def find(self, kind: ObjectKind, qualified_name: str) -> SchemaObject | None:
        return next(
            (
                item
                for item in self.objects
                if item.kind is kind and item.qualified_name == qualified_name
            ),
            None,
        )


__all__ = ["Column", "ObjectKind", "SchemaObject", "SchemaSnapshot"]
