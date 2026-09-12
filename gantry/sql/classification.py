# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SQLOperation(StrEnum):
    """What a statement does, at the granularity policy decisions need.

    `MULTI_STATEMENT` is its own operation rather than a list, because a batch
    is refused as a batch unless the policy allows several statements.
    `UNKNOWN` is deny-by-default: an unclassifiable statement is not a
    `SELECT`.
    """

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    MERGE = "MERGE"
    DDL = "DDL"
    MULTI_STATEMENT = "MULTI_STATEMENT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class SQLObjectRef:
    """A possibly-qualified reference to a table as it appeared in the SQL.

    `schema` and `catalog` are `None` when the statement did not qualify the
    name; `qualified_name` joins whichever parts are present. Policy matching
    compares against these names, so an unqualified reference is matched as
    written rather than silently resolved.
    """

    name: str
    schema: str | None = None
    catalog: str | None = None

    @property
    def qualified_name(self) -> str:
        return ".".join(part for part in (self.catalog, self.schema, self.name) if part)


@dataclass(frozen=True, slots=True)
class ParsedSQL:
    """The statements a dialect found in one submitted string.

    Splitting is the step that makes "one statement" checkable. Anything that
    yields more than one statement is a multi-statement submission, whatever it
    looked like to the caller.
    """

    statements: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SQLClassification:
    """The structure a policy check runs against, never a transpilation.

    `read_only` is the field most decisions turn on, and it is deliberately
    conservative: a statement must be recognizably read-only to be treated as
    such. `tables` is everything referenced, `write_targets` only what is
    written, so a policy can allow reading a table it forbids writing.

    A `SELECT` is not automatically a read. `SELECT ... FOR UPDATE` takes row
    locks and `SELECT nextval(...)` advances a sequence, both of which
    PostgreSQL refuses in a read-only transaction. When `operation` is `SELECT`
    and `read_only` is false, `read_only_reason` says which of those it was, so
    a refusal can explain itself instead of reporting that a SELECT is not
    allowed by a read-only policy.
    """

    operation: SQLOperation
    read_only: bool
    tables: tuple[SQLObjectRef, ...] = ()
    write_targets: tuple[SQLObjectRef, ...] = ()
    functions: tuple[str, ...] = ()
    statement_count: int = 1
    read_only_reason: str | None = None
