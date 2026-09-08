# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SQLOperation(StrEnum):
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
    name: str
    schema: str | None = None
    catalog: str | None = None

    @property
    def qualified_name(self) -> str:
        return ".".join(part for part in (self.catalog, self.schema, self.name) if part)


@dataclass(frozen=True, slots=True)
class ParsedSQL:
    statements: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SQLClassification:
    operation: SQLOperation
    read_only: bool
    tables: tuple[SQLObjectRef, ...] = ()
    write_targets: tuple[SQLObjectRef, ...] = ()
    functions: tuple[str, ...] = ()
    statement_count: int = 1
