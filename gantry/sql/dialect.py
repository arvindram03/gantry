# SPDX-License-Identifier: Apache-2.0
"""Conservative SQL structure used for policy checks, never transpilation."""

from __future__ import annotations

import re
from typing import Protocol

from gantry.sql.classification import ParsedSQL, SQLClassification, SQLObjectRef, SQLOperation

_LEADING_COMMENTS = re.compile(r"\A(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*", re.DOTALL)
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_IDENTIFIER = r'(?:[A-Za-z_][A-Za-z0-9_$-]*|"(?:""|[^"])+"|`(?:``|[^`])+`|\[(?:\]\]|[^\]])+\])'
_OBJECT = re.compile(
    rf"\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+"
    rf"({_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER}){{0,2}})",
    re.IGNORECASE,
)
_FUNCTION = re.compile(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*\(")
_DDL = {"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMENT"}
_OPERATIONS = {
    "SELECT": SQLOperation.SELECT,
    "INSERT": SQLOperation.INSERT,
    "UPDATE": SQLOperation.UPDATE,
    "DELETE": SQLOperation.DELETE,
    "MERGE": SQLOperation.MERGE,
}


class SQLDialect(Protocol):
    def parse(self, sql: str) -> ParsedSQL: ...

    def classify(self, sql: str) -> SQLClassification: ...

    def referenced_objects(self, sql: str) -> tuple[SQLObjectRef, ...]: ...


class ConservativeDialect:
    """A small deny-by-default classifier shared until a dialect overrides it."""

    def parse(self, sql: str) -> ParsedSQL:
        statements = tuple(part.strip() for part in _split_statements(sql) if part.strip())
        return ParsedSQL(statements)

    def classify(self, sql: str) -> SQLClassification:
        parsed = self.parse(sql)
        if not parsed.statements:
            return SQLClassification(SQLOperation.UNKNOWN, False, statement_count=0)
        if len(parsed.statements) > 1:
            return SQLClassification(
                SQLOperation.MULTI_STATEMENT,
                False,
                tables=self.referenced_objects(sql),
                statement_count=len(parsed.statements),
            )

        statement = _LEADING_COMMENTS.sub("", parsed.statements[0])
        words = [match.group(0).upper() for match in _WORD.finditer(statement)]
        first = words[0] if words else ""
        if first == "WITH":
            dangerous = {*_DDL, "INSERT", "UPDATE", "DELETE", "MERGE"}
            first = next((word for word in words[1:] if word in dangerous), "")
            if not first and "SELECT" in words:
                first = "SELECT"
        operation = (
            SQLOperation.DDL if first in _DDL else _OPERATIONS.get(first, SQLOperation.UNKNOWN)
        )
        tables = self.referenced_objects(statement)
        write_targets = (
            tables[:1]
            if operation
            in {
                SQLOperation.INSERT,
                SQLOperation.UPDATE,
                SQLOperation.DELETE,
                SQLOperation.MERGE,
                SQLOperation.DDL,
            }
            else ()
        )
        functions = tuple(dict.fromkeys(match.group(1) for match in _FUNCTION.finditer(statement)))
        return SQLClassification(
            operation=operation,
            read_only=operation is SQLOperation.SELECT,
            tables=tables,
            write_targets=write_targets,
            functions=functions,
            statement_count=1,
        )

    def referenced_objects(self, sql: str) -> tuple[SQLObjectRef, ...]:
        references: list[SQLObjectRef] = []
        seen: set[str] = set()
        for match in _OBJECT.finditer(sql):
            qualified = match.group(1)
            if qualified.lower() in seen:
                continue
            seen.add(qualified.lower())
            parts = [_unquote(part.strip()) for part in qualified.split(".")]
            if len(parts) == 3:
                reference = SQLObjectRef(name=parts[2], schema=parts[1], catalog=parts[0])
            elif len(parts) == 2:
                reference = SQLObjectRef(name=parts[1], schema=parts[0])
            else:
                reference = SQLObjectRef(name=parts[0])
            references.append(reference)
        return tuple(references)


def _split_statements(sql: str) -> tuple[str, ...]:
    parts: list[str] = []
    start = 0
    quote: str | None = None
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in {"'", '"', "`"}:
            quote = char
        elif char == "-" and index + 1 < len(sql) and sql[index + 1] == "-":
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline == -1 else newline
        elif char == "/" and index + 1 < len(sql) and sql[index + 1] == "*":
            end = sql.find("*/", index + 2)
            index = len(sql) if end == -1 else end + 1
        elif char == ";":
            parts.append(sql[start:index])
            start = index + 1
        index += 1
    parts.append(sql[start:])
    return tuple(parts)


def _unquote(identifier: str) -> str:
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1].replace('""', '"')
    if identifier.startswith("`") and identifier.endswith("`"):
        return identifier[1:-1].replace("``", "`")
    if identifier.startswith("[") and identifier.endswith("]"):
        return identifier[1:-1].replace("]]", "]")
    return identifier
