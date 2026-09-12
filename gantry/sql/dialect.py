# SPDX-License-Identifier: Apache-2.0
"""Conservative SQL structure used for policy checks, never transpilation."""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
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
_DOLLAR_QUOTE = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")
_DDL = {"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMENT"}
# A `FOR` followed by one of these opens a row-locking clause. PostgreSQL counts
# the locks as writes: `SELECT ... FOR UPDATE` is refused by a read-only
# transaction, so a classifier that calls it read-only disagrees with the engine.
_LOCKING = {"UPDATE", "SHARE", "NO", "KEY"}
# Functions that write despite appearing in a `SELECT`. This is a floor, not a
# boundary: any user-defined function can write, and no scanner can know that.
# The guarantee comes from the adapter holding a read-only session, which
# `policy_errors` requires before it will admit `read_only=True` at all.
#
# Membership is measured, not assumed. `nextval` and `setval` are here because
# PostgreSQL 16 refuses them in a read-only transaction; `pg_advisory_lock` and
# `pg_logical_emit_message` were in an earlier draft of this list and are not
# here because it permits those.
#
# `dblink_exec` is the one that matters most, and for the opposite reason: the
# read-only transaction *permits* it, because the write happens on another
# server. Measured — the inserted row survived the ROLLBACK. It is the only
# member the session cannot also catch.
_WRITING_FUNCTIONS = frozenset({"nextval", "setval", "dblink_exec"})
_OPERATIONS = {
    "SELECT": SQLOperation.SELECT,
    "INSERT": SQLOperation.INSERT,
    "UPDATE": SQLOperation.UPDATE,
    "DELETE": SQLOperation.DELETE,
    "MERGE": SQLOperation.MERGE,
}


class SQLDialect(Protocol):
    """How one SQL dialect is split and classified for policy checks.

    Three methods, none of which rewrite SQL: `parse` splits a submission into
    statements, `classify` says what the single statement does, and
    `referenced_objects` lists the tables it names. Register an implementation
    with `register_dialect`; `ConservativeDialect` is the deny-by-default
    fallback.
    """

    def parse(self, sql: str) -> ParsedSQL: ...

    def classify(self, sql: str) -> SQLClassification: ...

    def referenced_objects(self, sql: str) -> tuple[SQLObjectRef, ...]: ...


class ConservativeDialect:
    """A small deny-by-default classifier shared until a dialect overrides it.

    The two options are lexical rules that genuinely differ between engines, and
    getting them wrong changes where a statement ends. `backslash_escapes` is
    off by default, matching PostgreSQL with `standard_conforming_strings`,
    where a backslash is only special inside an `E''` string. `dollar_quoting`
    is on by default because PostgreSQL has it and MySQL does not — there, `$$`
    is a syntax error and `$` is an ordinary identifier character.
    """

    def __init__(self, *, backslash_escapes: bool = False, dollar_quoting: bool = True) -> None:
        self._backslash_escapes = backslash_escapes
        self._dollar_quoting = dollar_quoting

    def parse(self, sql: str) -> ParsedSQL:
        statements = tuple(part.strip() for part in self._split(sql) if part.strip())
        return ParsedSQL(statements)

    def _split(self, sql: str) -> tuple[str, ...]:
        return _split_statements(
            sql,
            backslash_escapes=self._backslash_escapes,
            dollar_quoting=self._dollar_quoting,
        )

    def _blank(self, sql: str) -> str:
        return _blank_comments(
            sql,
            backslash_escapes=self._backslash_escapes,
            dollar_quoting=self._dollar_quoting,
        )

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
        # Keyword scanning runs on the statement with comments blanked, so a
        # comment neither hides a keyword nor contributes words of its own.
        scanned = self._blank(statement)
        words = [match.group(0).upper() for match in _WORD.finditer(scanned)]
        first = words[0] if words else ""
        if first == "WITH":
            dangerous = {*_DDL, "INSERT", "UPDATE", "DELETE", "MERGE"}
            first = next((word for word in words[1:] if word in dangerous), "")
            if not first and "SELECT" in words:
                first = "SELECT"
        operation = (
            SQLOperation.DDL if first in _DDL else _OPERATIONS.get(first, SQLOperation.UNKNOWN)
        )
        if operation is SQLOperation.SELECT and "INTO" in words:
            # `SELECT ... INTO new_table` creates a table. PostgreSQL refuses it
            # in a read-only transaction for exactly that reason, and MySQL's
            # `SELECT ... INTO OUTFILE` writes a file. It is a create, not a read.
            operation = SQLOperation.DDL
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
        reason = _why_not_read_only(words, functions) if operation is SQLOperation.SELECT else None
        return SQLClassification(
            operation=operation,
            read_only=operation is SQLOperation.SELECT and reason is None,
            read_only_reason=reason,
            tables=tables,
            write_targets=write_targets,
            functions=functions,
            statement_count=1,
        )

    def referenced_objects(self, sql: str) -> tuple[SQLObjectRef, ...]:
        references: list[SQLObjectRef] = []
        seen: set[str] = set()
        # `/* FROM secret */` names no table. Scanning the comment would add a
        # reference the engine never resolves, and let the text of a comment
        # decide whether an allow-list matches.
        for match in _OBJECT.finditer(self._blank(sql)):
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


class MySQLDialect(ConservativeDialect):
    """`ConservativeDialect` with MySQL's lexical rules rather than PostgreSQL's.

    Two differences, both of which move where a statement ends. MySQL escapes
    backslashes inside ordinary strings — `NO_BACKSLASH_ESCAPES` is not in the
    default `sql_mode` — so `'a\\'; SELECT 2'` is one string and one statement.
    Reading it with PostgreSQL's rule splits it in two and refuses it as a
    batch, which is safe but wrong. And MySQL has no dollar-quoting: `$$` is a
    syntax error there, while `$` is an ordinary identifier character, so
    `my$tab$le` is one name.

    Checked against MySQL 8.4 with the default `sql_mode`.
    """

    def __init__(self) -> None:
        super().__init__(backslash_escapes=True, dollar_quoting=False)


def _spans(
    sql: str, *, backslash_escapes: bool = False, dollar_quoting: bool = True
) -> Iterator[tuple[str, int, int]]:
    """Walk `sql` once, yielding `(kind, start, end)` covering every character.

    `kind` is `"code"`, `"quoted"` for a string or quoted identifier, or
    `"comment"`. One scanner serves both consumers below, because the previous
    arrangement — each consumer tracking quote state itself — is how a `;`
    inside `$$...$$` (#7) and a `\'` inside `E''` (#8) each got read as a
    separator in one place while being handled correctly in another.

    Unterminated constructs run to the end of the input, the way PostgreSQL
    treats them: the statement is malformed, and swallowing the rest refuses it
    rather than splitting it into something that looks executable.
    """
    index = 0
    code_start = 0
    while index < len(sql):
        char = sql[index]
        if char in {"'", '"', "`"}:
            yield "code", code_start, index
            stop = _end_of_quoted(sql, index, backslash_escapes=backslash_escapes)
            yield "quoted", index, stop
            index = code_start = stop
            continue
        if char == "-" and sql.startswith("--", index):
            yield "code", code_start, index
            newline = sql.find("\n", index + 2)
            # The newline ends the comment and belongs to the code after it.
            stop = len(sql) if newline == -1 else newline
            yield "comment", index, stop
            index = code_start = stop
            continue
        if char == "/" and sql.startswith("/*", index):
            yield "code", code_start, index
            end = sql.find("*/", index + 2)
            stop = len(sql) if end == -1 else end + 2
            yield "comment", index, stop
            index = code_start = stop
            continue
        if dollar_quoting and char == "$" and _starts_token(sql, index):
            match = _DOLLAR_QUOTE.match(sql, index)
            if match is not None:
                yield "code", code_start, index
                tag = match.group(0)
                end = sql.find(tag, match.end())
                stop = len(sql) if end == -1 else end + len(tag)
                yield "quoted", index, stop
                index = code_start = stop
                continue
        index += 1
    yield "code", code_start, len(sql)


def _end_of_quoted(sql: str, index: int, *, backslash_escapes: bool = False) -> int:
    """The index just past the string or quoted identifier opening at `index`."""
    quote = sql[index]
    # MySQL escapes backslashes in every string unless NO_BACKSLASH_ESCAPES is
    # set; PostgreSQL only does so after an `E` prefix. Backticks quote an
    # identifier in both, where a backslash is literal.
    escapes = (backslash_escapes and quote in {"'", '"'}) or (
        quote == "'" and _has_escape_prefix(sql, index)
    )
    cursor = index + 1
    while cursor < len(sql):
        char = sql[cursor]
        if escapes and char == "\\" and cursor + 1 < len(sql):
            # Inside an E'' string a backslash escapes whatever follows,
            # including the closing quote, so `E'O\'Brien'` is one string.
            cursor += 2
            continue
        if char == quote:
            if cursor + 1 < len(sql) and sql[cursor + 1] == quote:
                cursor += 2
                continue
            return cursor + 1
        cursor += 1
    return len(sql)


def _split_statements(
    sql: str, *, backslash_escapes: bool = False, dollar_quoting: bool = True
) -> tuple[str, ...]:
    """Split on the semicolons that actually separate statements.

    A `;` only separates when it is code: inside a string, a quoted identifier,
    a dollar-quoted body or a comment it is just a character.
    """
    parts: list[str] = []
    start = 0
    for kind, span_start, span_end in _spans(
        sql, backslash_escapes=backslash_escapes, dollar_quoting=dollar_quoting
    ):
        if kind != "code":
            continue
        offset = sql.find(";", span_start, span_end)
        while offset != -1:
            parts.append(sql[start:offset])
            start = offset + 1
            offset = sql.find(";", start, span_end)
    parts.append(sql[start:])
    return tuple(parts)


def _blank_comments(
    sql: str, *, backslash_escapes: bool = False, dollar_quoting: bool = True
) -> str:
    """Replace each comment with spaces, as the engine's lexer treats them.

    Two reasons this has to happen before tokenizing. A comment is whitespace,
    so `SELECT ... FOR /* x */ UPDATE` really is a locking clause and has to be
    seen as one — found by the property tests, confirmed against PostgreSQL 16.
    And a comment's *contents* are not SQL, so the words inside it must not
    reach the keyword scan at all.

    Spaces rather than nothing, because a comment is a token boundary:
    PostgreSQL reads `DEL/**/ETE` as two tokens and rejects it, so gluing the
    halves into `DELETE` would invent a keyword the engine never saw. The
    replacement is the same width as what it replaces, so an offset into the
    result still points at the same character of the original.
    """
    if "--" not in sql and "/*" not in sql:
        return sql
    return "".join(
        " " * (end - start) if kind == "comment" else sql[start:end]
        for kind, start, end in _spans(
            sql, backslash_escapes=backslash_escapes, dollar_quoting=dollar_quoting
        )
    )


def _has_escape_prefix(sql: str, index: int) -> bool:
    """Whether the quote at `index` opens a PostgreSQL `E''` string.

    Backslashes are only special in the `E''` form. With
    `standard_conforming_strings` on — the default since 9.1 — a backslash in
    an ordinary `'...'` string is a literal backslash, so treating it as an
    escape everywhere would run the opposite risk: a string ending later than
    it should.

    The `E` has to be a token of its own. In `tableE'x'` PostgreSQL reads
    `tableE` as an identifier and `'x'` as a plain string, so an `E` preceded
    by an identifier character is not a prefix.
    """
    if index == 0 or sql[index - 1] not in {"E", "e"}:
        return False
    return _starts_token(sql, index - 1)


def _why_not_read_only(words: Sequence[str], functions: Sequence[str]) -> str | None:
    """Why this `SELECT` is not a read, or `None` when it is one.

    Both answers are things PostgreSQL refuses in a read-only transaction, so
    the classifier and the engine agree about what counts as a write.
    """
    if _locks_rows(words):
        return "the statement takes row locks with a FOR UPDATE or FOR SHARE clause"
    writing = sorted({name for name in functions if name.lower() in _WRITING_FUNCTIONS})
    if writing:
        return f"the statement calls a function that writes: {', '.join(writing)}"
    return None


def _locks_rows(words: Sequence[str]) -> bool:
    """Whether a `SELECT` carries a row-locking clause.

    `FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE` and `FOR KEY SHARE` all take
    locks that PostgreSQL treats as writes. Matching on the word list means a
    `FOR` inside a string literal counts too, which over-refuses rather than
    under-refuses — the same trade the rest of this module makes.
    """
    return any(
        word == "FOR" and words[index + 1] in _LOCKING for index, word in enumerate(words[:-1])
    )


def _starts_token(sql: str, index: int) -> bool:
    """Whether the character at `index` can begin a token.

    PostgreSQL identifiers may contain `$` and letters after the first
    character, so `my$tab$le` is a single legal identifier and `tableE'x'` is an
    identifier followed by a plain string. Constructs that can only *begin* a
    token — dollar-quoting, an `E''` prefix — are not one when the preceding
    character continues an identifier. Without this, `my$tab$le; DROP TABLE
    victim` reads as one read-only SELECT while the engine runs two statements.
    """
    if index == 0:
        return True
    previous = sql[index - 1]
    return not (previous.isalnum() or previous in {"_", "$"})


def _unquote(identifier: str) -> str:
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1].replace('""', '"')
    if identifier.startswith("`") and identifier.endswith("`"):
        return identifier[1:-1].replace("``", "`")
    if identifier.startswith("[") and identifier.endswith("]"):
        return identifier[1:-1].replace("]]", "]")
    return identifier
