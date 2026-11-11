# SPDX-License-Identifier: Apache-2.0
"""Conservative SQL structure used for policy checks, never transpilation."""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from typing import Protocol

from gantry.sql.classification import ParsedSQL, SQLClassification, SQLObjectRef, SQLOperation

_LEADING_COMMENTS = re.compile(r"\A(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*", re.DOTALL)
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
# Any letter may start a name, not only an ASCII one: PostgreSQL and MySQL both
# accept `sécurité` unquoted, and a pattern that stopped at the `é` would report
# a table called `s`.
_IDENTIFIER = r'(?:[^\W\d][\w$-]*|"(?:""|[^"])+"|`(?:``|[^`])+`|\[(?:\]\]|[^\]])+\])'
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
# Functions that reach a table a scanner cannot see: they run a string argument
# as SQL, or read a table named by one. `table_to_xml('secret', ...)` returns
# the whole of `secret` with no `FROM` anywhere, and `query_to_xml('SELECT ...')`
# does the same for any query — the string can be assembled with `||`, so
# looking inside it for a table name proves nothing.
#
# Measured against PostgreSQL 16: under a policy allowing only `analytics.*`,
# both returned the rows of a table in another schema. A statement that calls
# one is therefore not classified at all.
#
# This is a floor, like `_WRITING_FUNCTIONS`. A user-defined function can do the
# same and nothing here can know. What the database role may reach is the
# boundary; this list is the part of it that ships in every PostgreSQL.
_EXECUTING_FUNCTIONS = frozenset(
    {
        *(
            f"{subject}_to_{form}"
            for subject in ("query", "table", "schema", "database", "cursor")
            for form in ("xml", "xmlschema", "xml_and_xmlschema")
        ),
        "dblink",
        "dblink_exec",
        "dblink_open",
        "dblink_send_query",
    }
)
_DML = {
    SQLOperation.INSERT,
    SQLOperation.UPDATE,
    SQLOperation.DELETE,
    SQLOperation.MERGE,
}
# One token of a statement as the write scan reads it: a possibly-qualified
# name, or a piece of punctuation that changes what the names around it mean.
_TOKEN = re.compile(rf"({_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER}){{0,2}})|([(),])")
# Words that end a `FROM` list. Past one of these a comma separates something
# else — columns, sort keys, assignments — and is not another table.
_FROM_END = frozenset(
    {
        "WHERE",
        "GROUP",
        "ORDER",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "MINUS",
        "RETURNING",
        "WINDOW",
        "FOR",
        "SET",
        "WHEN",
        "CONFLICT",
        "DUPLICATE",
        "QUALIFY",
    }
)
# Functions whose argument list uses `FROM` as punctuation: `EXTRACT(YEAR FROM
# ts)`, `SUBSTRING(s FROM 2)`, `TRIM(BOTH ' ' FROM s)`.
_FROM_AS_PUNCTUATION = frozenset({"EXTRACT", "SUBSTRING", "TRIM", "OVERLAY"})
# What can open a parenthesis in a table position and make it a subquery, whose
# own `FROM` is then read on its own.
_SUBQUERY = frozenset({"SELECT", "WITH", "VALUES", "TABLE"})
# Words that sit between a verb and the table it names without being the table.
_VERB_MODIFIERS = frozenset(
    {"ONLY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED", "IGNORE", "QUICK", "OVERWRITE", "TABLE"}
)
# What an `UPDATE` follows when it is a clause of something else rather than a
# statement: `FOR UPDATE`, `FOR NO KEY UPDATE`, `ON CONFLICT ... DO UPDATE`,
# `ON DUPLICATE KEY UPDATE`, and a `MERGE` arm's `THEN UPDATE`.
_NOT_A_STATEMENT_AFTER = frozenset({"FOR", "KEY", "DO", "THEN"})
# Keywords that can follow a verb where a table name would be. Seeing one means
# the "verb" was something else — `insert`, `update` and `delete` are not
# reserved in PostgreSQL, so `SELECT update FROM t` reads a column.
_NOT_A_TABLE = frozenset(
    {
        "FROM",
        "SET",
        "WHERE",
        "VALUES",
        "SELECT",
        "AS",
        "ON",
        "USING",
        "RETURNING",
        "INTO",
        "AND",
        "OR",
        "NOT",
        "IS",
        "IN",
        "DEFAULT",
        "NULL",
        "THEN",
        "WHEN",
        "ELSE",
        "END",
        "CASE",
        "ORDER",
        "GROUP",
        "BY",
        "LIMIT",
        "UNION",
        "OF",
        "NOWAIT",
        "SKIP",
    }
)
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
        functions = tuple(dict.fromkeys(match.group(1) for match in _FUNCTION.finditer(statement)))
        code = self._blank_literals(statement)
        unreadable = _unreadable(code)
        if unreadable is not None:
            # Reported with whatever tables are visible, so a refusal can still
            # name them, but as nothing that could be admitted — and with the
            # reason, so the refusal says which call it was.
            return SQLClassification(
                SQLOperation.UNKNOWN,
                False,
                tables=self.referenced_objects(statement),
                functions=functions,
                statement_count=1,
                read_only_reason=unreadable,
            )
        if operation in _DML:
            return self._classify_write(operation, code, functions)
        tables = self.referenced_objects(statement)
        write_targets = tables[:1] if operation is SQLOperation.DDL else ()
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

    def _classify_write(
        self, operation: SQLOperation, code: str, functions: tuple[str, ...]
    ) -> SQLClassification:
        """Classify an `INSERT`, `UPDATE`, `DELETE` or `MERGE`.

        `code` is the statement with its comments and string literals blanked.
        What an agent most often writes is text, and text is full of the words
        a scanner keys on: `VALUES ('update from Bob: moved into review')`
        names no table called `Bob` or `review`, and reading it as though it
        did refuses the commonest write there is.

        The names a top-level `WITH` defines are left out of what is read.
        They are not tables, and a write whose sources are restricted would
        otherwise be refused for reading its own CTE.
        """
        tokens = _tokens(code)
        shape = _write_shape(tokens)
        defined = _cte_names(tokens)
        read = tuple(
            reference
            for reference in _table_references(tokens, using=True)
            if not (
                reference.schema is None
                and reference.catalog is None
                and reference.name.lower() in defined
            )
        )
        if shape is None:
            # A verb with no table this scanner can attach it to — MySQL's
            # `DELETE t1, t2 FROM ...` is the usual one. Naming no target would
            # authorize a write to nowhere; refusing is the honest answer.
            return SQLClassification(
                SQLOperation.UNKNOWN, False, tables=read, functions=functions, statement_count=1
            )
        targets, filtered, returning = shape
        written = {reference.qualified_name.lower() for reference in targets}
        return SQLClassification(
            operation=operation,
            read_only=False,
            # A target leads the list, as it always has, so a caller reading
            # `tables[0]` still finds what is written first.
            tables=(
                *targets,
                *(ref for ref in read if ref.qualified_name.lower() not in written),
            ),
            write_targets=targets,
            functions=functions,
            statement_count=1,
            filtered=filtered if operation in {SQLOperation.UPDATE, SQLOperation.DELETE} else None,
            returning=returning,
        )

    def _blank_literals(self, sql: str) -> str:
        """Blank comments and string literals, keeping quoted identifiers.

        A double-quoted span is an identifier in standard SQL and a string in
        MySQL's default `sql_mode`, which is the dialect that escapes with
        backslashes — so that flag decides which it is here too.
        """
        strings = {"'", "$"} | ({'"'} if self._backslash_escapes else set())
        return "".join(
            " " * (end - start)
            if kind == "comment" or (kind == "quoted" and sql[start] in strings)
            else sql[start:end]
            for kind, start, end in _spans(
                sql,
                backslash_escapes=self._backslash_escapes,
                dollar_quoting=self._dollar_quoting,
            )
        )

    def referenced_objects(self, sql: str) -> tuple[SQLObjectRef, ...]:
        """Every table the statement names, in the order it names them.

        Two readings, joined. The pattern below is the original one and finds a
        name directly after `FROM`, `JOIN`, `UPDATE`, `INTO` or `TABLE`. It is
        kept because it reads string literals too, and so notices a table named
        inside one.

        It is not enough on its own, and what it missed was not an edge. It
        sees one table per `FROM`: in `FROM analytics.orders, secret.contacts`
        the second is never read, and neither is the first table inside a
        parenthesised join, `FROM (secret.contacts CROSS JOIN analytics.orders)`.
        Both were admitted under a policy that allowed only `analytics.*` and
        both returned the other table's rows — measured against PostgreSQL 16.
        The structural scan follows the list, so every table in it is named.

        Joined rather than replaced, so nothing the pattern used to report is
        lost: this can only name more tables than before, never fewer.
        """
        references: list[SQLObjectRef] = []
        seen: set[str] = set()

        def add(reference: SQLObjectRef) -> None:
            key = reference.qualified_name.lower()
            if key not in seen:
                seen.add(key)
                references.append(reference)

        # `/* FROM secret */` names no table. Scanning the comment would add a
        # reference the engine never resolves, and let the text of a comment
        # decide whether an allow-list matches.
        for match in _OBJECT.finditer(self._blank(sql)):
            add(_reference(match.group(1)))
        for reference in _table_references(_tokens(self._blank_literals(sql)), using=False):
            add(reference)
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


def _reference(qualified: str) -> SQLObjectRef:
    parts = [_unquote(part.strip()) for part in qualified.split(".")]
    if len(parts) == 3:
        return SQLObjectRef(name=parts[2], schema=parts[1], catalog=parts[0])
    if len(parts) == 2:
        return SQLObjectRef(name=parts[1], schema=parts[0])
    return SQLObjectRef(name=parts[0])


def _unreadable(code: str) -> str | None:
    """Why a statement reaches tables no scanner can name, or `None` if it does not.

    `code` has comments and string literals blanked. Two things qualify: a call
    to one of `_EXECUTING_FUNCTIONS`, however its name is quoted or qualified,
    and a `U&"..."` identifier — whose escapes spell a name this module would
    have to decode to compare, and which nothing an agent writes needs.
    """
    if re.search(r'\bU&\s*"', code, re.IGNORECASE):
        return 'the statement uses a U&"..." identifier, whose name cannot be compared'
    tokens = _tokens(code)
    for index, (text, _) in enumerate(tokens[:-1]):
        if (
            tokens[index + 1][0] == "("
            and text not in "(),"
            and _reference(text).name.lower() in _EXECUTING_FUNCTIONS
        ):
            return (
                f"the statement calls {_reference(text).name.lower()}, which can reach "
                "tables the statement does not name"
            )
    return None


def _tokens(code: str) -> list[tuple[str, int]]:
    """Names and structural punctuation, each with its parenthesis depth.

    `code` must already have comments and string literals blanked: a `(` or a
    `,` inside a string is text, and counting it would move every depth after
    it — which is enough to hide the second table of a `FROM` list.

    An opening parenthesis carries the depth inside it and a closing one the
    depth outside, so a name and the parenthesis that follows it compare equal
    when the name is a function being called.
    """
    tokens: list[tuple[str, int]] = []
    depth = 0
    for match in _TOKEN.finditer(code):
        text = match.group(0)
        if text == "(":
            depth += 1
        elif text == ")":
            depth = max(0, depth - 1)
        tokens.append((text, depth))
    return tokens


def _table_references(
    tokens: Sequence[tuple[str, int]], *, using: bool
) -> tuple[SQLObjectRef, ...]:
    """The tables a statement reads from, found by following its `FROM` lists.

    A table can appear after `FROM`, after any kind of `JOIN`, and after each
    comma of a `FROM` list until the list ends. `using` adds the `USING` of a
    `DELETE` or `MERGE`, which names sources the same way; it is off for
    everything else, where `USING (column)` belongs to a join.

    A parenthesis in a table position is either a subquery, whose own `FROM` is
    found when the scan reaches it, or a parenthesised join, whose first table
    is right there.

    Over-reports by design. A set-returning function in a `FROM` list is named
    as though it were a table, because it is a source the policy did not name.
    """

    def word(index: int) -> str:
        return tokens[index][0].upper() if 0 <= index < len(tokens) else ""

    found: list[SQLObjectRef] = []

    def table_at(index: int) -> None:
        while word(index) in {"ONLY", "LATERAL"}:
            index += 1
        if index >= len(tokens):
            return
        text = tokens[index][0]
        if text == "(":
            if word(index + 1) not in _SUBQUERY:
                table_at(index + 1)
            return
        if text in "),":
            return
        if text.upper() in _NOT_A_TABLE and "." not in text and text[0] not in '"`[':
            return
        found.append(_reference(text))

    open_lists: set[int] = set()
    callers: list[str] = []
    for index, (text, depth) in enumerate(tokens):
        if text == "(":
            callers.append(word(index - 1))
            continue
        if text == ")":
            if callers:
                callers.pop()
            open_lists = {level for level in open_lists if level <= depth}
            continue
        if text == ",":
            if depth in open_lists:
                table_at(index + 1)
            continue
        keyword = text.upper()
        if keyword == "FROM":
            if word(index - 1) == "DISTINCT":
                continue
            if callers and depth == len(callers) and callers[-1] in _FROM_AS_PUNCTUATION:
                continue
            table_at(index + 1)
            open_lists.add(depth)
        elif keyword in {"JOIN", "STRAIGHT_JOIN"}:
            table_at(index + 1)
        elif (
            using and keyword == "USING" and index + 1 < len(tokens) and tokens[index + 1][0] != "("
        ):
            table_at(index + 1)
            open_lists.add(depth)
        elif keyword in _FROM_END:
            open_lists.discard(depth)
    return tuple(dict.fromkeys(found))


def _cte_names(tokens: Sequence[tuple[str, int]]) -> frozenset[str]:
    """The names a statement's leading `WITH` defines.

    Only the top-level list. A CTE defined there is visible to the whole
    statement and shadows a table of the same name everywhere in it, so leaving
    it out of what is read hides nothing. One defined inside a subquery is
    visible only there, and the same name outside it is a real table.
    """

    def word(index: int) -> str:
        return tokens[index][0].upper() if index < len(tokens) else ""

    index = _first_word(tokens)
    if word(index) != "WITH":
        return frozenset()
    index += 1
    if word(index) == "RECURSIVE":
        index += 1
    names: set[str] = set()
    while index < len(tokens):
        name = tokens[index][0]
        if name in "()," or "." in name:
            break
        index += 1
        if index < len(tokens) and tokens[index][0] == "(":
            index = _after_group(tokens, index)  # a column list
        if word(index) != "AS":
            break
        index += 1
        while word(index) in {"NOT", "MATERIALIZED"}:
            index += 1
        if index >= len(tokens) or tokens[index][0] != "(":
            break
        names.add(_unquote(name).lower())
        index = _after_group(tokens, index)
        if index < len(tokens) and tokens[index][0] == ",":
            index += 1
            continue
        break
    return frozenset(names)


def _after_group(tokens: Sequence[tuple[str, int]], index: int) -> int:
    """The index just past the parenthesised group opening at `index`."""
    if index >= len(tokens) or tokens[index][0] != "(":
        return index
    inside = tokens[index][1]
    index += 1
    while index < len(tokens) and not (tokens[index][0] == ")" and tokens[index][1] == inside - 1):
        index += 1
    return index + 1


def _write_shape(
    tokens: Sequence[tuple[str, int]],
) -> tuple[tuple[SQLObjectRef, ...], bool, bool] | None:
    """What a write changes, whether it is filtered, and whether it returns rows.

    `tokens` is one statement, from `_tokens`. Returns `None` when a
    data-modifying verb has no table that can be attached to it, which the
    caller treats as unclassifiable.

    Every data-modifying clause counts, at any depth, because PostgreSQL runs
    every one of them: `WITH gone AS (DELETE FROM a RETURNING *) INSERT INTO b
    SELECT * FROM gone` changes `a` and `b`. Taking the first table the
    statement names — which is what this used to do — reported `a` as the only
    thing written by that statement, and for `WITH src AS (SELECT * FROM a)
    UPDATE b ...` reported the table being *read*.

    The `WHERE` that counts is the first verb's own: at its depth, before its
    clause closes. One inside a subquery filters the subquery.
    """

    def word(index: int) -> str:
        return tokens[index][0].upper() if 0 <= index < len(tokens) else ""

    def table_at(index: int) -> tuple[SQLObjectRef | None, int]:
        """The table named at or after `index`, past any modifiers."""
        while word(index) in _VERB_MODIFIERS:
            index += 1
        text = tokens[index][0] if index < len(tokens) else ""
        if not text or text in "(),":
            return None, index
        # A bare keyword is not a table; a quoted or qualified name always is.
        if text.upper() in _NOT_A_TABLE and "." not in text and text[0] not in '"`[':
            return None, index
        return _reference(text), index

    targets: list[SQLObjectRef] = []
    first: int | None = None
    for index, (text, _) in enumerate(tokens):
        verb = text.upper()
        found: list[SQLObjectRef] = []
        if verb in {"INSERT", "MERGE"}:
            start = index + 1
            while word(start) in _VERB_MODIFIERS:
                start += 1
            if verb == "MERGE" and word(start) != "INTO":
                continue
            target, _ = table_at(start + 1 if word(start) == "INTO" else start)
            if target is None:
                continue
            found.append(target)
        elif verb == "UPDATE":
            if word(index - 1) in _NOT_A_STATEMENT_AFTER:
                continue
            target, at = table_at(index + 1)
            if target is None:
                continue
            found.append(target)
            # MySQL updates through a join, and any table in it can be the one
            # a `SET` assigns to. Up to the `SET`, every table is a target.
            level = tokens[index][1]
            cursor = at + 1
            while cursor < len(tokens) and not (
                word(cursor) == "SET" and tokens[cursor][1] == level
            ):
                if tokens[cursor][1] == level and (
                    tokens[cursor][0] == "," or word(cursor) == "JOIN"
                ):
                    joined, _ = table_at(cursor + 1)
                    if joined is not None:
                        found.append(joined)
                cursor += 1
        elif verb == "DELETE":
            if word(index - 1) in {"THEN", "ON"}:
                continue
            if word(index + 1) != "FROM":
                if first is None and index == _first_word(tokens):
                    return None
                continue
            target, _ = table_at(index + 2)
            if target is None:
                continue
            found.append(target)
        else:
            continue
        if first is None:
            first = index
        for target in found:
            if target not in targets:
                targets.append(target)

    if first is None or not targets:
        return None
    level = tokens[first][1]
    filtered = False
    for cursor in range(first + 1, len(tokens)):
        if tokens[cursor][1] < level:
            break
        if tokens[cursor][1] == level and word(cursor) == "WHERE":
            filtered = True
            break
    returning = any(
        word(cursor) == "RETURNING" and tokens[cursor][1] == level
        for cursor in range(first + 1, len(tokens))
    )
    return tuple(targets), filtered, returning


def _first_word(tokens: Sequence[tuple[str, int]]) -> int:
    """The index of the statement's first word, past a leading `WITH` list."""
    return next((index for index, (text, _) in enumerate(tokens) if text not in "(),"), 0)


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
