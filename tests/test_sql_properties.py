# SPDX-License-Identifier: Apache-2.0
"""Property-based attacks on SQL classification, complementing the fixed corpus.

The corpus in `test_sql_corpus.py` records cases someone thought of. These
generate cases nobody thought of, and assert the properties that have to hold
across all of them:

1. A statement that writes never classifies as read-only, however it is dressed
   up in comments, casing and whitespace.
2. Appending a second statement never *narrows* the classification — a write
   hidden behind a read is still not a read.
3. Arbitrary text never crashes the classifier.

Property 1 is the one worth the machinery. Obfuscation is exactly where a
scanner fails, and it is mechanical to generate: the same DELETE with a comment
somewhere else in it is a different input and the same semantics.
"""

from __future__ import annotations

from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import ConservativeDialect
from hypothesis import given, settings
from hypothesis import strategies as st

DIALECT = ConservativeDialect()

WRITES: tuple[str, ...] = (
    "DELETE FROM orders",
    "UPDATE orders SET amount = 0",
    "INSERT INTO orders VALUES (1)",
    "DROP TABLE orders",
    "TRUNCATE orders",
    "ALTER TABLE orders ADD COLUMN x int",
    "CREATE TABLE x AS SELECT * FROM orders",
    "MERGE INTO orders USING src ON src.id = orders.id WHEN MATCHED THEN DELETE",
    "GRANT SELECT ON orders TO someone",
    "WITH t AS (DELETE FROM orders RETURNING *) SELECT * FROM t",
    "SELECT * INTO copy_of_orders FROM orders",
    "SELECT * FROM orders FOR UPDATE",
    "SELECT nextval('orders_seq')",
)

READS: tuple[str, ...] = (
    "SELECT * FROM orders",
    "SELECT count(*) FROM orders",
    "WITH t AS (SELECT 1) SELECT * FROM t",
    "SELECT a FROM orders WHERE b = 'x'",
)

# Whitespace a scanner might treat differently from the engine: ordinary space,
# tab, newline, vertical tab, form feed, non-breaking space.
SPACES = st.sampled_from((" ", "\t", "\n", "\x0b", "\x0c", " ", "  "))
COMMENTS = st.sampled_from(("/**/", "/* x */", "--x\n", "/*;*/", "/*'*/", "/*$$*/", "--DROP\n"))


@st.composite
def obfuscated(draw: st.DrawFn, statements: tuple[str, ...]) -> str:
    """One statement, with its whitespace resprayed and comments interleaved."""
    statement = draw(st.sampled_from(statements))
    words = statement.split(" ")
    out: list[str] = []
    for index, word in enumerate(words):
        if index:
            out.append(draw(SPACES))
            if draw(st.booleans()):
                out.append(draw(COMMENTS))
                out.append(draw(SPACES))
        out.append(draw(st.sampled_from((word, word.upper(), word.lower(), word.swapcase()))))
    prefix = draw(st.sampled_from(("", " ", "/*lead*/", "--lead\n", "\n\t")))
    return prefix + "".join(out)


@settings(max_examples=400, deadline=None)
@given(sql=obfuscated(WRITES))
def test_obfuscation_never_turns_a_write_into_a_read(sql: str) -> None:
    classification = DIALECT.classify(sql)

    assert not classification.read_only, f"obfuscated write classified read-only: {sql!r}"


@settings(max_examples=400, deadline=None)
@given(read=obfuscated(READS), write=obfuscated(WRITES), first=st.booleans())
def test_a_write_beside_a_read_is_never_a_read(read: str, write: str, first: bool) -> None:
    """Two statements must not collapse into one read.

    Either the splitter sees both — `MULTI_STATEMENT`, refused — or it fails to
    split and must then classify on something that still is not a read.
    """
    sql = f"{write}; {read}" if first else f"{read}; {write}"

    classification = DIALECT.classify(sql)

    assert not classification.read_only, f"write hidden beside a read: {sql!r}"


@settings(max_examples=300, deadline=None)
@given(read=obfuscated(READS))
def test_obfuscated_reads_are_still_usable(read: str) -> None:
    """The cheap way to pass the properties above is to refuse everything."""
    classification = DIALECT.classify(read)

    assert classification.operation is SQLOperation.SELECT, read
    assert classification.read_only, read


@settings(max_examples=1000, deadline=None)
@given(sql=st.text())
def test_arbitrary_text_never_raises(sql: str) -> None:
    """`classify` runs on whatever a caller submits, including bytes-as-text."""
    classification = DIALECT.classify(sql)

    assert classification.statement_count >= 0
    if classification.read_only:
        assert classification.operation is SQLOperation.SELECT


@settings(max_examples=1000, deadline=None)
@given(sql=st.text(alphabet="SELECT DROP TABLE;'\"$-/*\\ \n()", min_size=1, max_size=60))
def test_sql_shaped_noise_stays_consistent(sql: str) -> None:
    """Text drawn from SQL's own punctuation, where the scanner states live.

    Unbalanced quotes, half-open comments and stray dollar signs are what drive
    the scanner into its edge states, so this is the alphabet worth fuzzing.
    """
    classification = DIALECT.classify(sql)

    # Read-only is reserved for SELECT; nothing else may claim it.
    assert classification.read_only is (
        classification.operation is SQLOperation.SELECT and classification.read_only
    )
    if classification.read_only:
        assert classification.operation is SQLOperation.SELECT
    # The splitter has to agree with itself about how many statements there are.
    assert classification.statement_count == len(DIALECT.parse(sql).statements)


@settings(max_examples=500, deadline=None)
@given(sql=st.text(alphabet="SELECT DROP TABLE;'\"$-/*\\ \n()", min_size=1, max_size=60))
def test_blanking_comments_never_changes_the_length(sql: str) -> None:
    """`_blank_comments` replaces each comment character-for-character.

    If it ever removed characters instead, `DEL/**/ETE` would become `DELETE`
    and the scanner would see a keyword PostgreSQL does not.
    """
    from gantry.sql.dialect import _blank_comments

    assert len(_blank_comments(sql)) == len(sql)
