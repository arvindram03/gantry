# SPDX-License-Identifier: Apache-2.0
"""Check the corpus's ground truth against a real PostgreSQL.

`test_sql_corpus.py` carries a `writes` column saying whether each statement
modifies the database. Everything in that file depends on the column being
right, and a hand-written column is exactly the kind of thing that is quietly
wrong: the first draft claimed `pg_advisory_lock` and `SET work_mem` were
writes, and PostgreSQL permits both in a read-only transaction. It also had
`pg_logical_emit_message` in the classifier's writing-function list on the same
bad guess.

So the column is measured rather than asserted. A read-only transaction is
PostgreSQL's own answer to "does this write": it refuses `SELECT ... INTO`,
`SELECT ... FOR UPDATE` and `nextval()`, and permits an ordinary `SELECT`.

Skipped unless a database is reachable, like the other live files.
"""

from __future__ import annotations

import os

import pytest

from _live import require_live_or_skip
from test_sql_corpus import CORPUS, Case

URL = os.environ.get("GANTRY_TEST_POSTGRES_URL", "postgresql://gantry:gantry@localhost:5432/gantry")

SETUP = """
CREATE SCHEMA IF NOT EXISTS gantry_corpus;
SET search_path = gantry_corpus;
CREATE TABLE IF NOT EXISTS orders (id int, amount int, a int, b int, x int, into_total int);
CREATE TABLE IF NOT EXISTS t (id int, a int, b int, x int);
CREATE TABLE IF NOT EXISTS a (id int);
CREATE TABLE IF NOT EXISTS b (id int);
CREATE TABLE IF NOT EXISTS x (id int);
CREATE TABLE IF NOT EXISTS src (id int);
CREATE SEQUENCE IF NOT EXISTS s;
"""

# Cases that cannot be put to PostgreSQL: other dialects, deliberate syntax
# errors, and statements whose whole point is that they do not parse.
EXECUTABLE = tuple(case for case in CORPUS if case.statements == 1 and case.sql.strip())


async def _connect() -> object:
    import importlib

    asyncpg = importlib.import_module("asyncpg")
    try:
        return await asyncpg.connect(URL, timeout=5)
    except Exception:
        require_live_or_skip(f"no PostgreSQL at {URL.rsplit('@', 1)[-1]}")
        raise


async def _writes_according_to_postgres(connection: object, sql: str) -> bool | None:
    """`True` write, `False` read, `None` the server would not run it at all."""
    import importlib

    asyncpg = importlib.import_module("asyncpg")
    transaction = connection.transaction(readonly=True)  # type: ignore[attr-defined]
    await transaction.start()
    try:
        await connection.execute(f"SET LOCAL search_path = gantry_corpus; {sql}")  # type: ignore[attr-defined]
        return False
    except asyncpg.PostgresError as error:
        return True if "read-only transaction" in str(error) else None
    finally:
        await transaction.rollback()


async def test_the_corpus_ground_truth_matches_postgres() -> None:
    pytest.importorskip("asyncpg")
    connection = await _connect()
    await connection.execute(SETUP)  # type: ignore[attr-defined]

    wrong: list[str] = []
    checked = 0
    try:
        for case in EXECUTABLE:
            verdict = await _writes_according_to_postgres(connection, case.sql)
            if verdict is None:
                continue
            checked += 1
            if verdict != case.writes:
                wrong.append(
                    f"{case.label!r}: corpus says writes={case.writes}, "
                    f"PostgreSQL says {verdict} — {case.sql!r}"
                )
    finally:
        await connection.close()  # type: ignore[attr-defined]

    assert not wrong, "corpus ground truth disagrees with the engine:\n" + "\n".join(wrong)
    # Guard against the check quietly measuring nothing, which would pass.
    assert checked >= 30, f"only {checked} corpus cases reached the server"


@pytest.mark.parametrize(
    "case",
    [c for c in EXECUTABLE if c.writes],
    ids=[c.label for c in EXECUTABLE if c.writes],
)
async def test_every_write_is_refused_before_it_reaches_the_engine(case: Case) -> None:
    """Gantry must refuse the statement itself, not lean on the transaction.

    The assertion here is `REJECTED` and not "did not succeed", and the
    difference is the whole test. A write that the classifier misses still fails
    — the adapter runs every query in a read-only transaction, so PostgreSQL
    rejects it — but it fails as `FAILED` with an engine error *after*
    submission. `REJECTED` is only reachable through admission, before anything
    is sent.

    An earlier version of this test asserted `is not ACCEPTED`, which the
    read-only transaction satisfies on its own: with `classify` stubbed to call
    every statement a read-only SELECT, it still passed 25 of 25. It was
    measuring PostgreSQL, not Gantry.
    """
    import gantry

    pytest.importorskip("asyncpg")
    connection = await _connect()
    await connection.execute(SETUP)  # type: ignore[attr-defined]
    executable = await _writes_according_to_postgres(connection, case.sql)
    await connection.close()  # type: ignore[attr-defined]
    if executable is None:
        pytest.skip("PostgreSQL will not run this statement at all")

    # No schema allow-list, deliberately. With one, most of these are refused
    # for naming an unqualified table and the read-only gate is never reached —
    # which is how the first version of this test passed with `classify` stubbed
    # out. The only thing left that can refuse a write here is `read_only`.
    db = gantry.sql.connect("postgres", url=URL)
    result = await db.query(read_only=True)(case.sql)

    assert result.status is gantry.RunStatus.POLICY_REJECTED, (
        f"{case.label}: expected a policy refusal before submission, got "
        f"{result.status.value}" + (f" — {result.failure.message}" if result.failure else "")
    )
    assert result.failure is not None
    # An engine error that arrived here would mean the statement was submitted.
    assert "read-only transaction" not in result.failure.message, (
        f"{case.label}: this was caught by the engine, not by admission"
    )
    assert result.handle is None, f"{case.label}: a handle means it was submitted"
    assert "read-only" in result.failure.message, (
        f"{case.label}: refused for some other reason than being a write — {result.failure.message}"
    )
