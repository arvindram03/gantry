"""Applying change events, against a real target.

Requires: make dev-up && uv run alembic upgrade head

The exit criterion for the day: a shuffled, duplicated stream leaves the target
in exactly the state an ordered one would. If that holds, delivery order stops
being something the runtime has to guarantee.
"""

from __future__ import annotations

import os
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from gantry.core.changes import ChangeEvent, ChangeOperation, StreamPosition
from gantry.core.dataset import DatasetManifest, PhysicalRef
from gantry.core.schema import DatasetSchema, FieldSchema
from gantry.movement.cdc_apply import ApplyError, CDCApplier
from gantry.state.database import create_engine, transaction
from gantry.state.deadletters import PostgresDeadLetterStore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
DATASET = "public.cdc_apply"
TARGET = "public.cdc_apply"
OPERATION = "cdc-apply-test"
AT = datetime(2026, 9, 24, tzinfo=UTC)


def manifest() -> DatasetManifest:
    return DatasetManifest(
        name=DATASET,
        physical=PhysicalRef(adapter="postgres", reference=DATASET),
        dataset_schema=DatasetSchema(
            keys=("id",),
            fields=(
                FieldSchema(name="id", type="bigint", nullable=False),
                FieldSchema(name="label", type="text"),
                FieldSchema(name="source_lsn", type="bigint", nullable=False),
            ),
        ),
    )


@pytest.fixture
async def target() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(TARGET_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TARGET}"))
            await connection.execute(text("DROP TABLE IF EXISTS public.gantry_tombstones"))
            await connection.execute(
                text(
                    f"CREATE TABLE {TARGET} ("
                    f"  id bigint PRIMARY KEY, label text, source_lsn bigint NOT NULL DEFAULT 0)"
                )
            )
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def meta() -> AsyncIterator[AsyncEngine]:
    engine = create_engine(META_URL)
    try:
        async with transaction(engine) as connection:
            await connection.execute(
                text("DELETE FROM dead_letters WHERE operation = :o"), {"o": OPERATION}
            )
            await connection.execute(
                text("DELETE FROM operations WHERE name = :o"), {"o": OPERATION}
            )
            await connection.execute(
                text(
                    "INSERT INTO operations (name, operation_type, state, created_at, updated_at)"
                    " VALUES (:o, 'movement', 'executing', now(), now())"
                ),
                {"o": OPERATION},
            )
        yield engine
    finally:
        await engine.dispose()


def change(
    key: int,
    lsn: int,
    *,
    operation: ChangeOperation = ChangeOperation.UPDATE,
    label: str | None = None,
) -> ChangeEvent:
    after = None
    before = None
    if operation is ChangeOperation.DELETE:
        before = {"id": key, "label": label or "gone", "source_lsn": lsn}
    else:
        after = {"id": key, "label": label if label is not None else f"v{lsn}", "source_lsn": lsn}
    return ChangeEvent(
        dataset=DATASET,
        operation=operation,
        key={"id": str(key)},
        before=before,
        after=after,
        source_lsn=lsn,
        source_timestamp=AT + timedelta(seconds=lsn),
        stream_position=StreamPosition(topic="t", partition=0, offset=lsn),
    )


def applier(target: AsyncEngine) -> CDCApplier:
    return CDCApplier(target, operation=OPERATION, manifest=manifest(), target=TARGET)


async def prepared(target: AsyncEngine) -> CDCApplier:
    """An applier with its tombstone table in place, as Prepare would leave it."""
    instance = applier(target)
    await instance.ensure_tombstones()
    return instance


async def rows(engine: AsyncEngine) -> dict[int, tuple[str | None, int]]:
    async with transaction(engine) as connection:
        result = (
            await connection.execute(text(f"SELECT id, label, source_lsn FROM {TARGET}"))
        ).all()
    return {int(row.id): (row.label, int(row.source_lsn)) for row in result}


# --- the exit criterion ----------------------------------------------------


async def test_a_shuffled_duplicated_stream_lands_identically(
    target: AsyncEngine, meta: AsyncEngine
) -> None:
    """Delivery order and delivery count stop mattering.

    The same events, shuffled and duplicated, must leave the target exactly
    where the ordered stream would.
    """
    stream = [
        change(1, 10, operation=ChangeOperation.INSERT, label="a"),
        change(1, 20, label="b"),
        change(1, 30, label="c"),
        change(2, 15, operation=ChangeOperation.INSERT, label="x"),
        change(2, 25, label="y"),
        change(3, 12, operation=ChangeOperation.INSERT, label="p"),
        change(3, 40, operation=ChangeOperation.DELETE),
        change(4, 18, operation=ChangeOperation.INSERT, label="q"),
    ]

    ordered = await (await prepared(target)).apply(stream)
    expected = await rows(target)
    assert expected, "the ordered stream should have written something"

    # Reset and replay the same events shuffled, each delivered twice.
    async with transaction(target) as connection:
        await connection.execute(text(f"TRUNCATE {TARGET}"))
    async with transaction(target) as connection:
        await connection.execute(
            text("DELETE FROM public.gantry_tombstones WHERE operation = :o"), {"o": OPERATION}
        )

    scrambled = [*stream, *stream]
    random.Random(1234).shuffle(scrambled)
    shuffled = await (await prepared(target)).apply(scrambled)

    assert await rows(target) == expected, "delivery order changed the outcome"
    assert shuffled.rejected_stale > ordered.rejected_stale, (
        "duplicates and out-of-order events should have been declined, not applied"
    )


async def test_the_final_state_is_the_newest_version(
    target: AsyncEngine, meta: AsyncEngine
) -> None:
    await (await prepared(target)).apply(
        [
            change(1, 10, operation=ChangeOperation.INSERT, label="first"),
            change(1, 30, label="third"),
            change(1, 20, label="second"),
        ]
    )
    assert await rows(target) == {1: ("third", 30)}


# --- stale-write rejection -------------------------------------------------


async def test_a_stale_update_is_declined(target: AsyncEngine, meta: AsyncEngine) -> None:
    await (await prepared(target)).apply(
        [change(1, 50, operation=ChangeOperation.INSERT, label="new")]
    )
    report = await (await prepared(target)).apply([change(1, 10, label="old")])

    assert report.applied == 0
    assert report.rejected_stale == 1
    assert await rows(target) == {1: ("new", 50)}


async def test_a_duplicate_delivery_is_declined(target: AsyncEngine, meta: AsyncEngine) -> None:
    """An equal LSN is a repeat, not a newer version."""
    event = change(1, 50, operation=ChangeOperation.INSERT, label="once")
    await (await prepared(target)).apply([event])
    report = await (await prepared(target)).apply([event])

    assert report.applied == 0
    assert report.rejected_stale == 1


async def test_rejections_are_counted_not_silent(target: AsyncEngine, meta: AsyncEngine) -> None:
    await (await prepared(target)).apply([change(1, 100, operation=ChangeOperation.INSERT)])
    report = await (await prepared(target)).apply([change(1, 1), change(1, 2), change(1, 3)])

    assert report.rejected_stale == 3
    assert "3 stale rejected" in report.describe()


# --- deletes and resurrection ---------------------------------------------


async def test_a_delete_removes_the_row(target: AsyncEngine, meta: AsyncEngine) -> None:
    await (await prepared(target)).apply([change(1, 10, operation=ChangeOperation.INSERT)])
    report = await (await prepared(target)).apply([change(1, 20, operation=ChangeOperation.DELETE)])

    assert report.deleted == 1
    assert await rows(target) == {}


async def test_a_deleted_row_is_not_resurrected(target: AsyncEngine, meta: AsyncEngine) -> None:
    """A delete removes the LSN the guard compares against.

    Without a tombstone, an insert arriving after the delete but originating
    before it would bring the row back.
    """
    await (await prepared(target)).apply(
        [
            change(1, 10, operation=ChangeOperation.INSERT, label="alive"),
            change(1, 20, operation=ChangeOperation.DELETE),
        ]
    )
    assert await rows(target) == {}

    late = await (await prepared(target)).apply([change(1, 15, label="zombie")])
    assert late.applied == 0
    assert late.rejected_stale == 1
    assert await rows(target) == {}, "a deleted row came back"


async def test_a_newer_insert_after_a_delete_is_applied(
    target: AsyncEngine, meta: AsyncEngine
) -> None:
    """The tombstone must not block a genuinely newer change.

    A key can be deleted and re-created; refusing everything afterwards would
    trade one wrong answer for another.
    """
    await (await prepared(target)).apply(
        [
            change(1, 10, operation=ChangeOperation.INSERT),
            change(1, 20, operation=ChangeOperation.DELETE),
        ]
    )
    recreated = await (await prepared(target)).apply(
        [change(1, 30, operation=ChangeOperation.INSERT, label="reborn")]
    )

    assert recreated.applied == 1
    assert await rows(target) == {1: ("reborn", 30)}


async def test_a_delete_arriving_before_its_insert_wins(
    target: AsyncEngine, meta: AsyncEngine
) -> None:
    """Out of order in the other direction: the delete is newer and must hold."""
    await (await prepared(target)).apply([change(1, 20, operation=ChangeOperation.DELETE)])
    late_insert = await (await prepared(target)).apply(
        [change(1, 10, operation=ChangeOperation.INSERT, label="too late")]
    )

    assert late_insert.applied == 0
    assert await rows(target) == {}


# --- dead letters ----------------------------------------------------------


async def test_an_unapplyable_event_is_kept_not_dropped(
    target: AsyncEngine, meta: AsyncEngine
) -> None:
    """A queue that discards the payload is a counter."""
    broken = change(1, 10, operation=ChangeOperation.INSERT)
    broken = broken.model_copy(update={"after": {"id": 1}})  # missing `label`

    report = await (await prepared(target)).apply([broken])
    assert report.dead_lettered == 1

    store = PostgresDeadLetterStore(meta)
    assert await store.record(OPERATION, report.dead_letters) == 1
    assert await store.depth(OPERATION) == 1

    pending = await store.pending(OPERATION)
    assert "missing columns" in pending[0].reason
    assert pending[0].to_event().source_lsn == 10, "the event must be replayable"


async def test_replayed_letters_leave_the_queue(target: AsyncEngine, meta: AsyncEngine) -> None:
    broken = change(1, 10, operation=ChangeOperation.INSERT).model_copy(update={"after": {"id": 1}})
    report = await (await prepared(target)).apply([broken])
    store = PostgresDeadLetterStore(meta)
    await store.record(OPERATION, report.dead_letters)

    pending = await store.pending(OPERATION)
    assert await store.mark_replayed([letter.id for letter in pending]) == 1
    assert await store.depth(OPERATION) == 0


async def test_a_truncate_needs_an_operator(target: AsyncEngine, meta: AsyncEngine) -> None:
    """Applying a truncate automatically would let one event empty a target."""
    truncate = ChangeEvent(
        dataset=DATASET,
        operation=ChangeOperation.TRUNCATE,
        key={},
        source_lsn=10,
        source_timestamp=AT,
        stream_position=StreamPosition(topic="t", partition=0, offset=0),
    )
    report = await (await prepared(target)).apply([truncate])
    assert report.dead_lettered == 1
    assert "needs an operator" in report.dead_letters[0][1]


# --- preconditions ---------------------------------------------------------


async def test_a_target_without_an_lsn_column_is_refused(target: AsyncEngine) -> None:
    """Stale-write rejection needs something to compare against."""
    without_lsn = manifest().model_copy(
        update={
            "dataset_schema": DatasetSchema(
                keys=("id",), fields=(FieldSchema(name="id", type="bigint"),)
            )
        }
    )
    with pytest.raises(ApplyError, match="nothing to compare against"):
        CDCApplier(target, operation=OPERATION, manifest=without_lsn, target=TARGET)


async def test_an_empty_batch_is_a_noop(target: AsyncEngine, meta: AsyncEngine) -> None:
    report = await (await prepared(target)).apply([])
    assert report.seen == 0
    assert report.committed_at is not None
