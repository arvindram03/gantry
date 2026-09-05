# SPDX-License-Identifier: Apache-2.0
"""Reconciliation as a phase: layered, cheapest first, drilling only where it must.

Two properties are load-bearing and neither is visible from a passing verdict,
which is why they are asserted directly:

**The layers stop as soon as the question is settled.** A count mismatch already
proves the sides differ; checksumming to confirm it reads every row on both
sides to learn nothing.

**Localisation stays logarithmic.** The drill-down halves a key range, and it
needs both bounds to compute a midpoint. With an open lower bound it silently
degrades to enumerating every row — the verdict is still correct, it just takes
minutes instead of seconds, and nothing in the output says so.

Requires: make dev-up && uv run alembic upgrade head.
"""

from __future__ import annotations

import math
import os
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.core.dataset import DatasetManifest
from gantry.migration.reconcile import (
    LayerOutcome,
    LayerResult,
    ReconciliationLayer,
    ReconciliationReport,
    reconcile,
)
from gantry.state.database import create_engine, transaction
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)

TABLE = "public.reconcile_probe"
ROWS = 20_000

DDL = f"""
CREATE TABLE {TABLE} (
    id bigint PRIMARY KEY,
    amount numeric(12,2) NOT NULL,
    label text
)
"""
FILL = f"INSERT INTO {TABLE} SELECT g, g * 1.5, 'row' || g FROM generate_series(1, {ROWS}) g"


@pytest.fixture
async def sides() -> AsyncIterator[tuple[AsyncEngine, AsyncEngine]]:
    source = create_engine(SOURCE_URL)
    target = create_engine(TARGET_URL)

    async def build() -> None:
        for engine in (source, target):
            async with transaction(engine) as connection:
                await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
                await connection.execute(text(DDL))
                await connection.execute(text(FILL))
                await connection.execute(text(f"ANALYZE {TABLE}"))

    try:
        await build()
        yield source, target
    finally:
        for engine in (source, target):
            async with transaction(engine) as connection:
                await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
            await engine.dispose()


async def manifest_of(source: AsyncEngine) -> DatasetManifest:
    found = {m.name: m for m in await PostgresSourceAdapter(source).discover()}
    return found[TABLE]


async def run(sides: tuple[AsyncEngine, AsyncEngine]) -> ReconciliationReport:
    source, target = sides
    return await reconcile(
        await manifest_of(source),
        source_engine=source,
        target_engine=target,
        target=TABLE,
    )


def layer(report: ReconciliationReport, name: ReconciliationLayer) -> LayerResult:
    return next(entry for entry in report.layers if entry.layer is name)


async def test_identical_sides_agree_after_two_layers(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    report = await run(sides)

    assert report.agreed, report.describe()
    assert report.stopped_at is ReconciliationLayer.CHECKSUM
    assert layer(report, ReconciliationLayer.COUNT).outcome is LayerOutcome.AGREED
    assert report.queries == 4, "two counts and two checksums, nothing more"


async def test_the_watermark_is_typed_not_compared_as_text(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """`id::text <= '20000'` is a string comparison, and '3' > '20000'
    lexically. The first version of this bounded the whole table to a handful
    of rows and reported agreement over them — a comparison that silently
    narrows to the wrong subset is worse than one that errors."""
    report = await run(sides)

    counts = layer(report, ReconciliationLayer.COUNT)
    assert f"{ROWS:,} rows" in counts.detail, counts.detail


async def test_a_count_mismatch_skips_the_checksum(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Counts disagreeing already proves the sides differ."""
    _, target = sides
    async with transaction(target) as connection:
        await connection.execute(text(f"DELETE FROM {TABLE} WHERE id = 5000"))

    report = await run(sides)

    assert not report.agreed
    assert layer(report, ReconciliationLayer.COUNT).outcome is LayerOutcome.DISAGREED
    assert layer(report, ReconciliationLayer.CHECKSUM).outcome is LayerOutcome.SKIPPED
    assert layer(report, ReconciliationLayer.CHECKSUM).queries == 0


async def test_equal_counts_with_different_content_are_caught_by_the_checksum(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The case a count cannot see: the row is present on both sides and one
    of them is wrong."""
    _, target = sides
    async with transaction(target) as connection:
        await connection.execute(text(f"UPDATE {TABLE} SET amount = 0 WHERE id = 7777"))

    report = await run(sides)

    assert not report.agreed
    assert layer(report, ReconciliationLayer.COUNT).outcome is LayerOutcome.AGREED
    assert layer(report, ReconciliationLayer.CHECKSUM).outcome is LayerOutcome.DISAGREED


async def test_a_corrupted_row_is_located_without_a_full_scan(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """The exit criterion. Halving 20,000 rows takes about log2(20000) ≈ 15
    comparisons; enumerating them takes one, and twenty thousand row reads."""
    _, target = sides
    async with transaction(target) as connection:
        await connection.execute(text(f"UPDATE {TABLE} SET amount = 1 WHERE id = 13337"))

    report = await run(sides)

    assert report.localization is not None
    assert report.localization.differing_keys == ["13337"]

    comparisons = report.localization.comparisons
    ceiling = 3 * math.ceil(math.log2(ROWS))
    assert 1 < comparisons <= ceiling, (
        f"{comparisons} comparisons for {ROWS:,} rows is not a binary search; "
        f"an open bound makes the drill-down enumerate instead of halve"
    )


async def test_a_missing_row_is_named(sides: tuple[AsyncEngine, AsyncEngine]) -> None:
    _, target = sides
    async with transaction(target) as connection:
        await connection.execute(text(f"DELETE FROM {TABLE} WHERE id = 900"))

    report = await run(sides)
    assert report.localization is not None
    assert "900" in report.localization.missing_keys


async def test_rows_above_the_watermark_are_excluded_not_reported_as_missing(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Reconciling under live writes. Rows the source has and the target has
    not been given yet are lag, not corruption, and reporting them as missing
    would make every streaming migration look broken."""
    source, _ = sides
    async with transaction(source) as connection:
        await connection.execute(
            text(
                f"INSERT INTO {TABLE} SELECT g, g, 'new' "
                f"FROM generate_series({ROWS + 1}, {ROWS + 50}) g"
            )
        )

    report = await run(sides)

    assert report.agreed, f"new source rows are lag, not disagreement: {report.describe()}"
    assert report.watermark == str(ROWS)


async def test_an_empty_target_is_not_a_disagreement(
    sides: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Nothing has been moved yet. Saying 'disagrees' would be true and
    useless; the phase has not run."""
    _, target = sides
    async with transaction(target) as connection:
        await connection.execute(text(f"TRUNCATE {TABLE}"))

    report = await run(sides)
    assert report.agreed
    assert report.watermark is None
    assert layer(report, ReconciliationLayer.COUNT).outcome is LayerOutcome.SKIPPED
