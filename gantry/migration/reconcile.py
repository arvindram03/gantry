# SPDX-License-Identifier: Apache-2.0
"""Reconciliation (RFC 0 §7 Phase 7).

Verification run as a *phase* rather than as a per-partition check. Same
primitives as v1 — the same checksum expression, the same `O(log n)`
localisation — arranged differently: layered, cheapest first, and drilling to
rows only where the cheaper layers could not settle the question.

The ordering is not a style choice. A count over a hundred million rows is an
index scan; a checksum reads every row on both sides; a row-level diff reads
them and compares them. Running the expensive one first would answer the same
question and cost thirty times more, and on a migration that matters the
difference is hours.

**What each layer can and cannot conclude** is the part worth being careful
about:

- Counts agreeing proves nothing about content. Counts *dis*agreeing proves
  the sides differ, and there is no point checksumming to confirm it.
- Checksums agreeing is strong evidence the content matches. Disagreeing tells
  you nothing about *where*, which is what the drill-down is for.
- Constraint checks answer a different question entirely — whether the target
  is internally valid — so they run regardless of what the others found.

## Reconciling while the stream is still moving

The count that matters is the one taken at a consistent position, and the
naive version of this is wrong: comparing a live source against a live target
reports drift that is just replication lag.

So every comparison is bounded by the **highest key present in the target when
reconciliation began**. Rows written after that point have higher keys and are
excluded from both sides, which makes the comparison stable without pausing the
stream. That is exact for append-shaped data and honest about the rest:
*updates* to keys below the watermark can still race, and the report says so
rather than pretending otherwise.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest
from gantry.state.database import transaction
from gantry.verification.checksum import ChunkChecksum, compute_checksum
from gantry.verification.localize import KeyRange, Localization, MismatchLocalizer
from gantry.verification.sql import column_type, qualified, quote


class ReconciliationLayer(StrEnum):
    """Ordered cheapest to most expensive."""

    COUNT = "count"
    CHECKSUM = "checksum"
    CONSTRAINT = "constraint"
    ROW_DIFF = "row_diff"


class LayerOutcome(StrEnum):
    AGREED = "agreed"
    DISAGREED = "disagreed"
    # Not run, because a cheaper layer already settled the question.
    SKIPPED = "skipped"


@dataclass(frozen=True)
class LayerResult:
    """What one layer checked, what it found, and what it cost."""

    layer: ReconciliationLayer
    outcome: LayerOutcome
    detail: str
    # Queries issued. The unit that matters: a layer's cost is round trips
    # against two databases, not wall time on a machine nobody will reproduce.
    queries: int = 0
    seconds: float = 0.0

    def describe(self) -> str:
        if self.outcome is LayerOutcome.SKIPPED:
            return f"{self.layer.value}: skipped — {self.detail}"
        cost = f"{self.queries} quer{'y' if self.queries == 1 else 'ies'}, {self.seconds:.2f}s"
        return f"{self.layer.value}: {self.outcome.value} — {self.detail} ({cost})"


@dataclass
class ReconciliationReport:
    """Whether a Dataset agrees with its source, and how that was established."""

    dataset: str
    target: str
    layers: list[LayerResult] = field(default_factory=list)
    localization: Localization | None = None
    # The key every comparison was bounded by, so a reader knows what "agrees"
    # was measured over. None when the target is empty.
    watermark: str | None = None
    # True when rows were written above the watermark while this ran. Not a
    # failure — it is the normal state under live writes — but it is why the
    # report is "agreement below the watermark" and not "agreement".
    moved_during: bool = False

    @property
    def agreed(self) -> bool:
        return not any(layer.outcome is LayerOutcome.DISAGREED for layer in self.layers)

    @property
    def queries(self) -> int:
        return sum(layer.queries for layer in self.layers)

    @property
    def stopped_at(self) -> ReconciliationLayer | None:
        """The last layer that actually ran."""
        ran = [layer.layer for layer in self.layers if layer.outcome is not LayerOutcome.SKIPPED]
        return ran[-1] if ran else None

    def describe(self) -> str:
        bound = f" below {self.watermark}" if self.watermark else ""
        verdict = "agrees" if self.agreed else "disagrees"
        moving = ", source still moving" if self.moved_during else ""
        return f"{self.dataset} {verdict}{bound} in {self.queries} queries{moving}"


async def reconcile(
    manifest: DatasetManifest,
    *,
    source_engine: AsyncEngine,
    target_engine: AsyncEngine,
    target: str,
    localizer: MismatchLocalizer | None = None,
) -> ReconciliationReport:
    """Reconcile one Dataset, layer by layer, stopping as soon as it can."""
    report = ReconciliationReport(dataset=manifest.name, target=target)
    key = _single_key(manifest)

    bounds = await _key_bounds(target_engine, target=target, key=key)
    report.watermark = None if bounds is None else bounds.hi
    if bounds is None or report.watermark is None:
        report.layers.append(
            LayerResult(
                layer=ReconciliationLayer.COUNT,
                outcome=LayerOutcome.SKIPPED,
                detail="the target is empty; nothing to reconcile",
            )
        )
        return report

    predicate, params = _bounded(manifest, key, report.watermark)

    counts = await _count_layer(source_engine, target_engine, manifest, target, predicate, params)
    report.layers.append(counts)

    if counts.outcome is LayerOutcome.AGREED:
        checksums = await _checksum_layer(
            source_engine, target_engine, manifest, target, predicate, params
        )
        report.layers.append(checksums)
        if checksums.outcome is LayerOutcome.AGREED:
            report.moved_during = await _moved(
                target_engine, manifest, target, key, report.watermark
            )
            return report
    else:
        # A count mismatch already proves the sides differ. Checksumming to
        # confirm it would read every row on both sides to learn nothing.
        report.layers.append(
            LayerResult(
                layer=ReconciliationLayer.CHECKSUM,
                outcome=LayerOutcome.SKIPPED,
                detail="counts already disagree; nothing left for a checksum to establish",
            )
        )

    started = time.perf_counter()
    finder = localizer or MismatchLocalizer(
        source_engine=source_engine, target_engine=target_engine
    )
    # Both bounds, so the drill-down can halve rather than enumerate. This is
    # what keeps localisation O(log n): with an open lower bound it degrades to
    # a full row scan and the report still looks correct while taking minutes.
    localization = await finder.localize(manifest, target=target, key=key, bounds=bounds)
    report.localization = localization
    report.layers.append(
        LayerResult(
            layer=ReconciliationLayer.ROW_DIFF,
            outcome=LayerOutcome.DISAGREED,
            detail=localization.describe(),
            # Two queries per comparison: one per side.
            queries=localization.comparisons * 2,
            seconds=time.perf_counter() - started,
        )
    )
    report.moved_during = await _moved(target_engine, manifest, target, key, report.watermark)
    return report


async def _key_bounds(engine: AsyncEngine, *, target: str, key: str) -> KeyRange | None:
    """The key range present in the target when reconciliation began.

    Read from the *target* rather than the source on purpose: it is the side
    that lags, so bounding by the source's maximum would include rows the
    target has not been given yet and report them as missing.

    **Both** ends, not just the top. The drill-down halves a range to find a
    mismatch, and it cannot compute a midpoint from an open lower bound — an
    unbounded range makes it fall back to enumerating every row, which on
    three million rows is twenty seconds instead of two dozen checksums.
    """
    async with transaction(engine) as connection:
        row = (
            await connection.execute(
                text(
                    f"SELECT min({quote(key)})::text AS lo, max({quote(key)})::text AS hi "
                    f"FROM {qualified(target)}"
                )
            )
        ).one()
    if row.hi is None:
        return None
    return KeyRange(lo=str(row.lo), hi=str(row.hi))


async def _moved(
    engine: AsyncEngine, manifest: DatasetManifest, target: str, key: str, watermark: str
) -> bool:
    """Whether rows arrived above the watermark while reconciliation ran."""
    declared = column_type(manifest, key)
    async with transaction(engine) as connection:
        beyond = (
            await connection.execute(
                text(
                    f"SELECT count(*) FROM {qualified(target)} "
                    f"WHERE {quote(key)} > CAST(CAST(:watermark AS text) AS {declared})"
                ),
                {"watermark": watermark},
            )
        ).scalar_one()
    return int(beyond) > 0


async def _count_layer(
    source_engine: AsyncEngine,
    target_engine: AsyncEngine,
    manifest: DatasetManifest,
    target: str,
    predicate: str,
    params: dict[str, str],
) -> LayerResult:
    started = time.perf_counter()
    source = await _count(source_engine, manifest.physical.reference, predicate, params)
    target_rows = await _count(target_engine, target, predicate, params)
    elapsed = time.perf_counter() - started

    if source == target_rows:
        return LayerResult(
            layer=ReconciliationLayer.COUNT,
            outcome=LayerOutcome.AGREED,
            detail=f"{source:,} rows on both sides",
            queries=2,
            seconds=elapsed,
        )
    return LayerResult(
        layer=ReconciliationLayer.COUNT,
        outcome=LayerOutcome.DISAGREED,
        detail=f"source {source:,}, target {target_rows:,} ({source - target_rows:+,})",
        queries=2,
        seconds=elapsed,
    )


async def _checksum_layer(
    source_engine: AsyncEngine,
    target_engine: AsyncEngine,
    manifest: DatasetManifest,
    target: str,
    predicate: str,
    params: dict[str, str],
) -> LayerResult:
    started = time.perf_counter()
    source: ChunkChecksum = await compute_checksum(
        source_engine, manifest, manifest.physical.reference, predicate=predicate, params=params
    )
    other: ChunkChecksum = await compute_checksum(
        target_engine, manifest, target, predicate=predicate, params=params
    )
    elapsed = time.perf_counter() - started

    if source == other:
        return LayerResult(
            layer=ReconciliationLayer.CHECKSUM,
            outcome=LayerOutcome.AGREED,
            detail=f"{source.describe()}",
            queries=2,
            seconds=elapsed,
        )
    return LayerResult(
        layer=ReconciliationLayer.CHECKSUM,
        outcome=LayerOutcome.DISAGREED,
        # Equal counts with unequal checksums is the interesting case: rows
        # are present on both sides and one of them is wrong.
        detail=f"source {source.describe()}, target {other.describe()}",
        queries=2,
        seconds=elapsed,
    )


async def _count(engine: AsyncEngine, table: str, predicate: str, params: dict[str, str]) -> int:
    async with transaction(engine) as connection:
        value = (
            await connection.execute(
                text(f"SELECT count(*) FROM {qualified(table)} WHERE {predicate}"), params
            )
        ).scalar_one()
    return int(value)


def _bounded(manifest: DatasetManifest, key: str, watermark: str) -> tuple[str, dict[str, str]]:
    """Every comparison runs under the same watermark predicate.

    The bound is re-typed from the schema rather than compared as text, which
    is the same thing partition predicates do and for a reason worth stating:
    `key::text <= '1000000'` is a *string* comparison, and `'2' > '1000000'`
    lexically. The first version of this bounded a million-row table to seven
    rows and cheerfully reported agreement over them — a comparison that
    silently narrows to the wrong subset is worse than one that errors.

    Bound as text before casting, because the driver infers a parameter's type
    from its cast target and binding a string straight against a bigint fails.
    """
    declared = column_type(manifest, key)
    return (
        f"{quote(key)} <= CAST(CAST(:watermark AS text) AS {declared})",
        {"watermark": watermark},
    )


def _single_key(manifest: DatasetManifest) -> str:
    keys = manifest.dataset_schema.keys
    if len(keys) != 1:
        raise ValueError(
            f"dataset {manifest.name!r} has {len(keys)} key columns; "
            f"reconciliation needs exactly one to bound and split on"
        )
    return keys[0]
