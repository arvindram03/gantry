"""Locating a mismatch without a full-table diff.

A checksum says a range disagrees. Halving the range and re-checksumming says
which half, and repeating says where. That is O(log n) comparisons rather than
one comparison per row, which is the difference between verification costing
seconds and costing more than the migration.

Drilling stops at a range small enough to enumerate, and only then does the
runtime look at individual keys. The design document's hierarchy - count,
aggregate, chunk checksum, key diff, row diff - is exactly this: never pay for
a finer comparison than the question needs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest
from gantry.state.database import transaction
from gantry.verification.checksum import ChunkChecksum, compute_checksum
from gantry.verification.sql import column_type, qualified, quote

# Below this many rows, enumerating keys costs less than another round trip.
DEFAULT_ENUMERATION_THRESHOLD = 2_000
# A guard against pathological ranges; 64 halvings covers any realistic key space.
MAX_DEPTH = 64


@dataclass(frozen=True)
class KeyRange:
    """A half-open key range, as text so it stays type-agnostic."""

    lo: str | None
    hi: str | None

    def describe(self) -> str:
        return f"[{self.lo or '-inf'}, {self.hi or '+inf'})"


@dataclass
class Localization:
    """Where a mismatch is, and what it cost to find out."""

    ranges: list[KeyRange] = field(default_factory=list)
    missing_keys: list[str] = field(default_factory=list)
    extra_keys: list[str] = field(default_factory=list)
    differing_keys: list[str] = field(default_factory=list)
    comparisons: int = 0
    exhausted: bool = False

    @property
    def located(self) -> bool:
        return bool(self.missing_keys or self.extra_keys or self.differing_keys)

    def describe(self) -> str:
        parts: list[str] = []
        if self.missing_keys:
            parts.append(f"{len(self.missing_keys)} missing")
        if self.extra_keys:
            parts.append(f"{len(self.extra_keys)} extra")
        if self.differing_keys:
            parts.append(f"{len(self.differing_keys)} differing")
        if not parts:
            parts.append(f"{len(self.ranges)} unresolved ranges")
        return f"{', '.join(parts)} in {self.comparisons} comparisons"


class MismatchLocalizer:
    """Narrows a disagreeing key range down to the rows responsible."""

    def __init__(
        self,
        *,
        source_engine: AsyncEngine,
        target_engine: AsyncEngine,
        enumeration_threshold: int = DEFAULT_ENUMERATION_THRESHOLD,
    ) -> None:
        self._source_engine = source_engine
        self._target_engine = target_engine
        self._threshold = enumeration_threshold

    async def localize(
        self,
        manifest: DatasetManifest,
        *,
        target: str,
        key: str,
        bounds: KeyRange,
    ) -> Localization:
        """Find the rows responsible for a mismatch in `bounds`."""
        report = Localization()
        pending = [(bounds, 0)]

        while pending:
            current, depth = pending.pop()
            source, target_side = await self._checksums(manifest, target, key, current)
            report.comparisons += 1

            if source == target_side:
                continue

            rows = max(source.rows, target_side.rows)
            if rows <= self._threshold or depth >= MAX_DEPTH:
                await self._enumerate(manifest, target, key, current, report)
                continue

            halves = self._split(manifest, key, current)
            if halves is None:
                # A range that cannot be split - a non-numeric key, or one
                # whose bounds have converged. Enumerate rather than give up.
                await self._enumerate(manifest, target, key, current, report)
                continue

            pending.extend((half, depth + 1) for half in halves)

        report.ranges = [KeyRange(lo=bounds.lo, hi=bounds.hi)] if not report.located else []
        return report

    async def _checksums(
        self, manifest: DatasetManifest, target: str, key: str, bounds: KeyRange
    ) -> tuple[ChunkChecksum, ChunkChecksum]:
        predicate, params = self._predicate(manifest, key, bounds)
        source = await compute_checksum(
            self._source_engine,
            manifest,
            manifest.physical.reference,
            predicate=predicate,
            params=params,
        )
        target_side = await compute_checksum(
            self._target_engine, manifest, target, predicate=predicate, params=params
        )
        return source, target_side

    def _predicate(
        self, manifest: DatasetManifest, key: str, bounds: KeyRange
    ) -> tuple[str, dict[str, str]]:
        column = quote(key)
        declared = column_type(manifest, key)
        clauses: list[str] = []
        params: dict[str, str] = {}
        if bounds.lo is not None:
            params["lo"] = bounds.lo
            clauses.append(f"{column} >= CAST(CAST(:lo AS text) AS {declared})")
        if bounds.hi is not None:
            params["hi"] = bounds.hi
            clauses.append(f"{column} < CAST(CAST(:hi AS text) AS {declared})")
        return (" AND ".join(clauses) if clauses else "TRUE"), params

    def _split(
        self, manifest: DatasetManifest, key: str, bounds: KeyRange
    ) -> tuple[KeyRange, KeyRange] | None:
        """Halve a range, if its bounds are numeric and still distinguishable."""
        if bounds.lo is None or bounds.hi is None:
            return None
        try:
            low, high = Decimal(bounds.lo), Decimal(bounds.hi)
        except InvalidOperation:
            return None
        if high - low <= 1:
            return None
        middle = str(int(low + (high - low) / 2))
        if middle in (bounds.lo, bounds.hi):
            return None
        return KeyRange(bounds.lo, middle), KeyRange(middle, bounds.hi)

    async def _enumerate(
        self,
        manifest: DatasetManifest,
        target: str,
        key: str,
        bounds: KeyRange,
        report: Localization,
    ) -> None:
        """Compare the keys in a small range directly.

        Only reached once a range is small enough that reading its keys is
        cheaper than another pair of checksums.
        """
        predicate, params = self._predicate(manifest, key, bounds)
        column = quote(key)
        row = row_hash_expression(manifest)

        source_rows = await self._key_hashes(
            self._source_engine, manifest.physical.reference, column, row, predicate, params
        )
        target_rows = await self._key_hashes(
            self._target_engine, target, column, row, predicate, params
        )

        for identifier, digest in source_rows.items():
            if identifier not in target_rows:
                report.missing_keys.append(identifier)
            elif target_rows[identifier] != digest:
                report.differing_keys.append(identifier)
        for identifier in target_rows:
            if identifier not in source_rows:
                report.extra_keys.append(identifier)

    async def _key_hashes(
        self,
        engine: AsyncEngine,
        table: str,
        column: str,
        row: str,
        predicate: str,
        params: dict[str, str],
    ) -> dict[str, str]:
        query = text(
            f"SELECT {column}::text AS key, {row} AS digest "
            f"FROM {qualified(table)} WHERE {predicate}"
        )
        async with transaction(engine) as connection:
            rows = (await connection.execute(query, params)).all()
        return {str(entry.key): str(entry.digest) for entry in rows}


def row_hash_expression(manifest: DatasetManifest) -> str:
    """A per-row digest, so a differing row is distinguishable from a missing one."""
    from gantry.verification.checksum import row_expression

    return f"md5({row_expression(manifest)})"
