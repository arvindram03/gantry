"""The execution engine interface.

Engines own scans, joins, sorting, aggregation and shuffle. Gantry owns
submission, validation, resource limits, failure classification and provenance.
Two implementations is the minimum that keeps the boundary honest - with one,
the abstraction is whatever that engine happens to do.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol

from gantry.analysis.artifact import GeneratedArtifact


@dataclass(frozen=True)
class ExplainResult:
    """What an engine says it will do, before it does it."""

    plan: str
    estimated_rows: int | None = None
    estimated_cost: float | None = None

    def describe(self) -> str:
        parts: list[str] = []
        if self.estimated_rows is not None:
            parts.append(f"~{self.estimated_rows:,} rows")
        if self.estimated_cost is not None:
            parts.append(f"cost {self.estimated_cost:,.0f}")
        return ", ".join(parts) or "no estimate"


@dataclass(frozen=True)
class QueryResult:
    """Rows an engine returned."""

    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...] = field(default_factory=tuple)

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def as_dicts(self) -> list[dict[str, object]]:
        return [dict(zip(self.columns, row, strict=True)) for row in self.rows]


class EngineAdapter(Protocol):
    """Runs generated artifacts on one execution engine."""

    @property
    def engine(self) -> str: ...

    async def explain(self, artifact: GeneratedArtifact) -> ExplainResult:
        """Ask the engine what it would do.

        Both a syntax check and a cost estimate: an artifact that cannot be
        planned cannot be run, and one that plans to read more than policy
        allows should not be.
        """
        ...

    async def sample(self, artifact: GeneratedArtifact, *, limit: int) -> QueryResult:
        """Run a bounded slice of the artifact.

        Planning proves an artifact is well formed; running a little of it
        proves the engine can actually produce rows - which catches the errors
        that only appear at execution time.
        """
        ...

    async def execute(self, artifact: GeneratedArtifact) -> QueryResult:
        """Run the artifact in full."""
        ...

    async def close(self) -> None: ...


def bounded(body: str, limit: int) -> str:
    """Wrap an artifact so it returns at most `limit` rows.

    Wrapping rather than appending LIMIT: the artifact may already end in one,
    or in a clause where LIMIT would change what the query means.
    """
    return f"SELECT * FROM (\n{body.rstrip().rstrip(';')}\n) AS sample LIMIT {int(limit)}"


def explain_text(rows: Sequence[Sequence[object]]) -> str:
    return "\n".join(str(row[0]) for row in rows if row)


def as_int(value: object, default: int = 0) -> int:
    """Coerce an engine's number to an int.

    Engines disagree about Python types for the same aggregate - PostgreSQL
    returns Decimal where DuckDB returns float, and the reverse - so anything
    reading a result has to normalise or it compares unequal on values that
    agree. Doing it here means each caller does not invent its own rule.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float | Decimal):
        return int(value)
    return int(str(value))


def as_float(value: object, default: float = 0.0) -> float:
    """Coerce an engine's number to a float. See `as_int`."""
    if value is None:
        return default
    if isinstance(value, int | float | Decimal):
        return float(value)
    return float(str(value))
