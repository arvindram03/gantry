"""The Dataset half of the agent-facing API.

One rule holds this file together: every method that can return content calls
`AccessGate.authorize` first and masks its output with the decision it got
back. There is no path that reaches data without one, and the methods are not
free to reinterpret a decision - they receive the row limit and the masked
field list and apply them.

That is what "an LLM cannot override these constraints" has to mean in code.
Not a system prompt asking a model to behave, and not a tool description it
could argue with: a function it must call to get anything at all.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.api.aggregates import GROUP_SIZE_COLUMN, AggregateQuery
from gantry.core.dataset import DatasetManifest, DatasetRef
from gantry.movement.partitioning import PartitionPlan, plan_partitions
from gantry.policy.audit import AccessLog, NullAccessLog
from gantry.policy.gate import AccessDecision, AccessDeniedError, AccessGate, AccessRequest
from gantry.policy.ladder import AccessRung
from gantry.policy.redaction import redact_columns, redact_manifest, redact_rows
from gantry.policy.rules import AccessRules
from gantry.registry.base import DatasetRegistry
from gantry.state.database import transaction


@dataclass(frozen=True)
class QueryOutcome:
    """Rows, and the decision that shaped them.

    The decision travels with the data so a caller can render what was masked
    and why. A result that has been redacted and does not say so is worse than
    one that was refused.
    """

    rows: list[dict[str, object]]
    decision: AccessDecision

    @property
    def row_count(self) -> int:
        return len(self.rows)


class Datasets:
    """`gantry.datasets.{describe,profile,query,sample}`, gated."""

    def __init__(
        self,
        *,
        registry: DatasetRegistry,
        engine: AsyncEngine,
        rules: AccessRules | None = None,
        gate: AccessGate | None = None,
        audit: AccessLog | None = None,
    ) -> None:
        self._registry = registry
        self._engine = engine
        self._gate = gate or AccessGate(rules)
        self._audit = audit or NullAccessLog()

    @property
    def gate(self) -> AccessGate:
        return self._gate

    async def describe(self, name: str) -> DatasetManifest:
        """What the Dataset is: schema, keys, where it lives.

        The top rung, and the one that should almost always be permitted - an
        agent that cannot read a schema cannot form a narrower request, so
        denying it pushes callers toward asking for everything.
        """
        manifest = await self._manifest(name)
        decision = await self._decide(AccessRequest(rung=AccessRung.DESCRIBE, dataset=manifest))
        return redact_manifest(manifest, decision)

    async def profile(self, name: str, *, columns: Sequence[str] = ()) -> DatasetManifest:
        """What is in it, in aggregate: counts, null rates, distribution."""
        manifest = await self._manifest(name)
        decision = await self._decide(
            AccessRequest(rung=AccessRung.PROFILE, dataset=manifest, fields=tuple(columns))
        )
        return redact_manifest(manifest, decision)

    async def query(self, name: str, query: AggregateQuery) -> QueryOutcome:
        """A grouped aggregate, built from the fixed vocabulary.

        Structured rather than raw SQL so that "is this an aggregate" is
        decidable — see `gantry.api.aggregates`.
        """
        manifest = await self._manifest(name)
        query.check_against(manifest)

        decision = await self._decide(
            AccessRequest(
                rung=AccessRung.QUERY,
                dataset=manifest,
                fields=query.fields_returned(),
                estimated_bytes=manifest.physical.estimated_bytes,
            )
        )

        rows = await self._run(self._compile(manifest, query))
        await self._check_group_sizes(manifest, query, rows)
        return QueryOutcome(
            rows=redact_columns(rows, query.columns_exposing(decision.redacted_fields)),
            decision=decision,
        )

    async def partitions(
        self, name: str, *, count: int = 8
    ) -> tuple[PartitionPlan, AccessDecision]:
        """How the Dataset splits, including the key values at the boundaries."""
        manifest = await self._manifest(name)
        keys = manifest.dataset_schema.keys
        decision = await self._decide(
            AccessRequest(rung=AccessRung.PARTITION, dataset=manifest, fields=tuple(keys))
        )
        plan = plan_partitions(manifest, target_partitions=count)
        return _mask_partitions(plan, decision), decision

    async def sample(
        self,
        name: str,
        *,
        where: dict[str, str] | None = None,
        limit: int = 25,
        reason: str | None = None,
    ) -> QueryOutcome:
        """A bounded number of raw rows. Denied by default."""
        manifest = await self._manifest(name)
        filters = dict(where or {})
        _check_fields(manifest, tuple(filters))

        decision = await self._decide(
            AccessRequest(
                rung=AccessRung.SAMPLE,
                dataset=manifest,
                reason=reason,
                rows_requested=limit,
            )
        )
        rows = await self._run(_sample_sql(self._quote, manifest, filters, decision.row_limit or 0))
        return QueryOutcome(rows=redact_rows(rows, decision), decision=decision)

    async def records(
        self, name: str, *, keys: Sequence[str], reason: str | None = None
    ) -> QueryOutcome:
        """Exact records by key. The bottom rung, and denied by default."""
        manifest = await self._manifest(name)
        decision = await self._decide(
            AccessRequest(rung=AccessRung.RECORDS, dataset=manifest, reason=reason)
        )
        rows = await self._run(_records_sql(self._quote, manifest, tuple(keys)))
        return QueryOutcome(rows=redact_rows(rows, decision), decision=decision)

    async def _decide(self, request: AccessRequest) -> AccessDecision:
        """Authorize, and record the decision either way.

        Denials are recorded as carefully as grants: an audit trail that only
        holds what was permitted cannot show what was attempted.
        """
        try:
            decision = self._gate.authorize(request)
        except AccessDeniedError as denied:
            await self._audit.record(denied.decision)
            raise
        await self._audit.record(decision)
        return decision

    async def _manifest(self, name: str) -> DatasetManifest:
        return (await self._registry.get(DatasetRef(name=name))).manifest

    @property
    def _quote(self) -> Callable[[str], str]:
        preparer = self._engine.dialect.identifier_preparer
        return lambda identifier: str(preparer.quote(identifier))

    def _compile(
        self, manifest: DatasetManifest, query: AggregateQuery
    ) -> tuple[str, dict[str, str]]:
        quote = self._quote
        table = ".".join(quote(part) for part in manifest.physical.reference.split("."))

        projections = [f"{quote(field)} AS {quote(field)}" for field in query.group_by]
        projections.append(f"count(*) AS {quote(GROUP_SIZE_COLUMN)}")
        projections += [
            f"{aggregate.expression(quote)} AS {quote(aggregate.name)}"
            for aggregate in query.aggregates
        ]

        clauses: list[str] = []
        params: dict[str, str] = {}
        for index, (field, value) in enumerate(sorted(query.where.items())):
            params[f"w{index}"] = value
            clauses.append(f"{quote(field)}::text = :w{index}")

        sql = f"SELECT {', '.join(projections)} FROM {table}"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        if query.group_by:
            sql += f" GROUP BY {', '.join(quote(field) for field in query.group_by)}"
            sql += f" ORDER BY {', '.join(quote(field) for field in query.group_by)}"
        if query.limit is not None:
            sql += f" LIMIT {int(query.limit)}"
        return sql, params

    async def _run(self, compiled: tuple[str, dict[str, str]]) -> list[dict[str, object]]:
        sql, params = compiled
        async with transaction(self._engine) as connection:
            result = await connection.execute(text(sql), params)
            return [dict(row) for row in result.mappings()]

    async def _check_group_sizes(
        self, manifest: DatasetManifest, query: AggregateQuery, rows: list[dict[str, object]]
    ) -> None:
        """Refuse a result containing a group smaller than policy permits.

        The whole result is refused rather than the small groups dropped. A
        silently filtered aggregate answers a different question than the one
        asked, and does not say that it did.
        """
        minimum = self._gate.rules.queries.min_group_size
        if minimum <= 1 or not rows:
            return
        sizes = [_as_count(row.get(GROUP_SIZE_COLUMN)) for row in rows]
        smallest = min(sizes)
        if smallest >= minimum:
            return

        request = AccessRequest(
            rung=AccessRung.QUERY,
            dataset=manifest,
            fields=query.fields_returned(),
            group_size=smallest,
        )
        await self._decide(request)


def _mask_partitions(plan: PartitionPlan, decision: AccessDecision) -> PartitionPlan:
    """Mask key values at partition boundaries.

    A boundary is a real value out of the key column. Enough of them read in
    order reconstruct the column's distribution, which is why they are masked
    when the key is sensitive.
    """
    if not decision.redacted_fields:
        return plan
    masked = set(decision.redacted_fields)
    return plan.model_copy(
        update={
            "partitions": tuple(
                partition.model_copy(update={"lo": None, "hi": None})
                if partition.column in masked
                else partition
                for partition in plan.partitions
            )
        }
    )


def _check_fields(manifest: DatasetManifest, fields: Sequence[str]) -> None:
    known = {field.name for field in manifest.dataset_schema.fields}
    if not known:
        return
    unknown = sorted(set(fields) - known)
    if unknown:
        raise ValueError(f"{manifest.name} has no field {', '.join(repr(u) for u in unknown)}")


def _sample_sql(
    quote: Callable[[str], str],
    manifest: DatasetManifest,
    where: dict[str, str],
    limit: int,
) -> tuple[str, dict[str, str]]:
    table = ".".join(quote(part) for part in manifest.physical.reference.split("."))
    clauses: list[str] = []
    params: dict[str, str] = {}
    for index, (field, value) in enumerate(sorted(where.items())):
        params[f"w{index}"] = value
        clauses.append(f"{quote(field)}::text = :w{index}")
    sql = f"SELECT * FROM {table}"
    if clauses:
        sql += f" WHERE {' AND '.join(clauses)}"
    # Ordered so a sample taken twice is the same sample: an unordered LIMIT
    # is whatever the engine happened to scan first.
    keys = manifest.dataset_schema.keys
    if keys:
        sql += f" ORDER BY {', '.join(quote(key) for key in keys)}"
    return f"{sql} LIMIT {int(limit)}", params


def _records_sql(
    quote: Callable[[str], str], manifest: DatasetManifest, keys: Sequence[str]
) -> tuple[str, dict[str, str]]:
    table = ".".join(quote(part) for part in manifest.physical.reference.split("."))
    key_fields = manifest.dataset_schema.keys
    if len(key_fields) != 1:
        raise ValueError(f"{manifest.name} needs exactly one key field to address records by key")
    params = {f"k{index}": value for index, value in enumerate(keys)}
    placeholders = ", ".join(f":{name}" for name in params) or "NULL"
    return (
        f"SELECT * FROM {table} WHERE {quote(key_fields[0])}::text IN ({placeholders})",
        params,
    )


def _as_count(value: object) -> int:
    return 0 if value is None else int(str(value))
