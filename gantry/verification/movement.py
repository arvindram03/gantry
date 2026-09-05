"""Movement verifiers.

Each one compares what the source says against what the target says, or checks
an invariant the target must hold on its own. All of the counting happens in
the database; what crosses into Python is a number.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.evidence import Severity, VerificationResult, VerificationStatus
from gantry.core.verification import CheckName, NullRateCheck
from gantry.state.database import transaction
from gantry.verification.base import VerificationContext, Verifier
from gantry.verification.sql import partition_predicate, qualified, quote


def _now(context: VerificationContext) -> datetime:
    return context.observed_at or datetime.now(UTC)


def _result(
    context: VerificationContext,
    check: CheckName,
    status: VerificationStatus,
    *,
    source: str | None = None,
    target: str | None = None,
    difference: str | None = None,
    evidence: dict[str, str] | None = None,
    severity: Severity = Severity.CRITICAL,
) -> VerificationResult:
    return VerificationResult(
        check=check,
        status=status,
        scope=context.scope,
        severity=severity,
        operation=context.operation,
        plan_version=context.plan_version,
        source_result=source,
        target_result=target,
        difference=difference,
        evidence=evidence or {},
        observed_at=_now(context),
    )


async def _scalar(engine: AsyncEngine, query: str, params: dict[str, str]) -> int:
    async with transaction(engine) as connection:
        return int((await connection.execute(text(query), params)).scalar_one())


class RowCountVerifier:
    """Source and target must hold the same number of rows in scope.

    The cheapest question worth asking, and the one that catches whole
    partitions that never landed.
    """

    check = CheckName.ROW_COUNT

    async def verify(self, context: VerificationContext) -> VerificationResult:
        where, params = partition_predicate(context.manifest, context.partition)
        source_table = qualified(context.manifest.physical.reference)
        target_table = qualified(context.target)

        source = await _scalar(
            context.source_engine, f"SELECT count(*) FROM {source_table} WHERE {where}", params
        )
        target = await _scalar(
            context.target_engine, f"SELECT count(*) FROM {target_table} WHERE {where}", params
        )

        evidence = {"predicate": where, **params}
        if source == target:
            return _result(
                context,
                self.check,
                VerificationStatus.PASSED,
                source=str(source),
                target=str(target),
                evidence=evidence,
            )

        missing = source - target
        direction = "missing from target" if missing > 0 else "extra in target"
        return _result(
            context,
            self.check,
            VerificationStatus.FAILED,
            source=str(source),
            target=str(target),
            difference=f"{abs(missing):,} rows {direction}",
            evidence=evidence,
        )


class PrimaryKeyUniqueVerifier:
    """The target's key must actually be unique.

    A target-only invariant: duplicated keys mean the write path produced a
    second effect, which is the failure idempotency exists to prevent.
    """

    check = CheckName.PRIMARY_KEY_UNIQUE

    async def verify(self, context: VerificationContext) -> VerificationResult:
        keys = context.manifest.dataset_schema.keys
        if not keys:
            return _result(
                context,
                self.check,
                VerificationStatus.SKIPPED,
                evidence={"reason": "dataset has no declared key"},
            )

        where, params = partition_predicate(context.manifest, context.partition)
        columns = ", ".join(quote(key) for key in keys)
        table = qualified(context.target)
        duplicates = await _scalar(
            context.target_engine,
            f"SELECT count(*) - count(DISTINCT ({columns})) FROM {table} WHERE {where}",
            params,
        )

        evidence = {"key": ", ".join(keys), "predicate": where, **params}
        if duplicates == 0:
            return _result(
                context,
                self.check,
                VerificationStatus.PASSED,
                target="0 duplicates",
                evidence=evidence,
            )
        return _result(
            context,
            self.check,
            VerificationStatus.FAILED,
            target=f"{duplicates:,} duplicates",
            difference=f"{duplicates:,} duplicate keys in target",
            evidence=evidence,
        )


class NullRateVerifier:
    """A field's null rate must stay within a declared bound."""

    check = CheckName.NULL_RATE

    async def verify(self, context: VerificationContext) -> VerificationResult:
        requirement = context.requirement
        if not isinstance(requirement, NullRateCheck):
            return _result(
                context,
                self.check,
                VerificationStatus.ERRORED,
                difference="null_rate requires a field and a maximum",
            )

        where, params = partition_predicate(context.manifest, context.partition)
        field = quote(requirement.field)
        table = qualified(context.target)

        async with transaction(context.target_engine) as connection:
            row = (
                await connection.execute(
                    text(
                        f"SELECT count(*) AS total, "
                        f"count(*) FILTER (WHERE {field} IS NULL) AS nulls "
                        f"FROM {table} WHERE {where}"
                    ),
                    params,
                )
            ).one()

        total = int(row.total)
        nulls = int(row.nulls)
        rate = 0.0 if total == 0 else nulls / total
        evidence = {
            "field": requirement.field,
            "max": str(requirement.max),
            "nulls": str(nulls),
            "rows": str(total),
        }

        if rate <= requirement.max:
            return _result(
                context,
                self.check,
                VerificationStatus.PASSED,
                target=f"{rate:.4f}",
                evidence=evidence,
            )
        return _result(
            context,
            self.check,
            VerificationStatus.FAILED,
            target=f"{rate:.4f}",
            difference=f"null rate {rate:.4f} exceeds maximum {requirement.max}",
            evidence=evidence,
        )


class ForeignKeyIntegrityVerifier:
    """Every referencing row in the target must find its parent.

    A migration that copies children before parents, or loses a parent
    partition, produces orphans that no row count would reveal.
    """

    check = CheckName.FOREIGN_KEY_INTEGRITY

    async def verify(self, context: VerificationContext) -> VerificationResult:
        foreign_keys = context.manifest.dataset_schema.foreign_keys
        if not foreign_keys:
            return _result(
                context,
                self.check,
                VerificationStatus.SKIPPED,
                evidence={"reason": "dataset declares no foreign keys"},
            )

        where, params = partition_predicate(context.manifest, context.partition)
        table = qualified(context.target)
        orphans = 0
        details: dict[str, str] = {"predicate": where, **params}

        for index, key in enumerate(foreign_keys):
            # The parent is verified in the target, which is where integrity
            # has to hold - the source is already known to be consistent.
            parent = qualified(key.references)
            on = " AND ".join(
                f"child.{quote(child)} = parent.{quote(referenced)}"
                for child, referenced in zip(key.columns, key.referenced_columns, strict=True)
            )
            not_null = " AND ".join(f"child.{quote(child)} IS NOT NULL" for child in key.columns)
            anchor = quote(key.referenced_columns[0])
            found = await _scalar(
                context.target_engine,
                f"SELECT count(*) FROM {table} AS child "
                f"LEFT JOIN {parent} AS parent ON {on} "
                f"WHERE {where} AND {not_null} AND parent.{anchor} IS NULL",
                params,
            )
            orphans += found
            details[f"fk{index}"] = f"{'/'.join(key.columns)} -> {key.references}: {found} orphans"

        if orphans == 0:
            return _result(
                context,
                self.check,
                VerificationStatus.PASSED,
                target="0 orphans",
                evidence=details,
            )
        return _result(
            context,
            self.check,
            VerificationStatus.FAILED,
            target=f"{orphans:,} orphans",
            difference=f"{orphans:,} rows reference a parent the target does not hold",
            evidence=details,
        )


def movement_verifiers() -> tuple[Verifier, ...]:
    return (
        RowCountVerifier(),
        PrimaryKeyUniqueVerifier(),
        NullRateVerifier(),
        ForeignKeyIntegrityVerifier(),
    )
