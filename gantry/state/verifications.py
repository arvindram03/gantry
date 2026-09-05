# SPDX-License-Identifier: Apache-2.0
"""Persisting verification results.

Findings are append-only. A verification result is a statement about what was
true at a moment, and rewriting it would make the audit trail describe a
migration that did not happen.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from sqlalchemy import Row, select
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.evidence import (
    ScopeKind,
    Severity,
    VerificationResult,
    VerificationScope,
    VerificationStatus,
)
from gantry.core.verification import CheckName
from gantry.state.database import transaction
from gantry.state.tables import verification_results


class VerificationStore(Protocol):
    async def record(self, results: Sequence[VerificationResult]) -> None: ...

    async def for_operation(
        self, operation: str, plan_version: int | None = None
    ) -> Sequence[VerificationResult]: ...


class PostgresVerificationStore:
    """Verification findings in the metadata store."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(self, results: Sequence[VerificationResult]) -> None:
        if not results:
            return
        rows = [
            {
                "operation": result.operation,
                "plan_version": result.plan_version,
                "check_name": result.check.value,
                "scope": str(result.scope),
                "status": result.status.value,
                "severity": result.severity.value,
                "evidence": {
                    **result.evidence,
                    "scope_kind": result.scope.kind.value,
                    **({"dataset": result.scope.dataset} if result.scope.dataset else {}),
                    **({"identifier": result.scope.identifier} if result.scope.identifier else {}),
                    **({"source": result.source_result} if result.source_result else {}),
                    **({"target": result.target_result} if result.target_result else {}),
                    **({"difference": result.difference} if result.difference else {}),
                },
                "observed_at": result.observed_at,
            }
            for result in results
        ]
        async with transaction(self._engine) as connection:
            await connection.execute(verification_results.insert(), rows)

    async def for_operation(
        self, operation: str, plan_version: int | None = None
    ) -> Sequence[VerificationResult]:
        query = select(verification_results).where(verification_results.c.operation == operation)
        if plan_version is not None:
            query = query.where(verification_results.c.plan_version == plan_version)
        async with transaction(self._engine) as connection:
            rows = (await connection.execute(query.order_by(verification_results.c.id))).all()
        return tuple(_rehydrate(row) for row in rows)


def _rehydrate(row: Row[tuple[object, ...]]) -> VerificationResult:
    mapping = row._mapping
    evidence = dict(mapping["evidence"])
    return VerificationResult(
        check=CheckName(mapping["check_name"]),
        status=VerificationStatus(mapping["status"]),
        scope=VerificationScope(
            kind=ScopeKind(evidence.pop("scope_kind", "dataset")),
            dataset=evidence.pop("dataset", None),
            identifier=evidence.pop("identifier", None),
        ),
        severity=Severity(mapping["severity"]),
        operation=mapping["operation"],
        plan_version=mapping["plan_version"],
        source_result=evidence.pop("source", None),
        target_result=evidence.pop("target", None),
        difference=evidence.pop("difference", None),
        evidence=evidence,
        observed_at=mapping["observed_at"],
    )
