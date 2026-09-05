# SPDX-License-Identifier: Apache-2.0
"""Running the verification a plan requires.

Verification is a lifecycle stage, not a report produced afterwards. A
Movement that finished every partition is not a Movement whose output can be
trusted, and the difference is decided here.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest
from gantry.core.evidence import (
    ScopeKind,
    Severity,
    VerificationResult,
    VerificationScope,
    VerificationStatus,
)
from gantry.core.verification import VerificationRequirement
from gantry.movement.model import Movement, MovementDataset
from gantry.movement.partitioning import Partition
from gantry.verification.base import VerificationContext, VerifierRegistry
from gantry.verification.movement import movement_verifiers


@dataclass(frozen=True)
class VerificationReport:
    """Everything a verification pass found."""

    results: tuple[VerificationResult, ...]

    @property
    def passed(self) -> bool:
        """True when nothing critical failed.

        Warnings do not make a Result untrustworthy; they make it worth
        reading.
        """
        return not self.blocking

    @property
    def blocking(self) -> tuple[VerificationResult, ...]:
        return tuple(result for result in self.results if result.blocks_cutover)

    @property
    def failed_scopes(self) -> tuple[str, ...]:
        """Where the failures are, which is what an operator needs first."""
        return tuple(str(result.scope) for result in self.blocking)


class MovementVerificationRunner:
    """Runs a Movement's declared checks."""

    def __init__(
        self,
        *,
        source_engine: AsyncEngine,
        target_engine: AsyncEngine,
        registry: VerifierRegistry | None = None,
    ) -> None:
        self._source_engine = source_engine
        self._target_engine = target_engine
        self._registry = registry or VerifierRegistry(movement_verifiers())

    async def verify_dataset(
        self,
        movement: Movement,
        dataset: MovementDataset,
        manifest: DatasetManifest,
        *,
        plan_version: int,
        partitions: Sequence[Partition] = (),
    ) -> VerificationReport:
        """Verify one dataset, and each of its partitions where given.

        Dataset-scope checks answer whether the whole thing is right;
        partition-scope checks answer where it is wrong. Running both is what
        turns "the counts disagree" into "partition 14 is short".
        """
        results: list[VerificationResult] = []

        for requirement in dataset.verification:
            results.append(
                await self._run(
                    movement,
                    dataset,
                    manifest,
                    requirement,
                    plan_version=plan_version,
                    scope=VerificationScope(kind=ScopeKind.DATASET, dataset=dataset.name),
                )
            )

            for partition in partitions:
                results.append(
                    await self._run(
                        movement,
                        dataset,
                        manifest,
                        requirement,
                        plan_version=plan_version,
                        scope=VerificationScope(
                            kind=ScopeKind.PARTITION,
                            dataset=dataset.name,
                            identifier=partition.id,
                        ),
                        partition=partition,
                    )
                )

        return VerificationReport(results=tuple(results))

    async def _run(
        self,
        movement: Movement,
        dataset: MovementDataset,
        manifest: DatasetManifest,
        requirement: VerificationRequirement,
        *,
        plan_version: int,
        scope: VerificationScope,
        partition: Partition | None = None,
    ) -> VerificationResult:
        verifier = self._registry.get(requirement.check)
        if verifier is None:
            # A check nobody implements is not a check that passed.
            return VerificationResult(
                check=requirement.check,
                status=VerificationStatus.ERRORED,
                scope=scope,
                severity=Severity.CRITICAL,
                operation=movement.name,
                plan_version=plan_version,
                difference=f"no verifier implements {requirement.check.value}",
                observed_at=datetime.now(UTC),
            )

        context = VerificationContext(
            operation=movement.name,
            plan_version=plan_version,
            manifest=manifest,
            target=dataset.target,
            scope=scope,
            source_engine=self._source_engine,
            target_engine=self._target_engine,
            requirement=requirement,
            partition=partition,
        )

        try:
            return await verifier.verify(context)
        except Exception as error:
            return VerificationResult(
                check=requirement.check,
                status=VerificationStatus.ERRORED,
                scope=scope,
                severity=Severity.CRITICAL,
                operation=movement.name,
                plan_version=plan_version,
                difference=f"{type(error).__name__}: {error}",
                observed_at=datetime.now(UTC),
            )
