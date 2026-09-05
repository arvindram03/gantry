# SPDX-License-Identifier: Apache-2.0
"""The verifier interface.

Written against Operations, not against Movement. An Analysis verifier bounding
row expansion and a Movement verifier comparing row counts implement the same
protocol and produce the same evidence; only what they measure differs. Getting
that right now is what makes the Analysis checks new verifiers rather than a
second framework.

Every verifier pushes its work into SQL on the system that holds the data.
Comparing a hundred million rows in Python would make verification cost more
than the migration it is checking.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest
from gantry.core.evidence import VerificationResult, VerificationScope
from gantry.core.verification import CheckName, VerificationRequirement
from gantry.movement.partitioning import Partition


@dataclass(frozen=True)
class VerificationContext:
    """Everything a verifier needs to answer its question."""

    operation: str
    plan_version: int
    manifest: DatasetManifest
    target: str
    scope: VerificationScope
    source_engine: AsyncEngine
    target_engine: AsyncEngine
    requirement: VerificationRequirement
    # Present when verifying one partition rather than a whole dataset.
    partition: Partition | None = None
    observed_at: datetime | None = None


class Verifier(Protocol):
    """Answers one question about an Operation's output."""

    @property
    def check(self) -> CheckName: ...

    async def verify(self, context: VerificationContext) -> VerificationResult: ...


class VerifierRegistry:
    """Which verifier answers which check.

    A requirement naming a check nothing implements is an error rather than a
    silent pass: a verification that did not run has not been satisfied.
    """

    def __init__(self, verifiers: Sequence[Verifier] = ()) -> None:
        self._by_check: dict[CheckName, Verifier] = {
            verifier.check: verifier for verifier in verifiers
        }

    def register(self, verifier: Verifier) -> None:
        self._by_check[verifier.check] = verifier

    def get(self, check: CheckName) -> Verifier | None:
        return self._by_check.get(check)

    def supports(self, check: CheckName) -> bool:
        return check in self._by_check

    @property
    def checks(self) -> tuple[CheckName, ...]:
        return tuple(sorted(self._by_check))
