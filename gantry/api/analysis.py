# SPDX-License-Identifier: Apache-2.0
"""`gantry.analysis.{plan,execute,status}`.

Thin on purpose. The lifecycle already lives in `AnalysisService`; this is the
typed surface an agent or a script calls, and its job is to expose plan and
execute as separate steps.

That separation is the point rather than an accident of layering: planning
compiles and validates without running anything, so a caller can see the SQL,
the cost estimate and the refusals before committing to execution. An API where
the only verb is "run it" gives an agent nothing to inspect.
"""

from __future__ import annotations

from dataclasses import dataclass

from gantry.analysis.artifact import GeneratedArtifact
from gantry.analysis.model import Analysis
from gantry.analysis.service import AnalysisRun, AnalysisService
from gantry.analysis.validate import ValidationReport
from gantry.core.operation import OperationState
from gantry.state.operations import OperationStore, UnknownOperationError


@dataclass(frozen=True)
class AnalysisPlan:
    """What would run, and whether it is allowed to."""

    analysis: str
    artifact: GeneratedArtifact
    validation: ValidationReport

    @property
    def accepted(self) -> bool:
        return self.validation.accepted

    def describe(self) -> str:
        head = f"{self.analysis}  {self.artifact.content_hash}"
        return head if self.accepted else f"{head}  refused: {self.validation.describe()}"


class AnalysisApi:
    """The Analysis half of the agent-facing API."""

    def __init__(
        self, *, service: AnalysisService, operations: OperationStore | None = None
    ) -> None:
        self._service = service
        self._operations = operations

    async def plan(self, analysis: Analysis) -> AnalysisPlan:
        """Compile and validate, without executing."""
        artifact, validation = await self._service.prepare(analysis)
        return AnalysisPlan(analysis=analysis.name, artifact=artifact, validation=validation)

    async def execute(self, analysis: Analysis) -> AnalysisRun:
        """Run the whole lifecycle: plan, validate, execute, verify, result."""
        return await self._service.run(analysis)

    async def status(self, name: str) -> OperationState | None:
        """Where an Analysis operation has got to, or None if it is unknown."""
        if self._operations is None:
            return None
        try:
            return (await self._operations.get(name)).state
        except UnknownOperationError:
            return None
