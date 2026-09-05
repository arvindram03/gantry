"""Running an Analysis through the lifecycle.

Plan, generate, validate, execute, verify, result - the same sequence a
Movement runs, with different work at each stage. Verification decides whether
the Result is trustworthy, and an engine reporting success does not.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from gantry.adapters.engine.base import EngineAdapter, QueryResult
from gantry.analysis.artifact import GeneratedArtifact
from gantry.analysis.compiler import compile_analysis
from gantry.analysis.findings import derive_findings
from gantry.analysis.model import Analysis
from gantry.analysis.result import AnalysisResult, result_name
from gantry.analysis.validate import ValidationPolicy, ValidationReport, validate
from gantry.core.dataset import DatasetManifest, DatasetVersion
from gantry.core.evidence import VerificationResult
from gantry.core.positions import Checkpoint
from gantry.core.provenance import DatasetPin, Lineage, Provenance
from gantry.core.results import ResultStatus
from gantry.core.verification import CheckName, VerificationRequirement
from gantry.verification.analysis import AnalysisVerificationContext, analysis_verifiers


@dataclass
class AnalysisRun:
    """Everything one run produced, including what it refused to conclude."""

    analysis: str
    artifact: GeneratedArtifact
    validation: ValidationReport
    result: AnalysisResult | None = None
    output: QueryResult | None = None
    verification: tuple[VerificationResult, ...] = field(default_factory=tuple)

    @property
    def executed(self) -> bool:
        return self.output is not None

    def describe(self) -> str:
        if not self.validation.accepted:
            return f"refused before execution: {self.validation.describe()}"
        if self.result is None:  # pragma: no cover - defensive
            return "executed, no result"
        return f"{self.result.status.value}: {len(self.result.findings)} findings"


class AnalysisService:
    """Drives one Analysis from spec to Result."""

    def __init__(
        self,
        *,
        adapter: EngineAdapter,
        manifests: dict[str, DatasetManifest],
        pins: Sequence[DatasetPin] = (),
        checkpoints: Sequence[Checkpoint] = (),
        policy: ValidationPolicy | None = None,
    ) -> None:
        self._adapter = adapter
        self._manifests = manifests
        self._pins = tuple(pins)
        self._checkpoints = tuple(checkpoints)
        self._policy = policy

    async def prepare(self, analysis: Analysis) -> tuple[GeneratedArtifact, ValidationReport]:
        """Compile and validate, without executing.

        Split out so a caller can see the SQL, the estimate and the refusals
        before committing to a run. `run` goes through the same path, so what
        was inspected is what executes.
        """
        artifact = compile_analysis(analysis, self._manifests)
        validation = await validate(
            analysis, artifact, self._adapter, self._manifests, policy=self._policy
        )
        return artifact, validation

    async def run(self, analysis: Analysis) -> AnalysisRun:
        """Compile, validate, execute, verify, and build the Result."""
        started = datetime.now(UTC)
        artifact, validation = await self.prepare(analysis)
        run = AnalysisRun(analysis=analysis.name, artifact=artifact, validation=validation)
        if not validation.accepted:
            # Refused before execution: nothing ran, so there is nothing to
            # conclude and no Result to mistake for one.
            return run

        output = await self._adapter.execute(artifact)
        run.output = output

        verification = await self.verify(analysis)
        run.verification = verification
        blocking = tuple(finding for finding in verification if finding.blocks_cutover)

        finished = datetime.now(UTC)
        run.result = AnalysisResult(
            name=result_name(analysis.name),
            status=ResultStatus.VERIFICATION_FAILED if blocking else ResultStatus.OK,
            provenance=Provenance(
                generated_at=finished,
                operation=analysis.name,
                plan_version=1,
                lineage=Lineage(inputs=self._pins),
                artifacts=(artifact.to_ref(),),
                checkpoints=self._checkpoints,
                window=analysis.window,
            ),
            created_at=finished,
            verification=verification,
            # Findings are withheld when verification failed. A conclusion
            # drawn from a computation the runtime has rejected is not a
            # finding, it is a guess with provenance attached.
            findings=() if blocking else derive_findings(analysis, output),
            artifact_hash=artifact.content_hash,
            engine=self._adapter.engine,
            rows_returned=output.row_count,
            started_at=started,
            finished_at=finished,
        )
        return run

    async def verify(self, analysis: Analysis) -> tuple[VerificationResult, ...]:
        """Run the checks the spec declared against the joined relation."""
        verifiers = analysis_verifiers()
        results: list[VerificationResult] = []

        for requirement in analysis.verification:
            verifier = verifiers.get(requirement.check)
            if verifier is None:
                results.append(_unimplemented(analysis, requirement))
                continue
            context = AnalysisVerificationContext(
                analysis=analysis,
                manifests=self._manifests,
                adapter=self._adapter,
                requirement=requirement,
            )
            results.append(await verifier.verify(context))  # type: ignore[attr-defined]
        return tuple(results)


def _unimplemented(analysis: Analysis, requirement: VerificationRequirement) -> VerificationResult:
    """A declared check nothing implements is not a check that passed."""
    from gantry.core.evidence import (
        ScopeKind,
        Severity,
        VerificationScope,
        VerificationStatus,
    )

    return VerificationResult(
        check=requirement.check,
        status=VerificationStatus.ERRORED,
        scope=VerificationScope(kind=ScopeKind.OPERATION, dataset=analysis.name),
        severity=Severity.CRITICAL,
        operation=analysis.name,
        plan_version=1,
        difference=f"no Analysis verifier implements {requirement.check.value}",
        observed_at=datetime.now(UTC),
    )


def pins_for(versions: Sequence[DatasetVersion]) -> tuple[DatasetPin, ...]:
    return tuple(DatasetPin.from_version(version) for version in versions)


def known_analysis_checks() -> tuple[CheckName, ...]:
    return tuple(sorted(analysis_verifiers()))
