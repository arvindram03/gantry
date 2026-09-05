"""Analysis verifiers.

These implement the same protocol as the Movement verifiers and produce the
same evidence. What differs is only what they measure: a Movement verifier
compares a source against a target, an Analysis verifier bounds what a
computation did.

They all measure the **joined relation**, not the aggregate. A join that
multiplies its left side is invisible once the rows are grouped away, and an
engine will report success either way - which is the whole reason these checks
exist.
"""

from __future__ import annotations

from datetime import UTC, datetime

from gantry.adapters.engine.base import EngineAdapter, as_float, as_int
from gantry.analysis.artifact import ArtifactLanguage, GeneratedArtifact
from gantry.analysis.compiler import (
    compile_relation,
    left_input,
    normalised_name,
    qualified_column,
)
from gantry.analysis.model import Analysis
from gantry.core.dataset import DatasetManifest
from gantry.core.evidence import (
    ScopeKind,
    Severity,
    VerificationResult,
    VerificationScope,
    VerificationStatus,
)
from gantry.core.verification import (
    CheckName,
    JoinCoverageCheck,
    NullRateCheck,
    RowExpansionCheck,
    TemporalAlignmentCheck,
    VerificationRequirement,
)
from gantry.verification.sql import quote


class AnalysisVerificationContext:
    """What an Analysis verifier needs.

    Carries the Analysis and its manifests rather than a table name, because
    these checks compile their own measurement queries from the same source the
    artifact was compiled from.
    """

    def __init__(
        self,
        *,
        analysis: Analysis,
        manifests: dict[str, DatasetManifest],
        adapter: EngineAdapter,
        requirement: VerificationRequirement,
        plan_version: int = 1,
        observed_at: datetime | None = None,
    ) -> None:
        self.analysis = analysis
        self.manifests = manifests
        self.adapter = adapter
        self.requirement = requirement
        self.plan_version = plan_version
        self.observed_at = observed_at or datetime.now(UTC)

    @property
    def scope(self) -> VerificationScope:
        return VerificationScope(kind=ScopeKind.OPERATION, dataset=self.analysis.name)

    def measurement(self, projection: str) -> GeneratedArtifact:
        """A query measuring the joined relation.

        Built from the same relation the artifact aggregates over, so a
        measurement cannot disagree with the thing it is measuring.
        """
        prefix, relation = compile_relation(self.analysis, self.manifests)
        body = f"{prefix}\nSELECT {projection}\nFROM {relation}".strip()
        return GeneratedArtifact(
            analysis=self.analysis.name,
            engine=self.adapter.engine,
            language=ArtifactLanguage.SQL,
            body=body,
            inputs=self.analysis.inputs,
            generated_at=self.observed_at,
        )

    def result(
        self,
        check: CheckName,
        status: VerificationStatus,
        *,
        measured: str | None = None,
        expected: str | None = None,
        difference: str | None = None,
        evidence: dict[str, str] | None = None,
    ) -> VerificationResult:
        return VerificationResult(
            check=check,
            status=status,
            scope=self.scope,
            severity=Severity.CRITICAL,
            operation=self.analysis.name,
            plan_version=self.plan_version,
            source_result=expected,
            target_result=measured,
            difference=difference,
            evidence=evidence or {},
            observed_at=self.observed_at,
        )


async def _measure(context: AnalysisVerificationContext, projection: str) -> dict[str, object]:
    artifact = context.measurement(projection)
    result = await context.adapter.execute(artifact)
    rows = result.as_dicts()
    return rows[0] if rows else {}


class RowExpansionVerifier:
    """Bounds how far a join multiplied its input.

    The clearest expression of the guarantee boundary. An engine will happily
    report SUCCESS on a join that turns eighty-four million rows into one point
    seven billion - it did exactly what it was asked. Whether the result means
    anything is a different question, and this is where it is answered.
    """

    check = CheckName.ROW_EXPANSION

    async def verify(self, context: AnalysisVerificationContext) -> VerificationResult:
        requirement = context.requirement
        if not isinstance(requirement, RowExpansionCheck):
            return context.result(
                self.check,
                VerificationStatus.ERRORED,
                difference="rowExpansion requires a maximum",
            )

        left = left_input(context.analysis)
        measured = await _measure(context, "count(*) AS joined_rows")
        joined = as_int(measured.get("joined_rows"))

        base_artifact = GeneratedArtifact(
            analysis=context.analysis.name,
            engine=context.adapter.engine,
            body=f"SELECT count(*) AS base_rows FROM {quote(normalised_name(left))}",
            inputs=(left,),
            generated_at=context.observed_at,
        )
        prefix, _ = compile_relation(context.analysis, context.manifests)
        base_artifact = base_artifact.model_copy(
            update={"body": f"{prefix}\n{base_artifact.body}".strip()}
        )
        base_rows = as_int(
            (await context.adapter.execute(base_artifact)).as_dicts()[0]["base_rows"]
        )

        ratio = float(joined) / base_rows if base_rows else 0.0
        evidence = {
            "joined_rows": str(joined),
            "base_rows": str(base_rows),
            "base_dataset": left,
            "max": str(requirement.max),
        }

        if ratio <= requirement.max:
            return context.result(
                self.check,
                VerificationStatus.PASSED,
                measured=f"{ratio:.4f}x",
                expected=f"<= {requirement.max}x",
                evidence=evidence,
            )
        return context.result(
            self.check,
            VerificationStatus.FAILED,
            measured=f"{ratio:.4f}x",
            expected=f"<= {requirement.max}x",
            difference=(
                f"the join expanded {base_rows:,} rows to {joined:,} "
                f"({ratio:.2f}x), over the declared maximum of {requirement.max}x"
            ),
            evidence=evidence,
        )


class JoinCoverageVerifier:
    """The fraction of left rows that found a match.

    A join can stay within its expansion bound by matching almost nothing,
    which produces a result that is small, fast, and about a different
    population than the one asked about.
    """

    check = CheckName.JOIN_COVERAGE

    async def verify(self, context: AnalysisVerificationContext) -> VerificationResult:
        requirement = context.requirement
        if not isinstance(requirement, JoinCoverageCheck):
            return context.result(
                self.check,
                VerificationStatus.ERRORED,
                difference="joinCoverage requires a minimum",
            )

        joins = context.analysis.joins
        if not joins:
            return context.result(
                self.check,
                VerificationStatus.SKIPPED,
                evidence={"reason": "the analysis performs no joins"},
            )

        witness = _match_witness(context, joins[-1].right)
        measured = await _measure(
            context,
            f"count(*) AS total, count({witness}) AS matched",
        )
        total = as_int(measured.get("total"))
        matched = as_int(measured.get("matched"))
        coverage = matched / total if total else 0.0
        evidence = {
            "matched": str(matched),
            "total": str(total),
            "min": str(requirement.min),
            "witness": witness,
        }

        if coverage >= requirement.min:
            return context.result(
                self.check,
                VerificationStatus.PASSED,
                measured=f"{coverage:.4f}",
                expected=f">= {requirement.min}",
                evidence=evidence,
            )
        return context.result(
            self.check,
            VerificationStatus.FAILED,
            measured=f"{coverage:.4f}",
            expected=f">= {requirement.min}",
            difference=(
                f"only {matched:,} of {total:,} rows found a match "
                f"({coverage:.2%}), under the declared minimum of {requirement.min:.2%}"
            ),
            evidence=evidence,
        )


class TemporalAlignmentVerifier:
    """How far apart in time the joined sides actually were.

    A temporal join within its distance bound can still be joining things that
    are barely related. Measuring the worst case says whether the correlation
    is worth anything.
    """

    check = CheckName.TEMPORAL_ALIGNMENT

    async def verify(self, context: AnalysisVerificationContext) -> VerificationResult:
        requirement = context.requirement
        if not isinstance(requirement, TemporalAlignmentCheck):
            return context.result(
                self.check,
                VerificationStatus.ERRORED,
                difference="temporalAlignment requires a maximum difference",
            )

        temporal_joins = [join for join in context.analysis.joins if join.temporal]
        if not temporal_joins:
            return context.result(
                self.check,
                VerificationStatus.SKIPPED,
                evidence={"reason": "the analysis performs no temporal joins"},
            )

        join = temporal_joins[-1]
        left_time = context.manifests[join.left].dataset_schema.time_field
        right_time = context.manifests[join.right].dataset_schema.time_field
        if left_time is None or right_time is None:
            return context.result(
                self.check,
                VerificationStatus.ERRORED,
                difference="a temporal join needs a time field on both sides",
            )

        left_ref = f"{quote(join.left.replace('.', '_'))}.{quote(left_time)}"
        right_ref = f"{quote(join.right.replace('.', '_'))}.{quote(right_time)}"
        measured = await _measure(
            context,
            f"max(abs(extract(epoch FROM {left_ref} - {right_ref}))) AS worst_seconds",
        )
        worst = measured.get("worst_seconds")
        if worst is None:
            return context.result(
                self.check,
                VerificationStatus.SKIPPED,
                evidence={"reason": "no rows matched, so there is no skew to measure"},
            )

        seconds = as_float(worst)
        allowed = requirement.max_difference.total_seconds()
        evidence = {"worst_seconds": f"{seconds:.1f}", "max_seconds": f"{allowed:.0f}"}

        if seconds <= allowed:
            return context.result(
                self.check,
                VerificationStatus.PASSED,
                measured=f"{seconds:.1f}s",
                expected=f"<= {allowed:.0f}s",
                evidence=evidence,
            )
        return context.result(
            self.check,
            VerificationStatus.FAILED,
            measured=f"{seconds:.1f}s",
            expected=f"<= {allowed:.0f}s",
            difference=(
                f"joined rows were up to {seconds:.0f}s apart, "
                f"over the declared maximum of {allowed:.0f}s"
            ),
            evidence=evidence,
        )


class OutputNullRateVerifier:
    """The null rate of a field in the joined relation.

    The Movement verifier of the same name measures a target table; this one
    measures a computation's output. Same question, different subject - which
    is why they are two verifiers behind one check name rather than one
    verifier with a mode flag.
    """

    check = CheckName.NULL_RATE

    async def verify(self, context: AnalysisVerificationContext) -> VerificationResult:
        requirement = context.requirement
        if not isinstance(requirement, NullRateCheck):
            return context.result(
                self.check,
                VerificationStatus.ERRORED,
                difference="nullRate requires a field and a maximum",
            )

        column = qualified_column(context.analysis, context.manifests, requirement.field)
        measured = await _measure(
            context,
            f"count(*) AS total, count(*) FILTER (WHERE {column} IS NULL) AS nulls",
        )
        total = as_int(measured.get("total"))
        nulls = as_int(measured.get("nulls"))
        rate = nulls / total if total else 0.0
        evidence = {
            "field": requirement.field,
            "nulls": str(nulls),
            "rows": str(total),
            "max": str(requirement.max),
        }

        if rate <= requirement.max:
            return context.result(
                self.check,
                VerificationStatus.PASSED,
                measured=f"{rate:.4f}",
                expected=f"<= {requirement.max}",
                evidence=evidence,
            )
        return context.result(
            self.check,
            VerificationStatus.FAILED,
            measured=f"{rate:.4f}",
            expected=f"<= {requirement.max}",
            difference=(
                f"{nulls:,} of {total:,} rows have no {requirement.field} "
                f"({rate:.2%}), over the declared maximum of {requirement.max:.2%}"
            ),
            evidence=evidence,
        )


def _match_witness(context: AnalysisVerificationContext, right: str) -> str:
    """A column that is NULL exactly when the right side did not match.

    The join key would be wrong: normalisation makes it equal on both sides, so
    it is never null. A column unique to the right input is.
    """
    manifest = context.manifests[right]
    aliases = {alias for item in context.analysis.normalize for alias in item.aliases}
    candidates = [
        field.name for field in manifest.dataset_schema.fields if field.name not in aliases
    ]
    keys = manifest.dataset_schema.keys
    chosen = next((key for key in keys if key in candidates), None) or (
        candidates[0] if candidates else None
    )
    if chosen is None:  # pragma: no cover - a dataset of only join keys
        raise ValueError(f"no column on {right!r} can witness a match")
    return f"{quote(right.replace('.', '_'))}.{quote(chosen)}"


def analysis_verifiers() -> dict[CheckName, object]:
    return {
        CheckName.ROW_EXPANSION: RowExpansionVerifier(),
        CheckName.JOIN_COVERAGE: JoinCoverageVerifier(),
        CheckName.TEMPORAL_ALIGNMENT: TemporalAlignmentVerifier(),
        CheckName.NULL_RATE: OutputNullRateVerifier(),
    }
