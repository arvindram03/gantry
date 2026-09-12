# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field

from gantry.evidence import EvidenceBundle
from gantry.failure import Failure
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.nosql.output import InlineDocuments
from gantry.output import OutputKind, OutputRef
from gantry.result import ResultStatus
from gantry.verifier import VerificationResult


@dataclass(frozen=True, slots=True)
class NoSQLResult:
    status: ResultStatus
    handle: ExecutionHandle | None = None
    inline: InlineDocuments | None = None
    outputs: tuple[OutputRef, ...] = ()
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    verification: VerificationResult | None = None
    failure: Failure | None = None
    run: object | None = None
    """The durable run record for this operation."""

    @property
    def run_id(self) -> str | None:
        return None if self.run is None else str(getattr(self.run, "id", None))

    evidence: EvidenceBundle | None = None

    @property
    def ok(self) -> bool:
        return self.status is ResultStatus.ACCEPTED

    @property
    def uri(self) -> str | None:
        """Return the first engine-owned output URI, excluding inline documents."""

        return next(
            (output.uri for output in self.outputs if output.kind is not OutputKind.INLINE),
            None,
        )
