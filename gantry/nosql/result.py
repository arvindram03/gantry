# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field

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
