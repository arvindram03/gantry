# SPDX-License-Identifier: Apache-2.0
"""A job: the work an Operation compiles to.

Content-addressed, like every other generated artifact in this project, and for
the same reason — a job either is the one a Result came from or is a different
job. Generation time is excluded from the hash so that recompiling the same unit
of work yields the same identity whenever it happens.

Nothing here knows what a container is. The job carries a *packaging*, and what
that packaging turns out to be is somebody else's concern — which is the whole
point of separating them.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator

from gantry.core.names import ContentHash, ResourceName
from gantry.jobs.packaging import Packaging


class JobKind(StrEnum):
    """What sort of work this is.

    Not *how* it is packaged and not *where* it runs — what it is. A SQL script
    is the same job whether it arrives as a container today or as something else
    later.
    """

    SQL = "sql"
    BEAM = "beam"


class Job(BaseModel):
    """One unit of work, compiled and ready to hand to a runner."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # The Operation this belongs to. A `ResourceName`, not "the Movement",
    # because an Analysis compiles to a job too.
    operation: ResourceName
    kind: JobKind
    # What this job covers — one partition, or a group of them. Opaque to the
    # runner and meaningful to the plan.
    unit: str
    # The thing to run. Retained as provenance and meant to be read by a person,
    # so it must never carry a credential.
    body: str
    packaging: Packaging
    generated_at: datetime

    @model_validator(mode="after")
    def _check_job(self) -> Job:
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must be timezone-aware")
        if not self.body.strip():
            raise ValueError("a job with no body is not a job")
        if not self.unit.strip():
            raise ValueError("a job must say which unit of work it covers")
        return self

    def canonical(self) -> str:
        """The bytes the hash is taken over.

        Generation time is excluded so recompiling is idempotent. The packaging
        *is* included: the same script in a different image is a different job,
        which is what stops an image changing underneath a replay from looking
        like the same work behaving differently.
        """
        return "\n".join(
            (
                f"operation={self.operation}",
                f"kind={self.kind.value}",
                f"unit={self.unit}",
                f"packaging={self.packaging.identity()}",
                "body=",
                self.body,
            )
        )

    @property
    def content_hash(self) -> ContentHash:
        digest = hashlib.sha256(self.canonical().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    def describe(self) -> str:
        return f"{self.kind.value} job for {self.operation} {self.unit} ({self.content_hash[:19]}…)"
