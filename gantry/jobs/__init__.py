# SPDX-License-Identifier: Apache-2.0
"""Jobs, packaging and runners.

Three things that change on different schedules, so they get three names:

- a **job** is *what* to run — generated from a spec, content-addressed
- a **packaging** is *how* it is made runnable — a container, today
- a **runner** is *where* it runs — local Docker, today

Only `gantry.jobs.packaging.container` and `gantry.jobs.runners.docker` know
what a container is. Everything else names a packaging kind and nothing more.
"""

from __future__ import annotations

from gantry.jobs.execute import JobFailedError, run_to_completion
from gantry.jobs.model import Job, JobKind
from gantry.jobs.packaging import ContainerPackaging, Packaging, PackagingKind
from gantry.jobs.runner import (
    FailureKind,
    JobHandle,
    JobState,
    JobStatus,
    Runner,
    RunnerError,
    UnsupportedPackagingError,
)

__all__ = [
    "ContainerPackaging",
    "FailureKind",
    "Job",
    "JobFailedError",
    "JobHandle",
    "JobKind",
    "JobState",
    "JobStatus",
    "Packaging",
    "PackagingKind",
    "Runner",
    "RunnerError",
    "UnsupportedPackagingError",
    "run_to_completion",
]
