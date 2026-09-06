# SPDX-License-Identifier: Apache-2.0
"""Runners: where a job runs.

Two methods and a question. The question — `supports` — is what lets a
deployment hold several runners and route by packaging, and what makes a runner
refuse work it cannot run *before* launching rather than after.

Nothing in this protocol names a container, an image or a registry. A runner
that supports container packaging reads those fields; everything above this
interface does not know they exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from gantry.jobs.model import Job
from gantry.jobs.packaging import Packaging


class JobState(StrEnum):
    """Where a submitted job has got to."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in (JobState.SUCCEEDED, JobState.FAILED)


@dataclass(frozen=True)
class JobHandle:
    """A submitted job, addressable by whoever submitted it.

    Carries the runner's name as well as its id so a handle read back from the
    metadata store can be polled by the right runner — and so an operator can
    find the thing in the runner's own console.
    """

    runner: str
    id: str

    def describe(self) -> str:
        return f"{self.runner}:{self.id}"


class FailureKind(StrEnum):
    """Why a job is not going to succeed, at the only granularity that changes
    what the caller should do.

    A runner failure and a bad row are different, and conflating them is how a
    node gets quarantined for an infrastructure hiccup — or, worse, how a row
    the target will never accept is retried until something gives up.
    """

    # The work was not attempted, or was interrupted by the platform: the image
    # would not start, the container was killed, the daemon refused. Retrying is
    # safe and is usually right.
    RUNNER = "runner"
    # The work ran and failed on its own terms — a constraint violation, a type
    # error, a row the target refused. Retrying reproduces it.
    JOB = "job"


@dataclass(frozen=True)
class JobStatus:
    """What a runner reports about a submitted job."""

    state: JobState
    exit_code: int | None = None
    detail: str | None = None
    finished_at: datetime | None = None
    # Set only when the state is FAILED.
    failure: FailureKind | None = None

    @property
    def succeeded(self) -> bool:
        return self.state is JobState.SUCCEEDED

    def describe(self) -> str:
        code = "" if self.exit_code is None else f" (exit {self.exit_code})"
        return f"{self.state.value}{code}" + (f" — {self.detail}" if self.detail else "")


class UnsupportedPackagingError(Exception):
    """A runner was handed work it cannot run.

    Raised at submit rather than discovered at launch, because a job that fails
    two minutes into a container start is indistinguishable from one that failed
    for a real reason.
    """

    def __init__(self, runner: str, packaging: Packaging) -> None:
        super().__init__(f"runner {runner!r} cannot run {packaging.kind.value!r} packaging")
        self.runner = runner
        self.packaging = packaging


class RunnerError(Exception):
    """The runner itself failed — not the job.

    Kept separate because the two call for opposite responses: a runner that is
    unreachable should be retried, and a job that exited non-zero should not be
    retried blindly.
    """


class Runner(Protocol):
    """Somewhere a job can run."""

    @property
    def name(self) -> str:
        """Stable across restarts: it is half of every handle."""
        ...

    def supports(self, packaging: Packaging) -> bool:
        """Whether this runner can run work packaged this way."""
        ...

    async def submit(self, job: Job) -> JobHandle:
        """Start the job, or adopt it if it is already running.

        **Idempotent by contract.** A worker that dies between submitting and
        recording the submission must not cause a second copy of the work to
        run, so submitting a job that is already in flight returns the existing
        handle rather than starting another.
        """
        ...

    async def poll(self, handle: JobHandle) -> JobStatus:
        """What the job is doing now. Never blocks for the job to finish."""
        ...

    async def logs(self, handle: JobHandle) -> str:
        """Everything the job wrote.

        Part of the protocol rather than one runner's convenience: a job that
        cannot report what it did cannot attest a commit, and a checkpoint may
        not advance without that attestation.
        """
        ...
