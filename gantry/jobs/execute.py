# SPDX-License-Identifier: Apache-2.0
"""Running a job to completion.

The runner protocol is deliberately non-blocking: `poll` never waits. Something
still has to wait, and doing it here keeps that decision in one place rather
than in every caller that needs a finished job.
"""

from __future__ import annotations

import asyncio

from gantry.jobs.model import Job
from gantry.jobs.runner import FailureKind, JobHandle, JobState, Runner, RunnerError


class JobFailedError(RunnerError):
    """A job reached a terminal failed state."""

    def __init__(self, handle: JobHandle, detail: str | None) -> None:
        super().__init__(f"job {handle.id} failed: {detail or 'no detail reported'}")
        self.handle = handle
        self.detail = detail


async def run_to_completion(
    # A timeout parameter rather than a caller-supplied cancel scope: exceeding
    # it must raise RunnerError, because a cancelled poll and a job that never
    # finished are different things to everything upstream.
    runner: Runner,
    job: Job,
    *,
    timeout: float = 3600.0,  # noqa: ASYNC109 - see the note above
    poll_interval: float = 0.5,
) -> str:
    """Submit a job, wait for it, and return everything it wrote.

    The container is left behind on success, as a record of what ran. It is not
    a cache: a resubmission replaces a finished container rather than adopting
    it, because whether the work still needs doing is the engine's judgement,
    held in its checkpoints. A replay therefore redoes the partition, which the
    merge makes harmless. Reaping is not handled here.
    """
    handle = await runner.submit(job)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while loop.time() < deadline:
        status = await runner.poll(handle)
        if status.state.terminal:
            if status.state is JobState.FAILED:
                if status.failure is FailureKind.RUNNER:
                    # The work was not attempted or was interrupted. Raising the
                    # runner's own error keeps it retryable, rather than
                    # quarantining a node for an infrastructure fault.
                    raise RunnerError(
                        f"job {handle.id} was interrupted by the runner "
                        f"(exit {status.exit_code}): {status.detail or 'no detail reported'}"
                    )
                raise JobFailedError(handle, status.detail)
            return await runner.logs(handle)
        await asyncio.sleep(poll_interval)

    raise RunnerError(f"job {handle.id} did not finish within {timeout:.0f}s")
