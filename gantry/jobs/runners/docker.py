# SPDX-License-Identifier: Apache-2.0
"""Running jobs as local containers.

The development runner, and the reference implementation of the protocol. It
drives the `docker` CLI rather than a client library: the core stays small
(§17), and the CLI is the interface every container platform documents against.

**Idempotent submission is the load-bearing part.** A worker that dies between
starting a container and recording that it did must not cause a second copy of
the work to run. The container is named deterministically from the job's content
hash, so submitting the same job twice adopts the first rather than starting a
second — and because the hash covers the packaging, a job whose image changed is
a different name and therefore genuinely a different run.

This module and `gantry.jobs.packaging.container` are the only two that may name
an image, a registry or a container. A test enforces that.
"""

from __future__ import annotations

import asyncio
import shutil
from datetime import UTC, datetime

from gantry.jobs.model import Job
from gantry.jobs.packaging import Packaging, PackagingKind
from gantry.jobs.runner import (
    FailureKind,
    JobHandle,
    JobState,
    JobStatus,
    RunnerError,
    UnsupportedPackagingError,
)

# Docker names allow [a-zA-Z0-9][a-zA-Z0-9_.-]*, and a content hash carries a
# colon. Truncating is safe here because the name only has to be unique among
# in-flight jobs, and the full hash travels with the job itself.
_NAME_PREFIX = "gantry-job-"
_HASH_CHARS = 24


class DockerRunner:
    """Runs container-packaged jobs on the local Docker daemon."""

    def __init__(
        self,
        *,
        name: str = "docker",
        binary: str = "docker",
        secrets: dict[str, str] | None = None,
    ) -> None:
        self._name = name
        self._binary = binary
        # Resolved here, never in the job. The job body is retained as
        # provenance and is meant to be read.
        self._secrets = dict(secrets or {})

    @property
    def name(self) -> str:
        return self._name

    def supports(self, packaging: Packaging) -> bool:
        return packaging.kind is PackagingKind.CONTAINER

    async def submit(self, job: Job) -> JobHandle:
        if not self.supports(job.packaging):
            raise UnsupportedPackagingError(self._name, job.packaging)

        container = _container_name(job)
        state = await self._state(container)
        if state in _LIVE:
            # Adopted, not restarted. Adoption exists to stop two copies of the
            # same work running at once, so it applies to work still in flight.
            return JobHandle(runner=self._name, id=container)
        if state is not None:
            # A finished container with this name is a corpse from an earlier
            # attempt. Adopting it would return that attempt's output as this
            # one's, which is how a job that must run again reports a commit it
            # never made - and the caller would advance a checkpoint over it.
            # Whether the work still needs doing is the engine's judgement,
            # recorded in its checkpoints; the runner does not second-guess it.
            await self._run([self._binary, "rm", "-f", container])

        packaging = job.packaging
        argv = [self._binary, "run", "--detach", "--name", container]
        for key, value in sorted(packaging.environment.items()):
            argv += ["--env", f"{key}={value}"]
        for secret in packaging.secrets:
            resolved = self._secrets.get(secret)
            if resolved is None:
                raise RunnerError(
                    f"job {job.content_hash} needs secret {secret!r}, which this "
                    f"runner was not given"
                )
            argv += ["--env", f"{secret}={resolved}"]
        for outside, inside in packaging.mounts:
            argv += ["--volume", f"{outside}:{inside}"]
        if packaging.network:
            argv += ["--network", packaging.network]
        argv.append(packaging.reference)
        argv += list(packaging.command) if packaging.command else [*packaging.interpreter, job.body]

        code, out, err = await self._run(argv)
        if code != 0:
            # A container that could not start is a runner failure, not a job
            # failure: the work has not been attempted, so retrying is safe.
            raise RunnerError(
                f"could not start job {job.content_hash}: {err.strip() or out.strip()}"
            )
        return JobHandle(runner=self._name, id=container)

    async def poll(self, handle: JobHandle) -> JobStatus:
        code, out, err = await self._run(
            [
                self._binary,
                "inspect",
                handle.id,
                "--format",
                "{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}} {{.State.FinishedAt}}",
            ]
        )
        if code != 0:
            raise RunnerError(f"cannot inspect {handle.describe()}: {err.strip() or out.strip()}")

        status, _, rest = out.strip().partition(" ")
        exit_text, _, rest = rest.partition(" ")
        oom_text, _, finished_text = rest.partition(" ")
        exit_code = int(exit_text) if exit_text.lstrip("-").isdigit() else None
        oom_killed = oom_text.strip().lower() == "true"

        if status in ("created", "restarting", "paused"):
            return JobStatus(state=JobState.PENDING, detail=status)
        if status == "running":
            return JobStatus(state=JobState.RUNNING)

        # Exited or dead. The exit code is the commit signal for a SQL job, so
        # it travels even when it is zero.
        return JobStatus(
            state=JobState.SUCCEEDED if exit_code == 0 else JobState.FAILED,
            exit_code=exit_code,
            detail=None if exit_code == 0 else await self._tail(handle.id),
            failure=None if exit_code == 0 else _classify(exit_code, oom_killed=oom_killed),
            finished_at=_parse_time(finished_text),
        )

    async def reap(self, *, keep: int = 50) -> int:
        """Remove finished job containers, newest `keep` retained.

        Containers are left behind on purpose — a finished job is a record of
        what ran, and its logs are the commit attestation. But nothing removed
        them, so they accumulated without bound, which is an operational
        problem masquerading as a design decision.

        Only exited containers are touched, and only ones this runner named. A
        job still in flight is never disturbed, because adoption depends on it
        being there.
        """
        code, out, _ = await self._run(
            [
                self._binary,
                "ps",
                "--all",
                "--quiet",
                "--filter",
                f"name=^{_NAME_PREFIX}",
                "--filter",
                "status=exited",
            ]
        )
        if code != 0:
            return 0
        # `docker ps` lists newest first, so the tail is the oldest.
        stale = out.split()[keep:]
        for container in stale:
            await self._run([self._binary, "rm", "-f", container])
        return len(stale)

    async def logs(self, handle: JobHandle) -> str:
        _, out, err = await self._run([self._binary, "logs", handle.id])
        return (out + err).strip()

    async def remove(self, handle: JobHandle) -> None:
        """Discard a finished container. Never called automatically.

        A failed job's container is the only place its output survives, and
        cleaning up before someone has read it is how a failure becomes
        undiagnosable.
        """
        await self._run([self._binary, "rm", "-f", handle.id])

    async def _tail(self, container: str, lines: int = 20) -> str:
        _, out, err = await self._run([self._binary, "logs", "--tail", str(lines), container])
        return (out + err).strip()[:2000]

    async def _state(self, container: str) -> str | None:
        """Docker's own word for what the container is doing, or None if there
        is no such container."""
        code, out, _ = await self._run(
            [self._binary, "inspect", container, "--format", "{{.State.Status}}"]
        )
        return out.strip() if code == 0 else None

    async def _run(self, argv: list[str]) -> tuple[int, str, str]:
        if shutil.which(argv[0]) is None:
            raise RunnerError(f"{argv[0]!r} is not on PATH; this runner needs it")
        process = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await process.communicate()
        return process.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")


# States in which a container is still work in flight. Anything else - exited,
# dead - is a finished attempt, and finished is not the same as adoptable.
def _classify(exit_code: int | None, *, oom_killed: bool) -> FailureKind:
    """Whether the work failed, or whether it was never allowed to finish.

    The codes are Docker's own, confirmed against the daemon rather than read
    from documentation: 125 is the daemon refusing, 126 and 127 are a command
    that could not be run, and 137 or 143 are a container killed by a signal —
    an OOM kill or an operator, in both cases work interrupted rather than work
    refused. Everything else is the job's own exit status, which is the whole
    commit signal for a SQL job and must not be mistaken for a platform fault.

    A job could of course exit 137 by itself. Calling that retryable is the
    right way to be wrong: a retried infrastructure fault costs one more
    attempt, and a quarantined node costs an operator.
    """
    if oom_killed or exit_code in (125, 126, 127, 137, 143):
        return FailureKind.RUNNER
    return FailureKind.JOB


_LIVE = frozenset({"created", "running", "restarting", "paused"})


def _container_name(job: Job) -> str:
    """Deterministic from the job's identity, which is what makes submit adopt."""
    return _NAME_PREFIX + job.content_hash.removeprefix("sha256:")[:_HASH_CHARS]


def _parse_time(value: str) -> datetime | None:
    text = value.strip()
    if not text or text.startswith("0001-01-01"):
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
