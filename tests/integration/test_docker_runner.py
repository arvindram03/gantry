# SPDX-License-Identifier: Apache-2.0
"""The Docker runner, against a real daemon.

The protocol's one hard promise is that **submitting twice does not run twice**.
A worker that dies between starting a job and recording that it did must not
cause a second copy of the work — and that is not something a mock can tell you,
because the whole question is what the daemon does with a name it has seen.

Requires Docker, and the `postgres:16-alpine` image the dev stack already uses.
"""

from __future__ import annotations

import shutil
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from gantry.jobs import (
    ContainerPackaging,
    Job,
    JobKind,
    JobState,
    Packaging,
    UnsupportedPackagingError,
)
from gantry.jobs.execute import run_to_completion
from gantry.jobs.runner import RunnerError
from gantry.jobs.runners import DockerRunner

pytestmark = pytest.mark.integration

IMAGE = "postgres:16-alpine"
AT = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def job(body: str, *, unit: str = "probe/00000", **overrides: object) -> Job:
    base: dict[str, object] = {
        "operation": "runner-probe",
        "kind": JobKind.SQL,
        "unit": unit,
        "body": body,
        "packaging": ContainerPackaging(image=IMAGE),
        "generated_at": AT,
    }
    base.update(overrides)
    return Job.model_validate(base)


async def _sweep(runner: DockerRunner) -> None:
    """Remove every container this suite could have created.

    By name prefix rather than by tracking submissions: the names are
    deterministic, so this also catches containers left behind by a test that
    failed before it could clean up — which is exactly when leftovers matter,
    because the next run would adopt them.
    """
    _, out, _ = await runner._run(["docker", "ps", "-aq", "--filter", "name=gantry-job-"])
    for container in out.split():
        await runner._run(["docker", "rm", "-f", container])


@pytest.fixture
async def runner() -> AsyncIterator[DockerRunner]:
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH")
    started = DockerRunner()
    await _sweep(started)
    try:
        yield started
    finally:
        await _sweep(started)


async def wait_for_terminal(runner: DockerRunner, handle, limit: float = 30.0):  # type: ignore[no-untyped-def]
    import asyncio

    deadline = asyncio.get_running_loop().time() + limit
    while asyncio.get_running_loop().time() < deadline:
        status = await runner.poll(handle)
        if status.state.terminal:
            return status
        await asyncio.sleep(0.1)
    raise AssertionError(f"{handle.describe()} never reached a terminal state")


async def test_a_job_that_succeeds_reports_exit_zero(runner: DockerRunner) -> None:
    handle = await runner.submit(job("exit 0"))
    status = await wait_for_terminal(runner, handle)

    assert status.state is JobState.SUCCEEDED
    assert status.exit_code == 0


async def test_a_job_that_fails_reports_its_exit_code_and_its_output(
    runner: DockerRunner,
) -> None:
    """A failed job's output is the only place the reason survives."""
    handle = await runner.submit(job("echo 'the target refused'; exit 3", unit="probe/00001"))
    status = await wait_for_terminal(runner, handle)

    assert status.state is JobState.FAILED
    assert status.exit_code == 3
    assert "the target refused" in (status.detail or "")


async def test_submitting_the_same_job_twice_adopts_rather_than_duplicates(
    runner: DockerRunner,
) -> None:
    """The protocol's one hard promise, and the reason it is tested against a
    real daemon: the question is what Docker does with a name it has seen."""
    work = job("sleep 5", unit="probe/00002")

    first = await runner.submit(work)
    second = await runner.submit(work)

    assert first == second, "a resubmitted job must adopt, not start a second"

    code, out, _ = await runner._run(
        ["docker", "ps", "-a", "--filter", f"name={first.id}", "--format", "{{.ID}}"]
    )
    assert code == 0
    assert len(out.split()) == 1, "exactly one container should exist for one job"


async def test_a_job_whose_packaging_changed_is_a_different_run(
    runner: DockerRunner,
) -> None:
    """Identity covers the packaging, so an image changing underneath a replay
    is a different job rather than the same one behaving differently."""
    body = "exit 0"
    original = job(body, unit="probe/00003")
    repackaged = job(
        body,
        unit="probe/00003",
        packaging=ContainerPackaging(image=IMAGE, environment={"PGAPPNAME": "changed"}),
    )

    assert original.content_hash != repackaged.content_hash
    assert (await runner.submit(original)).id != (await runner.submit(repackaged)).id


async def test_a_secret_is_resolved_by_the_runner_and_never_in_the_job(
    runner: DockerRunner,
) -> None:
    """The job body is retained as provenance. The value must reach the
    process, and must not reach the artifact."""
    value = "s3cr3t-probe-value"
    holder = DockerRunner(secrets={"PROBE_SECRET": value})
    # The job checks the variable arrived without naming what it holds. Writing
    # the expected value into the script would put the secret in the artifact,
    # which is the thing being tested against — the first draft of this test
    # did exactly that.
    work = job(
        'test -n "$PROBE_SECRET"',
        unit="probe/00004",
        packaging=ContainerPackaging(image=IMAGE, secrets=("PROBE_SECRET",)),
    )
    assert value not in work.model_dump_json(), "the value must not reach the artifact"
    assert "PROBE_SECRET" in work.model_dump_json(), "but its name must, for identity"

    handle = await holder.submit(work)
    try:
        status = await wait_for_terminal(holder, handle)
        assert status.state is JobState.SUCCEEDED, "the secret did not reach the process"
    finally:
        await _sweep(holder)


async def test_a_missing_secret_fails_before_anything_runs(runner: DockerRunner) -> None:
    work = job(
        "exit 0",
        unit="probe/00005",
        packaging=ContainerPackaging(image=IMAGE, secrets=("ABSENT",)),
    )
    with pytest.raises(RunnerError, match="ABSENT"):
        await DockerRunner().submit(work)


def test_a_runner_refuses_packaging_it_cannot_run() -> None:
    """Refused at submit, not discovered at launch: a job that fails two
    minutes into a start is indistinguishable from one that failed for a real
    reason."""

    class Elsewhere(DockerRunner):
        def supports(self, packaging: Packaging) -> bool:
            return False

    import asyncio

    with pytest.raises(UnsupportedPackagingError, match="container"):
        asyncio.run(Elsewhere().submit(job("exit 0")))


async def test_a_finished_container_is_replaced_rather_than_adopted(
    runner: DockerRunner,
) -> None:
    """A finished attempt is not a cached answer.

    Container names are content-derived, so the same job submitted after an
    earlier attempt finished would otherwise adopt that attempt's corpse and
    report its output as this run's. A caller reading a commit attestation out
    of those logs would advance a checkpoint over work that never happened this
    time round. Whether the work still needs doing is the engine's judgement,
    not the runner's.
    """
    work = job("echo attempt", unit="probe/00009")

    first = await runner.submit(work)
    # Adopts the container just started and waits for it to finish.
    await run_to_completion(runner, work, poll_interval=0.05)
    started_at = await runner._run(
        ["docker", "inspect", first.id, "--format", "{{.State.StartedAt}}"]
    )

    second = await runner.submit(work)
    assert second == first, "the name is still the job's name"

    restarted_at = await runner._run(
        ["docker", "inspect", second.id, "--format", "{{.State.StartedAt}}"]
    )
    assert restarted_at[1] != started_at[1], (
        "the finished container was adopted instead of being replaced"
    )
