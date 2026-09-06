# SPDX-License-Identifier: Apache-2.0
"""Moving a group into Iceberg, safely on a replay.

The PostgreSQL path is idempotent because the write itself is: an upsert applied
twice is the upsert applied once. **Iceberg's write is an append**, so the same
trick is not available — running a Beam job twice against an Iceberg table adds
the rows twice. Measured, not assumed: two runs of a 200-row job leave 400 rows.

That matters here more than it would elsewhere, because Gantry *will* replay. A
worker that dies between committing and checkpointing is the case the whole
runtime is built around, and a target that duplicates on replay turns the
recovery path into the corruption path.

So idempotence is arranged rather than inherited: **verify first, and move only
if the target does not already hold what it should.** That is sound only because
of two facts about the sink, both measured (`docs/benchmarks.md`):

1. **One job commits one atomic Iceberg snapshot**, even at 300,000 rows. It
   does not dribble out a snapshot per bundle.
2. **A job killed mid-flight leaves nothing at all** — no snapshot, no orphan
   data files, not even the table.

Together those mean the target is only ever in one of two states: without the
group, or with exactly the group. There is no partial state for a verification
to be confused by, so "does the target already hold this?" is a question with a
trustworthy answer.

If either fact stops being true — a sink that commits per bundle, a runner that
leaves half a snapshot — this strategy is unsound and the guarantee goes with
it. That is why they are named here rather than left as background knowledge.
"""

from __future__ import annotations

from dataclasses import dataclass

from gantry.jobs.execute import JobFailedError, run_to_completion
from gantry.jobs.model import Job
from gantry.jobs.runner import Runner
from gantry.verification.checksum import ChunkChecksum
from gantry.verification.iceberg import parse_checksum


class GroupNotReconciledError(Exception):
    """The target still disagrees after the move ran.

    Raised rather than returned: no `CommitResult` means no checkpoint, so the
    group will be attempted again rather than recorded as done.
    """


@dataclass(frozen=True)
class GroupOutcome:
    """What happened, and what the target holds now."""

    checksum: ChunkChecksum
    #: False when the target already held the group and no job was run.
    moved: bool


async def ensure_group(
    runner: Runner,
    *,
    verify: Job,
    move: Job,
    expected: ChunkChecksum,
    timeout: float = 3600.0,  # noqa: ASYNC109 - passed through to the runner
) -> GroupOutcome:
    """Make the target hold `expected` for this group, exactly once.

    The verification runs first. When the target already agrees, the move is
    skipped entirely — which is what makes a replay a no-op against a sink that
    would otherwise append a second copy.
    """
    before = await _checksum(runner, verify, timeout=timeout)
    if before == expected:
        return GroupOutcome(checksum=before, moved=False)

    await run_to_completion(runner, move, timeout=timeout)

    after = await _checksum(runner, verify, timeout=timeout)
    if after != expected:
        raise GroupNotReconciledError(
            f"after moving, the target reports {after.describe()} "
            f"but the source reports {expected.describe()}"
        )
    return GroupOutcome(checksum=after, moved=True)


async def _checksum(
    runner: Runner,
    verify: Job,
    *,
    timeout: float,  # noqa: ASYNC109
) -> ChunkChecksum:
    """Run the verification job and read its one line.

    A target that does not exist yet is not an error: it reports nothing, which
    is exactly the checksum of nothing, and the move then creates it.
    """
    try:
        output = await run_to_completion(runner, verify, timeout=timeout)
    except JobFailedError:
        # The verification job exits non-zero when there is no Iceberg metadata
        # to read, which is the ordinary state before the first move.
        return ChunkChecksum(checksum="0", rows=0)
    return parse_checksum(output)
