# SPDX-License-Identifier: Apache-2.0
"""How many partitions travel in one job.

Only some job kinds need this. A SQL script starts in about a quarter of a
second, so one job per partition is affordable and the checkpoint unit is the
partition — the strongest granularity Gantry offers. A Beam job costs about
eleven seconds before it reads a row, so one job per partition would spend
minutes of startup on a dataset of any size, and partitions have to travel
together.

That is a real loss and is written down rather than hidden: a group is the
checkpoint unit for the kinds that need one, so a crash costs the group. The
numbers below are measured (`docs/benchmarks.md`), and the sizing rule is stated
in terms of them so that re-measuring changes the groups rather than requiring
this to be rewritten.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from gantry.movement.partitioning import Partition


@dataclass(frozen=True)
class JobCost:
    """What one job of a given kind costs, as measured.

    `fixed_seconds` is what a job costs having read nothing — image start,
    interpreter, expansion service, pipeline construction. `rows_per_second` is
    what it manages once it is moving.
    """

    fixed_seconds: float
    rows_per_second: float

    def rows_for_overhead(self, fraction: float) -> int:
        """How many rows a job must carry for startup to be `fraction` of it.

        At a tenth, an eleven-second startup wants a job lasting about a hundred
        seconds — which is the arithmetic that decides how coarse the checkpoint
        unit becomes, and it is better done here than guessed at.
        """
        if not 0 < fraction < 1:
            raise ValueError("overhead fraction must be between 0 and 1, exclusive")
        # startup / (startup + moving) = fraction  =>  moving = startup * (1 - f) / f
        moving_seconds = self.fixed_seconds * (1 - fraction) / fraction
        return max(1, int(moving_seconds * self.rows_per_second))


# Measured on the Direct runner in a container, Postgres to Postgres, warm
# image. See docs/benchmarks.md — 10.8 s for a job that reads nothing, and
# ~15k rows/sec once moving.
BEAM_COST = JobCost(fixed_seconds=10.8, rows_per_second=15_000.0)

# A SQL job starts in ~0.25 s and moves ~180k rows/sec.
#
# Note what the fraction rule says if applied here: about 405,000 rows a job,
# which would group SQL partitions too. The `sql` kind still does not group, and
# the reason is absolute rather than fractional — sixty-one partitions cost
# fifteen seconds of startup in total, and fifteen seconds is not worth trading
# partition-granular checkpoints for. The same sixty-one under `beam` cost about
# eleven minutes, which is.
#
# So the rule is: group when the *absolute* overhead of not grouping is
# material, and size the group by the fraction. Recorded here because a fraction
# alone would have quietly coarsened the strongest guarantee in the project.
SQL_COST = JobCost(fixed_seconds=0.25, rows_per_second=180_000.0)

# A tenth of a job spent starting it is the point where grouping stops buying
# much and starts costing checkpoint granularity. Chosen, not measured — but
# chosen against measured numbers, and named so it can be argued with.
DEFAULT_OVERHEAD_FRACTION = 0.1


def group_partitions(
    partitions: Sequence[Partition],
    *,
    cost: JobCost = BEAM_COST,
    overhead_fraction: float = DEFAULT_OVERHEAD_FRACTION,
) -> tuple[tuple[Partition, ...], ...]:
    """Partitions, gathered into the units one job should cover.

    Order is preserved: partitions are contiguous ranges of the key, and a group
    of adjacent ones is a range too, which is what makes a group's verification
    a question about a contiguous span rather than a scattered set.

    A partition whose size is unknown counts as a whole group's worth. Guessing
    small would produce a job that reads far more than intended; guessing large
    costs one extra job. The cheaper mistake is the one worth making.
    """
    target = cost.rows_for_overhead(overhead_fraction)
    groups: list[tuple[Partition, ...]] = []
    current: list[Partition] = []
    carried = 0

    for partition in partitions:
        rows = partition.estimated_rows if partition.estimated_rows is not None else target
        current.append(partition)
        carried += rows
        if carried >= target:
            groups.append(tuple(current))
            current = []
            carried = 0

    if current:
        groups.append(tuple(current))
    return tuple(groups)
