# SPDX-License-Identifier: Apache-2.0
"""The bound on how many rows a write may change, as adapters apply it.

A read is bounded by not fetching the rows past the limit. A write cannot be
bounded that way — by the time anyone can count what it changed, it has
changed it. So the bound is applied the only way that leaves nothing behind:
the statement runs inside a transaction, the engine says how many rows it
touched, and a count over the limit rolls the transaction back.

That makes it a real ceiling rather than a report. A statement that would have
changed too much changes nothing, and says how much it would have changed.

What is counted is what the engine counts for the statement itself. Rows
changed on its behalf — a cascading delete, a trigger — are the database's
doing and are not in the number.
"""

from __future__ import annotations

import re

from gantry.failure import Failure, FailureKind
from gantry.sql.classification import SQLOperation

#: The statements this bound applies to. A `CREATE TABLE AS` is a write too,
#: but it is a materialization, bounded and verified by its own path.
ROW_WRITES = frozenset(
    {SQLOperation.INSERT, SQLOperation.UPDATE, SQLOperation.DELETE, SQLOperation.MERGE}
)


class AffectedRowLimitError(Exception):
    """A write changed more rows than its limit, and is being rolled back.

    Raised inside the adapter's transaction so that leaving the block undoes
    the statement. `affected` is exact when the engine reported a count and
    `None` when all that is known is that the limit was passed — a `RETURNING`
    read stops one row past it rather than fetching the rest to count them.
    """

    def __init__(self, limit: int, affected: int | None = None) -> None:
        self.limit = limit
        self.affected = affected
        changed = "more than the limit" if affected is None else f"{affected} rows"
        super().__init__(
            f"the statement would have changed {changed}; at most {limit} "
            f"{'row is' if limit == 1 else 'rows are'} allowed, so it was rolled back "
            "and nothing was changed"
        )

    def failure(self) -> Failure:
        # Not retryable: the same statement changes the same rows.
        return Failure(FailureKind.ROW_LIMIT_EXCEEDED, False, str(self))


def check(limit: int | None, affected: int) -> None:
    """Raise if `affected` is past `limit`. No limit means nothing to check."""
    if limit is not None and affected > limit:
        raise AffectedRowLimitError(limit, affected)


_COUNT = re.compile(r"(\d+)\s*\Z")


def from_command_tag(tag: str) -> int:
    """The row count at the end of a PostgreSQL command tag.

    `UPDATE 5`, `DELETE 3`, `MERGE 4` and `INSERT 0 5` — the count is the last
    number in each. A tag with no number is a statement that changed no rows a
    tag can count, which is zero rather than unknown.
    """
    match = _COUNT.search(tag or "")
    return int(match.group(1)) if match else 0
