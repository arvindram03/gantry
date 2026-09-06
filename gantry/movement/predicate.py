# SPDX-License-Identifier: Apache-2.0
"""Turning a plan's partition bounds into SQL, safely.

Shared rather than duplicated per job kind on purpose. A partition bound is
customer data on its way into generated SQL, so every job kind that writes a
predicate is writing the same security-critical code — and a second copy is how
one of them ends up subtly different from the one that was tested.
"""

from __future__ import annotations

import re

from gantry.core.dataset import DatasetManifest
from gantry.movement.partitioning import Partition
from gantry.verification.sql import column_type, quote

# Control characters have no business in a key bound and would let a value break
# out of the line it is written on. Rejected rather than escaped: a bound that
# needs escaping this badly is a bug upstream.
_FORBIDDEN = re.compile(r"[\x00-\x1f\x7f]")


def sql_literal(value: str) -> str:
    """A single-quoted SQL literal.

    Doubling the quote is sufficient because PostgreSQL has had
    `standard_conforming_strings` on by default since 9.1, so a backslash is
    just a backslash. Control characters are refused outright.
    """
    if _FORBIDDEN.search(value):
        raise ValueError(f"control characters in a key bound: {value!r}")
    return "'" + value.replace("'", "''") + "'"


def bounds(manifest: DatasetManifest, partition: Partition) -> str:
    """The partition predicate, with bounds re-typed from the schema.

    Bounds travel as text so the runtime need not know whether a key is a
    bigint or a uuid; the cast puts the type back on at the point of use.
    """
    column = quote(partition.column)
    declared = column_type(manifest, partition.column)
    clauses: list[str] = []
    if partition.lo is not None:
        clauses.append(f"{column} >= CAST({sql_literal(partition.lo)} AS {declared})")
    if partition.hi is not None:
        clauses.append(f"{column} < CAST({sql_literal(partition.hi)} AS {declared})")
    return " AND ".join(clauses) if clauses else "TRUE"
