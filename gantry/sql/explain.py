# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from gantry.sql.classification import SQLObjectRef


@dataclass(frozen=True, slots=True)
class ExplainResult:
    """What the engine's planner estimated, before anything ran.

    `supported` is false when the engine cannot explain the statement, which is
    distinct from an estimate of zero: a byte-scanned bound cannot be enforced
    on an estimate that does not exist. Every estimate is optional for the same
    reason.
    """

    supported: bool
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    estimated_cost: float | None = None
    referenced_objects: tuple[SQLObjectRef, ...] = ()
    summary: Mapping[str, object] = field(default_factory=dict)
    native: Mapping[str, object] = field(default_factory=dict)
