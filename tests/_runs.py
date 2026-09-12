# SPDX-License-Identifier: Apache-2.0
"""A Run factory for tests that need one without performing an operation.

Stubs standing in for a governed connection have to hand something back, and
store tests need a record to write. Both previously built a `Run` by hand,
which meant two places to update whenever the model gained a field. This is
the one place.
"""

from __future__ import annotations

from typing import Any

from gantry.actor import UNKNOWN_ACTOR
from gantry.runs.model import OperationKind, OperationRef, Run
from gantry.runs.status import RunStatus


def make_run(
    *,
    id: str = "run_test",  # noqa: A002
    status: RunStatus = RunStatus.ACCEPTED,
    kind: OperationKind = OperationKind.QUERY,
    engine: str = "sql",
    provider: str | None = None,
    **overrides: Any,
) -> Run:
    """A minimal valid run. Pass `**overrides` for whatever the test is about."""
    return Run(
        id=id,
        status=status,
        actor=overrides.pop("actor", UNKNOWN_ACTOR),
        operation=overrides.pop(
            "operation", OperationRef(kind=kind, engine=engine, provider=provider)
        ),
        **overrides,
    )


__all__ = ["make_run"]
