# SPDX-License-Identifier: Apache-2.0
"""Where runs are kept, and the process-default store.

The store is deliberately boring. What matters is the invariant above it: once
a run id has been handed out, the run stays retrievable independently of the
process that started it, which is why `create` happens before anything external
does and why a failure to create is a reason not to submit.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from gantry.runs.model import Run
from gantry.runs.sqlite import SQLiteRunStore


@runtime_checkable
class RunStore(Protocol):
    """Persistence for runs, keyed by Gantry run id."""

    def create(self, run: Run) -> None: ...

    def update(self, run: Run) -> None: ...

    def get(self, run_id: str) -> Run | None: ...


class MemoryRunStore:
    """Process-local storage.

    The default, because writing a file into someone's working directory
    because they imported a library is not a default worth having. It satisfies
    every part of the contract except the one that matters most — surviving the
    process — so anything that needs durability configures SQLite.
    """

    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}

    def create(self, run: Run) -> None:
        self._runs[run.id] = run

    def update(self, run: Run) -> None:
        self._runs[run.id] = run

    def get(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    def recent(self, *, limit: int = 50, status: str | None = None) -> Sequence[Run]:
        runs = sorted(self._runs.values(), key=lambda run: run.created_at, reverse=True)
        if status is not None:
            runs = [run for run in runs if run.status.value == status]
        return tuple(runs[:limit])


class RunPersistenceError(RuntimeError):
    """The control plane could not record a run.

    Raised where the spec says to fail closed: if the initial record cannot be
    written, no external work is submitted. An engine job that exists without a
    record of why it was allowed to is the one outcome this design cannot
    tolerate.
    """


_default: RunStore = MemoryRunStore()


def configure(store: RunStore) -> None:
    """Replace the process-default store."""
    global _default
    _default = store


def store() -> RunStore:
    return _default


def create(run: Run) -> Run:
    """Record a new run before anything external happens.

    A failure here is fatal to the operation by design, so it is raised rather
    than logged.
    """
    try:
        _default.create(run)
    except Exception as error:
        raise RunPersistenceError(f"could not record run {run.id}: {error}") from error
    return run


def update(run: Run) -> Run:
    """Move a run on.

    Unlike `create`, a failure here cannot un-submit work that is already
    running, so it surfaces as an error on the run rather than stopping
    anything — with the native execution id preserved, since that is the only
    handle anyone has on the work that is now unrecorded.
    """
    try:
        _default.update(run)
    except Exception as error:
        raise RunPersistenceError(
            f"could not update run {run.id} "
            f"(native execution {_native(run)} may still be running): {error}"
        ) from error
    return run


def get(run_id: str) -> Run | None:
    """The run with this id, or `None`."""
    return _default.get(run_id)


def recent(*, limit: int = 50, status: str | None = None) -> Sequence[Run]:
    """Recent runs, newest first. Not required by v0; useful in a terminal."""
    lister = getattr(_default, "recent", None)
    return () if lister is None else lister(limit=limit, status=status)


def _native(run: Run) -> str | None:
    return None if run.execution is None else run.execution.native_id


__all__ = [
    "MemoryRunStore",
    "RunPersistenceError",
    "RunStore",
    "SQLiteRunStore",
    "configure",
    "create",
    "get",
    "recent",
    "store",
    "update",
]
