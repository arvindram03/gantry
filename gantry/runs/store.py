# SPDX-License-Identifier: Apache-2.0
"""Where runs are kept, and the process-default store.

The store is deliberately boring. What matters is the invariant above it: once
a run id has been handed out, the run stays retrievable independently of the
process that started it, which is why `create` happens before anything external
does and why a failure to create is a reason not to submit.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from gantry.runs.model import Run
from gantry.runs.sqlite import SQLiteRunStore
from gantry.runs.status import RunStatus


@runtime_checkable
class RunStore(Protocol):
    """Persistence for runs, keyed by Gantry run id."""

    def create(self, run: Run) -> None: ...

    def update(self, run: Run) -> None: ...

    def get(self, run_id: str) -> Run | None: ...

    def compare_and_set(self, run_id: str, expected: RunStatus, updated: Run) -> bool:
        """Move a run on only if it is still where the caller last saw it.

        The one operation `update` cannot express. Two hosts may confirm the
        same run at the same moment, and exactly one of them may be the reason
        work gets submitted — so the check and the write have to be one step.
        Returns whether this caller was the one that made the transition.
        """
        ...


class MemoryRunStore:
    """Process-local storage.

    The default, because writing a file into someone's working directory
    because they imported a library is not a default worth having. It satisfies
    every part of the contract except the one that matters most — surviving the
    process — so anything that needs durability configures SQLite.
    """

    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}
        self._lock = threading.Lock()

    def create(self, run: Run) -> None:
        self._runs[run.id] = run

    def update(self, run: Run) -> None:
        self._runs[run.id] = run

    def get(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    def compare_and_set(self, run_id: str, expected: RunStatus, updated: Run) -> bool:
        with self._lock:
            current = self._runs.get(run_id)
            if current is None or current.status is not expected:
                return False
            self._runs[run_id] = updated
            return True

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


def compare_and_set(run_id: str, expected: RunStatus, updated: Run) -> bool:
    """Transition a run if it is still in `expected`, atomically.

    Used by confirmation, where only one caller may be the one that lets work
    start. A store that predates this method cannot make the guarantee, so the
    failure is explicit rather than a silently weaker transition.
    """
    transition = getattr(_default, "compare_and_set", None)
    if transition is None:
        raise RunPersistenceError(
            f"{type(_default).__name__} cannot transition a run atomically; "
            "confirmation needs a store that implements compare_and_set"
        )
    try:
        return bool(transition(run_id, expected, updated))
    except Exception as error:
        raise RunPersistenceError(f"could not transition run {run_id}: {error}") from error


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
