# SPDX-License-Identifier: Apache-2.0
"""Pluggable persistence for durable execution identity and reconnection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from gantry.admission import AdmissionDecision
from gantry.artifact import Artifact
from gantry.context import Context
from gantry.handle import ExecutionHandle
from gantry.policy import PolicyRequirements
from gantry.target import ExecutionTarget


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Everything needed to resume governing a run in another process.

    Persisted at submission. It keeps the artifact, policy and admission
    decision beside the handle, because reconnecting to a job is not enough:
    deciding whether to accept its result requires knowing what was promised
    when it was admitted.
    """

    handle: ExecutionHandle
    artifact: Artifact
    target: ExecutionTarget
    context: Context
    policy: PolicyRequirements
    admission: AdmissionDecision


class ExecutionStore(Protocol):
    """Persistence for `RunRecord`s, keyed by `gantry_id`.

    Implement this to let handles outlive the process that created them. The
    default is `MemoryExecutionStore`, which does not.
    """

    async def put(self, record: RunRecord) -> None: ...

    async def get(self, gantry_id: str) -> RunRecord | None: ...


class MemoryExecutionStore:
    """Process-local store for development and tests."""

    def __init__(self) -> None:
        self._records: dict[str, RunRecord] = {}

    async def put(self, record: RunRecord) -> None:
        self._records[record.handle.gantry_id] = record

    async def get(self, gantry_id: str) -> RunRecord | None:
        return self._records.get(gantry_id)
