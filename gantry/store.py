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
    handle: ExecutionHandle
    artifact: Artifact
    target: ExecutionTarget
    context: Context
    policy: PolicyRequirements
    admission: AdmissionDecision


class ExecutionStore(Protocol):
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
