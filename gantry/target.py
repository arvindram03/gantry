# SPDX-License-Identifier: Apache-2.0
"""Opaque execution targets owned by adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ExecutionTarget:
    """The engine-specific environment in which an artifact should run."""

    kind: str
    config: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("execution target kind must not be empty")
