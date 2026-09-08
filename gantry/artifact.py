# SPDX-License-Identifier: Apache-2.0
"""Artifacts proposed to the Gantry runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Artifact:
    """An opaque payload produced by an agent or another upstream system."""

    payload: object
    kind: str
    metadata: Mapping[str, object] = field(default_factory=dict)
    dependencies: tuple[str, ...] = ()
    declared_inputs: tuple[str, ...] = ()
    declared_outputs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("artifact kind must not be empty")
