# SPDX-License-Identifier: Apache-2.0
"""References to engine-produced outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum


class OutputKind(StrEnum):
    INLINE = "INLINE"
    TABLE = "TABLE"
    DATASET = "DATASET"
    FILE = "FILE"
    OBJECT = "OBJECT"
    STREAM = "STREAM"
    CUSTOM = "CUSTOM"


@dataclass(frozen=True, slots=True)
class OutputRef:
    kind: OutputKind
    uri: str
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.uri.strip():
            raise ValueError("output reference URI must not be empty")
