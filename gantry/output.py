# SPDX-License-Identifier: Apache-2.0
"""References to engine-produced outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum


class OutputKind(StrEnum):
    """Where an execution's output lives, which decides how to read it.

    `INLINE` means the rows came back with the result; everything else is a
    reference to somewhere the engine wrote.
    """

    INLINE = "INLINE"
    TABLE = "TABLE"
    DATASET = "DATASET"
    FILE = "FILE"
    OBJECT = "OBJECT"
    STREAM = "STREAM"
    CUSTOM = "CUSTOM"


@dataclass(frozen=True, slots=True)
class OutputRef:
    """A reference to something an execution produced.

    The `uri` is interpreted per `kind` and must be non-empty. Gantry returns
    references rather than data so that a large result does not have to pass
    through the process governing it.
    """

    kind: OutputKind
    uri: str
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.uri.strip():
            raise ValueError("output reference URI must not be empty")
