# SPDX-License-Identifier: Apache-2.0
"""Engine-native Flink SQL artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import cast

from gantry.artifact import Artifact


class FlinkMode(StrEnum):
    STREAMING = "streaming"
    BATCH = "batch"


@dataclass(frozen=True, slots=True)
class FlinkSQLArtifact:
    """A Flink SQL statement plus declarations used by the control plane."""

    sql: str
    mode: FlinkMode | str = FlinkMode.STREAMING
    declared_inputs: Sequence[str] = ()
    declared_outputs: Sequence[str] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.sql.strip():
            raise ValueError("Flink SQL must not be empty")
        try:
            mode = FlinkMode(self.mode)
        except (TypeError, ValueError) as error:
            raise ValueError("Flink mode must be 'streaming' or 'batch'") from error
        inputs = _names(self.declared_inputs, "input")
        outputs = _names(self.declared_outputs, "output")
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "declared_inputs", inputs)
        object.__setattr__(self, "declared_outputs", outputs)

    def to_artifact(self) -> Artifact:
        return Artifact(
            payload=self.sql,
            kind="flink_sql",
            metadata={**self.metadata, "flink_mode": cast(FlinkMode, self.mode).value},
            declared_inputs=tuple(self.declared_inputs),
            declared_outputs=tuple(self.declared_outputs),
        )


def _names(values: Sequence[str], kind: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise TypeError(f"declared {kind}s must be a collection of names")
    normalized = tuple(values)
    if any(not isinstance(value, str) or not value.strip() for value in normalized):
        raise ValueError(f"declared {kind}s must contain non-empty strings")
    return normalized
