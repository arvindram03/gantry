# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SQLTarget:
    provider: str
    dialect: str
    driver: str
    config: Mapping[str, object]
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("provider", self.provider),
            ("dialect", self.dialect),
            ("driver", self.driver),
        ):
            if not value.strip():
                raise ValueError(f"SQL target {name} must not be empty")
