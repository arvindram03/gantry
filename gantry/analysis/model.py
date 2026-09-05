# SPDX-License-Identifier: Apache-2.0
"""The Analysis domain model.

The sibling of `Movement`, and the same seam: the spec layer owns YAML shape
and converts into this, so the compiler and planner never bind to spec field
names.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ContentHash, FieldName, ResourceName
from gantry.core.timewindow import TimeWindow
from gantry.core.verification import VerificationRequirement


class ExecutionEngine(StrEnum):
    AUTO = "auto"
    POSTGRES = "postgres"
    DUCKDB = "duckdb"


class TemporalJoinStrategy(StrEnum):
    NEAREST_PRECEDING = "nearest_preceding"
    NEAREST_FOLLOWING = "nearest_following"
    NEAREST = "nearest"


class FieldNormalization(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    canonical: FieldName
    aliases: tuple[FieldName, ...] = Field(min_length=1)


class TemporalJoin(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    strategy: TemporalJoinStrategy
    max_distance_seconds: int | None = None


class Join(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    left: ResourceName
    right: ResourceName
    on: tuple[FieldName, ...] = Field(min_length=1)
    temporal: TemporalJoin | None = None


class AnalysisLimits(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    max_concurrency: int | None = None
    max_retries: int | None = None
    max_bytes_scanned: int | None = None
    timeout_seconds: int | None = None


class Analysis(BaseModel):
    """A reproducible computation over one or more Datasets."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    objective: str | None = None
    inputs: tuple[ResourceName, ...] = Field(min_length=1)
    window: TimeWindow | None = None
    normalize: tuple[FieldNormalization, ...] = ()
    joins: tuple[Join, ...] = ()
    signals: tuple[str, ...] = ()
    engine: ExecutionEngine = ExecutionEngine.AUTO
    verification: tuple[VerificationRequirement, ...] = ()
    limits: AnalysisLimits = AnalysisLimits()

    @model_validator(mode="after")
    def _check_inputs(self) -> Analysis:
        if len(set(self.inputs)) != len(self.inputs):
            raise ValueError(f"duplicate inputs: {sorted(self.inputs)}")
        return self

    def guarantee_fingerprint(self) -> ContentHash:
        """Hash of what may not change without a replan.

        Inputs, join semantics, the window and the verification requirements
        define what the Result means. Engine choice and resource limits do not,
        so they are tunable within a plan version.
        """
        payload = {
            "inputs": list(self.inputs),
            "window": (
                None
                if self.window is None
                else {
                    "start": _iso(self.window.start),
                    "end": _iso(self.window.end),
                }
            ),
            "normalize": [item.model_dump(mode="json") for item in self.normalize],
            "joins": [join.model_dump(mode="json") for join in self.joins],
            "signals": list(self.signals),
            "verification": [check.model_dump(mode="json") for check in self.verification],
        }
        rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return f"sha256:{hashlib.sha256(rendered.encode('utf-8')).hexdigest()}"


def _iso(moment: datetime) -> str:
    return moment.isoformat()
