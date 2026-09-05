# SPDX-License-Identifier: Apache-2.0
"""The shared base for Movement and Analysis specs.

Both are Operations over Datasets, and both traverse one lifecycle:

    Plan -> Generate -> Validate -> Execute -> Verify -> Result

Only what is genuinely common lives here - document envelope, verification
requirements and resource limits. Partitioning and CDC belong to Movement;
joins and windows belong to Analysis. Forcing those into a shared base would
buy nothing and make both harder to read.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from gantry.core.names import ResourceName
from gantry.core.verification import VerificationRequirement
from gantry.spec.apiversion import CANONICAL_API_VERSION, normalize_api_version
from gantry.spec.verification import normalize_requirements


class OperationKind(StrEnum):
    """Operation types that run on the lifecycle engine."""

    MOVEMENT = "Movement"
    ANALYSIS = "Analysis"


class MetadataBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName


class LimitsBlock(BaseModel):
    """Resource bounds enforced regardless of operation type.

    Movement adds throughput rate limits; Analysis adds scan and cost bounds.
    Concurrency, retries and timeout mean the same thing for both.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    max_concurrency: int | None = Field(default=None, alias="maxConcurrency", ge=1)
    max_retries: int | None = Field(default=None, alias="maxRetries", ge=0)


class OperationSpec(BaseModel):
    """Fields shared by every Operation spec."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    api_version: str = Field(alias="apiVersion")
    metadata: MetadataBlock
    verify: tuple[VerificationRequirement, ...] = ()

    @field_validator("verify", mode="before")
    @classmethod
    def _normalize_verify(cls, value: object) -> object:
        return normalize_requirements(value)

    @model_validator(mode="after")
    def _check_api_version(self) -> OperationSpec:
        if normalize_api_version(self.api_version) != CANONICAL_API_VERSION:
            raise ValueError(
                f"apiVersion must be {CANONICAL_API_VERSION!r}, got {self.api_version!r}"
            )
        return self

    @property
    def name(self) -> str:
        return self.metadata.name
