# SPDX-License-Identifier: Apache-2.0
"""YAML for `kind: Migration`.

    apiVersion: gantry.dev/v1alpha1
    kind: Migration
    metadata:
      name: orders-to-warehouse
    movements:
      - orders-snapshot
    cutover:
      gates:
        allPartitionsVerified: true
        maxCdcLag: 2s
        criticalVerificationFailures: 0
        requireApproval: true
    rollback:
      window: 24h
      sourceRemainsAuthoritative: true

The `cutover:` and `rollback:` blocks are the ones a Movement spec has parsed
since v1 while reporting them as deprecated placement — they belong to this
workflow, and a Movement does not imply a cutover. This is where they land
properly. A Movement spec that still carries them keeps warning; nothing about
that changes, because specs in the field should not break to make a point.

Movements are **named, not embedded**. A Movement is a resource with its own
lifecycle, its own plan versions and its own checkpoints. Inlining one here
would make the Migration own execution, which is exactly the thing the
composition claim says it must not do.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from gantry.core.durations import DurationError, parse_duration
from gantry.core.names import ResourceName
from gantry.migration.model import CutoverGates, Migration, RollbackPolicy
from gantry.spec.operation import OperationSpec


def _duration(value: object) -> object:
    if isinstance(value, str):
        try:
            return parse_duration(value)
        except DurationError as exc:
            raise ValueError(str(exc)) from exc
    return value


class GatesBlock(BaseModel):
    """The `cutover.gates` block.

    Every field defaults to the strict answer. A gate omitted from a spec is
    still enforced — the omission cannot be a way to skip it, or the spec
    becomes a place to quietly disable checks.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    all_partitions_verified: bool = Field(default=True, alias="allPartitionsVerified")
    max_cdc_lag: timedelta = Field(default=timedelta(seconds=2), alias="maxCdcLag")
    critical_verification_failures: int = Field(
        default=0, alias="criticalVerificationFailures", ge=0
    )
    target_healthy: bool = Field(default=True, alias="targetHealthy")
    schema_compatible: bool = Field(default=True, alias="schemaCompatible")
    require_approval: bool = Field(default=True, alias="requireApproval")

    @field_validator("max_cdc_lag", mode="before")
    @classmethod
    def _parse_lag(cls, value: object) -> object:
        return _duration(value)

    def to_gates(self) -> CutoverGates:
        return CutoverGates(
            all_partitions_verified=self.all_partitions_verified,
            max_cdc_lag=self.max_cdc_lag,
            critical_verification_failures=self.critical_verification_failures,
            target_healthy=self.target_healthy,
            schema_compatible=self.schema_compatible,
            require_approval=self.require_approval,
        )


class CutoverBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    gates: GatesBlock = GatesBlock()


class RollbackBlock(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    window: timedelta = Field(default=timedelta(hours=24))
    source_remains_authoritative: bool = Field(default=True, alias="sourceRemainsAuthoritative")

    @field_validator("window", mode="before")
    @classmethod
    def _parse_window(cls, value: object) -> object:
        return _duration(value)

    def to_policy(self) -> RollbackPolicy:
        return RollbackPolicy(
            window=self.window,
            source_remains_authoritative=self.source_remains_authoritative,
        )


class MovementRef(BaseModel):
    """One Movement this Migration drives.

    Accepts either a bare name or `{movement: name, spec: path}`. The spec path
    is what lets the workflow *run* the Movement rather than only name it —
    and it stays a reference rather than an inlined definition, because a
    Movement is a resource with its own lifecycle.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    movement: ResourceName
    spec: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _accept_bare_name(cls, value: object) -> object:
        return {"movement": value} if isinstance(value, str) else value


class MigrationSpec(OperationSpec):
    """A parsed `kind: Migration` document."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["Migration"]
    movements: tuple[MovementRef, ...] = Field(min_length=1)
    cutover: CutoverBlock = CutoverBlock()
    rollback: RollbackBlock = RollbackBlock()
    description: str | None = None

    @model_validator(mode="after")
    def _check_document(self) -> MigrationSpec:
        names = [ref.movement for ref in self.movements]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate movements: {sorted(names)}")
        return self

    def to_migration(self) -> Migration:
        return Migration(
            name=self.metadata.name,
            movements=tuple(ref.movement for ref in self.movements),
            movement_specs={ref.movement: ref.spec for ref in self.movements if ref.spec},
            cutover=self.cutover.gates.to_gates(),
            rollback=self.rollback.to_policy(),
            description=self.description,
        )
