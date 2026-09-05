# SPDX-License-Identifier: Apache-2.0
"""The Migration domain model.

A Migration is a workflow composed from Movements. It is not a fourth resource
alongside Dataset, Movement, Analysis and Result — it is the first thing built
*on* them, and the design document is explicit that this is the distinction
that matters (RFC 0 §5.2, §1279).

Practically, that means this module carries almost no machinery. It names the
Movements to run, the gates that must pass before traffic may move, and how
long the source stays authoritative afterwards. Everything about how data
actually moves belongs to Movement and stays there. If this file starts
growing partition logic or checkpointing, the composition claim was wrong and
that is worth noticing rather than working around.
"""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ResourceName

# Gates the runtime knows how to evaluate. A gate names a fact the runtime
# already measures; anything needing a judgement call is an approval, not a
# gate, and the two are not interchangeable.
DEFAULT_MAX_CDC_LAG = timedelta(seconds=2)


class CutoverGates(BaseModel):
    """What must be true before traffic may move.

    Every field here is a threshold compared against something the runtime
    measured during the Movements below. None of them is a heuristic, and none
    of them asks a model what it thinks.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Every planned partition verified, not merely copied.
    all_partitions_verified: bool = True
    # How far behind the change stream may be at the moment of cutover.
    max_cdc_lag: timedelta = DEFAULT_MAX_CDC_LAG
    # Critical failures tolerated. Zero, unless someone deliberately says
    # otherwise and has to write the number down to do it.
    critical_verification_failures: int = Field(default=0, ge=0)
    # Target reachable and writable.
    target_healthy: bool = True
    # Source and target schemas still compatible at cutover time, not merely
    # when the migration was planned.
    schema_compatible: bool = True
    # A recorded human decision. Defaults on: a migration that cuts over with
    # nobody accountable is the failure mode this whole workflow exists to
    # prevent.
    require_approval: bool = True

    @model_validator(mode="after")
    def _check_gates(self) -> CutoverGates:
        if self.max_cdc_lag < timedelta(0):
            raise ValueError("maxCdcLag cannot be negative")
        return self


class RollbackPolicy(BaseModel):
    """How long the source stays authoritative after cutover."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    window: timedelta = timedelta(hours=24)
    # Traffic rollback rather than reverse bulk migration (RFC 0 §7 Phase 9).
    # Turning this off means accepting that rolling back is a migration of its
    # own, which v1.1 does not do.
    source_remains_authoritative: bool = True

    @model_validator(mode="after")
    def _check_policy(self) -> RollbackPolicy:
        if self.window < timedelta(0):
            raise ValueError("rollback window cannot be negative")
        if not self.source_remains_authoritative and self.window > timedelta(0):
            raise ValueError(
                "a rollback window with a non-authoritative source has nothing to roll "
                "back to; set the window to 0 or keep the source authoritative"
            )
        return self


class Migration(BaseModel):
    """A cutover composed from Movements."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: ResourceName
    # The Movements this workflow drives, in dependency order. Named rather
    # than embedded: a Movement is a resource with its own lifecycle, and
    # inlining it here would make the Migration own execution.
    movements: tuple[ResourceName, ...] = Field(min_length=1)
    cutover: CutoverGates = CutoverGates()
    rollback: RollbackPolicy = RollbackPolicy()
    description: str | None = None

    @model_validator(mode="after")
    def _check_migration(self) -> Migration:
        if len(set(self.movements)) != len(self.movements):
            raise ValueError(f"duplicate movements: {sorted(self.movements)}")
        return self
