"""The agent access policy, as declared.

This is the RFC's `agentAccess` block:

    agentAccess:
      default:  {rows: deny, aggregates: allow, metadata: allow}
      pii:      {mode: redact}
      samples:  {maxRows: 50, requireReason: true}
      queries:  {maxBytesScanned: 100GB, timeout: 5m}
      evidence: {persist: true}

Kept separate from the Dataset manifest on purpose. A manifest describes what
a Dataset *is*; this describes what an agent may do with it, and the two change
for entirely different reasons and by different people.
"""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from gantry.core.durations import parse_duration
from gantry.core.sizes import parse_byte_size


class Decision(StrEnum):
    """What a policy permits, ordered from most to least permissive."""

    ALLOW = "allow"
    # Permitted, but with sensitive fields masked before anything is returned.
    REDACT = "redact"
    DENY = "deny"


_RANK = {Decision.ALLOW: 0, Decision.REDACT: 1, Decision.DENY: 2}


def most_restrictive(*decisions: Decision) -> Decision:
    """The strictest of several decisions.

    Every place two policies meet resolves this way. A Dataset can tighten the
    global default and can never loosen it, which is the only composition rule
    that does not need a precedence table nobody remembers.
    """
    return max(decisions, key=lambda decision: _RANK[decision])


class PiiMode(StrEnum):
    REDACT = "redact"
    DENY = "deny"
    ALLOW = "allow"


class AccessDefaults(BaseModel):
    """The three-way default the RFC states."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: Decision = Decision.DENY
    aggregates: Decision = Decision.ALLOW
    metadata: Decision = Decision.ALLOW


class PiiRules(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: PiiMode = PiiMode.REDACT


class SampleRules(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    max_rows: int = Field(default=50, ge=0, alias="maxRows")
    # A sample is the first rung that hands over real records. Requiring a
    # stated reason does not authorise anything by itself; it is what makes
    # the audit trail answer "why" as well as "what".
    require_reason: bool = Field(default=True, alias="requireReason")


class QueryRules(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    max_bytes_scanned: int | None = Field(default=None, alias="maxBytesScanned")
    timeout: timedelta | None = None
    # Smallest group an aggregate may return. At 1 an aggregate over a unique
    # key is row access wearing a GROUP BY, and raising this is what closes
    # that gap - see docs/guarantees.md.
    min_group_size: int = Field(default=1, ge=1, alias="minGroupSize")

    @field_validator("max_bytes_scanned", mode="before")
    @classmethod
    def _parse_bytes(cls, value: object) -> object:
        return parse_byte_size(value) if isinstance(value, str) else value

    @field_validator("timeout", mode="before")
    @classmethod
    def _parse_timeout(cls, value: object) -> object:
        return parse_duration(value) if isinstance(value, str) else value


class EvidenceRules(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    persist: bool = True


class AccessRules(BaseModel):
    """The whole `agentAccess` policy."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    default: AccessDefaults = AccessDefaults()
    pii: PiiRules = PiiRules()
    samples: SampleRules = SampleRules()
    queries: QueryRules = QueryRules()
    evidence: EvidenceRules = EvidenceRules()

    def for_class(self, *, rows: bool, values: bool) -> Decision:
        """The declared default covering a rung of this kind."""
        if rows:
            return self.default.rows
        if values:
            return self.default.aggregates
        return self.default.metadata
