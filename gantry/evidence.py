# SPDX-License-Identifier: Apache-2.0
"""What Gantry observed while deciding whether to accept an execution.

A result says a run was accepted. Evidence says why, in enough detail that
someone who was not there — and who does not have the agent conversation that
produced the work — can decide whether they agree.

Three sources, kept distinct because they are trusted differently. The engine
reports its own execution: a job id, a state, a runtime. The output is measured
where it landed: a row count, a schema, an existence check. Gantry records what
it was asked for and what it decided: the checks configured, their results, the
timestamps, the decision.

Bounded by construction. An observation is a name, a scalar and a source, so a
bundle stays small however large the output it describes — the row count of a
billion-row table is one integer. Gantry is not the data plane, and evidence is
not a copy of the data.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from gantry.verifier import CheckResult, CheckSource, VerificationResult


class ObservationSource(StrEnum):
    """Where an observation came from, and therefore how far to trust it.

    `ENGINE` is the engine's account of its own work. `OUTPUT` is a measurement
    of what was produced, taken by Gantry rather than reported by the thing that
    produced it. `GANTRY` is what the control plane itself did — the checks
    requested, the decision reached.
    """

    ENGINE = "engine"
    OUTPUT = "output"
    GANTRY = "gantry"


@dataclass(frozen=True, slots=True)
class Observation:
    """One bounded measurement, with the source that produced it.

    A scalar, deliberately. "The destination has 1,190,432 rows" is an
    observation; the rows themselves are not.
    """

    name: str
    value: object
    source: ObservationSource
    unit: str | None = None
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("observation name must not be empty")

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "name": self.name,
            "value": _plain(self.value),
            "source": self.source.value,
            "observed_at": self.observed_at.isoformat(),
        }
        if self.unit is not None:
            payload["unit"] = self.unit
        return payload


@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    """The record of one governed run: what ran, what was seen, what was decided.

    Serializable on purpose. The point of evidence is that it outlives the
    process that gathered it, so everything here survives `json.dumps`.
    """

    run_id: str
    engine: str
    operation: str
    decision: str
    native_execution_id: str | None = None
    proposal_hash: str | None = None
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()
    started_at: datetime | None = None
    finished_at: datetime | None = None
    execution: Mapping[str, object] = field(default_factory=dict)
    proposal: Mapping[str, object] = field(default_factory=dict)
    observations: tuple[Observation, ...] = ()
    checks: tuple[CheckResult, ...] = ()

    @property
    def trusted_checks(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if check.source == CheckSource.TRUSTED)

    @property
    def agent_checks(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if check.source == CheckSource.AGENT)

    @property
    def verification(self) -> VerificationResult:
        return VerificationResult(ok=all(check.ok for check in self.checks), checks=self.checks)

    @property
    def duration_ms(self) -> int | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return int((self.finished_at - self.started_at).total_seconds() * 1000)

    def observation(self, name: str) -> Observation | None:
        """The most recent observation with this name, if one was made."""
        return next(
            (item for item in reversed(self.observations) if item.name == name),
            None,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "engine": self.engine,
            "operation": self.operation,
            "decision": self.decision,
            "native_execution_id": self.native_execution_id,
            "proposal_hash": self.proposal_hash,
            "inputs": list(self.inputs),
            "outputs": list(self.outputs),
            "started_at": None if self.started_at is None else self.started_at.isoformat(),
            "finished_at": None if self.finished_at is None else self.finished_at.isoformat(),
            "duration_ms": self.duration_ms,
            "execution": {key: _plain(value) for key, value in self.execution.items()},
            "proposal": {key: _plain(value) for key, value in self.proposal.items()},
            "observations": [item.as_dict() for item in self.observations],
            "checks": [_check_as_dict(check) for check in self.checks],
            "trusted_checks": [_check_as_dict(check) for check in self.trusted_checks],
            "agent_checks": [_check_as_dict(check) for check in self.agent_checks],
            "verification": {
                "passed": all(check.ok for check in self.checks),
                "checks": [_check_as_dict(check) for check in self.checks],
            },
        }

    def to_json(self, *, indent: int | None = None) -> str:
        return json.dumps(self.as_dict(), indent=indent, sort_keys=False)


def _check_as_dict(check: CheckResult) -> dict[str, object]:
    """A check as structured data, not as a sentence.

    `expected` and `observed` rather than a rendered message, because something
    downstream has to be able to compare them without parsing English.
    """
    payload: dict[str, object] = {
        "check": check.name,
        "passed": check.ok,
        "expected": _plain(check.expected),
        "observed": _plain(check.actual),
    }
    if check.source is not None:
        payload["source"] = check.source
    if check.message is not None:
        payload["message"] = check.message
    if not check.supported:
        payload["supported"] = False
    if check.metadata:
        payload["metadata"] = {key: _plain(value) for key, value in check.metadata.items()}
    if check.evidence_refs:
        payload["evidence_refs"] = list(check.evidence_refs)
    return payload


def _plain(value: object) -> object:
    """Reduce a value to something `json.dumps` accepts.

    Evidence that cannot be serialized is not evidence, so this never raises:
    anything it does not recognise is rendered as its string form, which is
    worse than a typed value and much better than losing the observation.
    """
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_plain(item) for item in value]
    if isinstance(value, set | frozenset):
        return sorted(_plain(item) for item in value)  # type: ignore[type-var]
    return str(value)


def bundle_from_verification(
    verification: VerificationResult | None,
    *,
    run_id: str,
    engine: str,
    operation: str,
    decision: str,
    **rest: object,
) -> EvidenceBundle:
    """Build a bundle carrying the checks a verification already ran."""
    checks = () if verification is None else verification.checks
    return EvidenceBundle(
        run_id=run_id,
        engine=engine,
        operation=operation,
        decision=decision,
        checks=checks,
        **rest,  # type: ignore[arg-type]
    )


__all__ = [
    "EvidenceBundle",
    "Observation",
    "ObservationSource",
    "bundle_from_verification",
]
