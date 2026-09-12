# SPDX-License-Identifier: Apache-2.0
"""The provider-independent question policy answers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from gantry.actor import ActorRef
from gantry.runs.model import OperationKind, ResourceRef


@dataclass(frozen=True, slots=True)
class PolicyRequest:
    """One proposed operation, normalized away from any engine's syntax.

    Built by provider inspection, never by the agent: `inputs` and `outputs`
    are what Gantry determined the proposal touches, not what it claimed to
    touch. `constraints` are the bounds the operation was configured with, so a
    rule can require that a request already sits under a ceiling.

    `unresolved` is how inspection says it could not tell. A request carrying
    any is denied — an effect nobody can name is not one anybody authorized.
    """

    actor: ActorRef
    operation: OperationKind
    engine: str
    inputs: tuple[ResourceRef, ...] = ()
    outputs: tuple[ResourceRef, ...] = ()
    environment: str | None = None
    constraints: Mapping[str, float] = field(default_factory=dict)
    unresolved: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation", OperationKind(self.operation))
        object.__setattr__(self, "inputs", tuple(self.inputs))
        object.__setattr__(self, "outputs", tuple(self.outputs))
        object.__setattr__(self, "unresolved", tuple(self.unresolved))
        environment = self.environment
        if environment is not None:
            object.__setattr__(self, "environment", environment.strip().lower() or None)

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "actor": self.actor.label,
            "operation": self.operation.value,
            "engine": self.engine,
            "inputs": [ref.resource for ref in self.inputs],
            "outputs": [ref.resource for ref in self.outputs],
            "environment": self.environment,
        }
        if self.constraints:
            payload["constraints"] = dict(sorted(self.constraints.items()))
        if self.unresolved:
            payload["unresolved"] = list(self.unresolved)
        return payload


def resources(refs: Sequence[ResourceRef]) -> tuple[str, ...]:
    return tuple(ref.resource for ref in refs)
