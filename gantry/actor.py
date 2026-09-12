# SPDX-License-Identifier: Apache-2.0
"""Who initiated a piece of governed work.

A run records an actor so the question "who asked for this" has an answer
months later. The identity comes from the application, not from the proposal:
an agent that could name itself could name someone else, and an audit record
an agent can write is not one.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import StrEnum


class ActorType(StrEnum):
    """What kind of thing initiated a run.

    `UNKNOWN` is the honest default rather than a failure: a library that
    guessed would put a wrong name in a durable record, and an unattributed run
    is more useful than a misattributed one.
    """

    AGENT = "agent"
    USER = "user"
    SERVICE = "service"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ActorRef:
    """The thing that initiated a run.

    `metadata` is for the few facts that help identify a caller later — which
    framework, which deployment. It is deliberately not somewhere to put a
    conversation: a run record is not a transcript store, and anything written
    here outlives the process and lands in a durable file.
    """

    type: ActorType = ActorType.UNKNOWN
    id: str | None = None
    session_id: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.id is not None and not self.id.strip():
            raise ValueError("actor id must not be empty when given")
        oversized = [key for key, value in self.metadata.items() if len(str(value)) > 1024]
        if oversized:
            raise ValueError(
                f"actor metadata values must stay small; too long: {', '.join(sorted(oversized))}"
            )

    @property
    def label(self) -> str:
        """`agent:migration-agent`, or just the type when nothing identified it."""
        return self.type.value if self.id is None else f"{self.type.value}:{self.id}"

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {"type": self.type.value, "id": self.id}
        if self.session_id is not None:
            payload["session_id"] = self.session_id
        if self.metadata:
            payload["metadata"] = {str(key): value for key, value in self.metadata.items()}
        return payload


UNKNOWN_ACTOR = ActorRef()

_current: ContextVar[ActorRef] = ContextVar("gantry_actor", default=UNKNOWN_ACTOR)
_environment: ContextVar[str | None] = ContextVar("gantry_environment", default=None)


def actor(
    type: str | ActorType = ActorType.AGENT,  # noqa: A002
    id: str | None = None,  # noqa: A002
    *,
    session_id: str | None = None,
    metadata: Mapping[str, object] | None = None,
) -> ActorRef:
    """Name the caller. `gantry.actor("agent", "research-agent")`."""
    return ActorRef(
        type=ActorType(type) if not isinstance(type, ActorType) else type,
        id=id,
        session_id=session_id,
        metadata=dict(metadata or {}),
    )


@contextmanager
def context(*, actor: ActorRef | None = None, environment: str | None = None) -> Iterator[ActorRef]:
    """Establish the trusted context every run inside the block is judged in.

    Both facts come from the host application and neither is reachable from a
    proposal: an agent that could name its own actor could name someone else's,
    and one that could name its own environment could call production staging.

    Context variables rather than globals: concurrent requests in one process
    each keep their own caller and environment, which a module-level assignment
    would not.
    """
    if actor is None and environment is None:
        raise ValueError("a trusted context must set an actor, an environment, or both")
    if environment is not None and not environment.strip():
        raise ValueError("environment must not be empty when given")
    actor_token = None if actor is None else _current.set(actor)
    environment_token = (
        None if environment is None else _environment.set(environment.strip().lower())
    )
    try:
        yield actor or _current.get()
    finally:
        if environment_token is not None:
            _environment.reset(environment_token)
        if actor_token is not None:
            _current.reset(actor_token)


def current_actor() -> ActorRef:
    """The actor in scope, or the unknown one. Never raises, never guesses."""
    return _current.get()


def current_environment() -> str | None:
    """The environment label in scope, or `None` when the application set none.

    `None` is not `dev`. A library that assumed a default would let a policy
    scoped to `prod` quietly stop applying in the one place it was written for.
    """
    return _environment.get()


__all__ = [
    "UNKNOWN_ACTOR",
    "ActorRef",
    "ActorType",
    "actor",
    "context",
    "current_actor",
    "current_environment",
]
