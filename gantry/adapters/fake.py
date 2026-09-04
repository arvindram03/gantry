"""Fault-injecting fakes.

These exist to make failure ordinary. The runtime's guarantees are claims about
what happens when things go wrong, so the things that go wrong have to be
reproducible: a worker dying between commit and checkpoint is a test, not an
incident nobody can recreate.

The fake target is idempotent by stable key, which is what makes at-least-once
dispatch safe. If a replay ever produces a duplicate effect here, the guarantee
is broken.
"""

from __future__ import annotations

from dataclasses import dataclass, field


class SimulatedCrashError(Exception):
    """A worker process dying. Not catchable as an ordinary failure."""


class SimulatedFailureError(Exception):
    """A retryable error: a transient source, target or network fault."""


@dataclass
class FaultSpec:
    """Which faults to inject, keyed by the node they should strike."""

    # The hardest case in section 8.7: the effect commits, then the process
    # dies before the checkpoint advances. On replay the effect must not repeat.
    crash_after_commit: set[str] = field(default_factory=set)
    # Death before any effect is applied.
    crash_before_commit: set[str] = field(default_factory=set)
    # Retryable failures, consumed one per occurrence.
    transient_failures: dict[str, int] = field(default_factory=dict)
    # Nodes whose effect is delivered twice, as a duplicate would be.
    duplicate_delivery: set[str] = field(default_factory=set)

    def clear(self, node_id: str) -> None:
        """Stop injecting faults for a node, modelling a repaired system."""
        self.crash_after_commit.discard(node_id)
        self.crash_before_commit.discard(node_id)
        self.duplicate_delivery.discard(node_id)
        self.transient_failures.pop(node_id, None)


@dataclass
class FakeTarget:
    """An idempotent target keyed by a stable identity.

    Applying the same key twice is a no-op and is counted, so a test can assert
    that a replay produced a suppressed duplicate rather than a second effect.
    """

    applied: dict[str, str] = field(default_factory=dict)
    apply_calls: int = 0
    suppressed_duplicates: int = 0

    def apply(self, key: str, value: str) -> bool:
        """Apply an effect. Returns True when it was new."""
        self.apply_calls += 1
        if key in self.applied:
            self.suppressed_duplicates += 1
            return False
        self.applied[key] = value
        return True

    @property
    def effect_count(self) -> int:
        return len(self.applied)


class FakeWorkload:
    """The work a plan node performs, with faults injected around it.

    The commit/checkpoint ordering lives in the worker, not here. This only
    provides an effect that can fail in the specific ways the runtime claims to
    survive.
    """

    def __init__(self, target: FakeTarget, faults: FaultSpec | None = None) -> None:
        self.target = target
        self.faults = faults or FaultSpec()

    def run(self, operation: str, node_id: str) -> str:
        """Perform one node's effect and return its idempotency key."""
        if node_id in self.faults.crash_before_commit:
            self.faults.crash_before_commit.discard(node_id)
            raise SimulatedCrashError(f"worker died before committing {node_id}")

        remaining = self.faults.transient_failures.get(node_id, 0)
        if remaining > 0:
            self.faults.transient_failures[node_id] = remaining - 1
            raise SimulatedFailureError(f"transient fault on {node_id} ({remaining} left)")

        key = f"{operation}/{node_id}"
        self.target.apply(key, node_id)

        if node_id in self.faults.duplicate_delivery:
            # The same effect arriving twice, as at-least-once delivery allows.
            self.target.apply(key, node_id)

        if node_id in self.faults.crash_after_commit:
            self.faults.crash_after_commit.discard(node_id)
            raise SimulatedCrashError(f"worker died after committing {node_id}, before checkpoint")

        return key
