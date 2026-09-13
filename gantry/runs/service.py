# SPDX-License-Identifier: Apache-2.0
"""Host-side control over a parked run: confirm it, or decline it.

This is the second path the confirmation spec asks for. The agent proposes on
one path and never reaches this one — the default tool exposes no way to call
it — so the gate cannot be satisfied by the thing it exists to gate.

What lives here is the state transition, not the work. A provider parked its own
continuation; confirming is what releases it, and it is released exactly once
whichever host process asks and however many times.
"""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from gantry.confirmation.model import ConfirmationRecord, ConfirmationRequirement
from gantry.confirmation.status import ConfirmationStatus
from gantry.runs.model import ResourceRef, Run
from gantry.runs.status import RunStatus
from gantry.runs.store import compare_and_set as _compare_and_set
from gantry.runs.store import get as _get_run

if TYPE_CHECKING:
    # Policy imports the run model, and this module is reached through
    # `gantry.runs`, so importing policy here at module scope closes a cycle.
    # The evaluator is imported inside `gate` instead.
    from gantry.policy.decision import PolicyDecision
    from gantry.policy.model import Policy
    from gantry.policy.request import PolicyRequest

#: How many parked runs one process keeps resumable at once. A registry that
#: grew without bound would be the leak this repo already has open issues about:
#: a run nobody ever answers would pin its continuation, and its closed-over
#: proposal, for the life of the process. The oldest is dropped when the cap is
#: reached — the run itself stays in the store and stays readable, it just can
#: no longer be resumed here.
MAX_PENDING = 512


class ConfirmationError(RuntimeError):
    """A confirmation that cannot be honoured, refused rather than approximated."""


class Resumable(Protocol):
    """The part of the run recorder this service drives.

    A protocol rather than the class, so the service depends on the two
    transitions it needs and not on the whole lifecycle.
    """

    def awaiting_confirmation(self, requirement: ConfirmationRequirement) -> Run: ...

    def resumed(self, run: Run) -> None: ...

    def policy_rejected(self, decision: PolicyDecision, request: PolicyRequest) -> Run: ...

    def admitted(
        self,
        *,
        inputs: Sequence[ResourceRef] = (),
        decision: PolicyDecision | None = None,
        request: PolicyRequest | None = None,
    ) -> Run: ...


@dataclass(slots=True)
class _Parked:
    """One run waiting on a human, and the work that resumes if they agree."""

    recorder: Resumable
    resume: Callable[[], Awaitable[Run]]
    proposal_hash: str | None = None


@dataclass
class _Registry:
    """Process-local parked runs. Locked, because hosts confirm from threads."""

    entries: dict[str, _Parked] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, run_id: str, parked: _Parked) -> None:
        with self.lock:
            while len(self.entries) >= MAX_PENDING:
                self.entries.pop(next(iter(self.entries)))
            self.entries[run_id] = parked

    def peek(self, run_id: str) -> _Parked | None:
        with self.lock:
            return self.entries.get(run_id)

    def take(self, run_id: str) -> _Parked | None:
        """Remove and return, so one parked run resumes at most once."""
        with self.lock:
            return self.entries.pop(run_id, None)

    def drop(self, run_id: str) -> None:
        with self.lock:
            self.entries.pop(run_id, None)


_pending = _Registry()


def park(
    recorder: Resumable,
    requirement: ConfirmationRequirement,
    resume: Callable[[], Awaitable[Run]],
) -> Run:
    """Record the run as awaiting confirmation and keep the work resumable.

    Called by a provider once policy has allowed the operation and a rule has
    asked for confirmation. Persisting comes first: the caller must not be able
    to hold an `AWAITING_CONFIRMATION` run that the store has not recorded.
    """
    run = recorder.awaiting_confirmation(requirement)
    _pending.add(
        run.id,
        _Parked(
            recorder=recorder,
            resume=resume,
            proposal_hash=None if run.proposal is None else run.proposal.hash,
        ),
    )
    return run


async def gate(
    recorder: Resumable,
    *,
    policy: Policy | None,
    request: PolicyRequest,
    resume: Callable[[], Awaitable[Run]],
) -> Run:
    """The admission gate every provider goes through, written once.

    Three outcomes in a fixed order, and the order is the point: policy decides
    authority, confirmation decides whether to ask first, and only then does
    anything external happen. A refused proposal never reaches `resume`, and
    neither does one waiting on a user — so "no execution while confirmation is
    pending" holds for every engine because none of them implements it.
    """
    from gantry.policy.evaluator import evaluate

    if policy is None:
        recorder.admitted(request=request)
        return await resume()
    decision = evaluate(policy, request)
    if not decision.allowed:
        return recorder.policy_rejected(decision, request)
    recorder.admitted(request=request, decision=decision)
    if decision.confirmation.required:
        return park(recorder, decision.confirmation, resume)
    return await resume()


async def confirm(run_id: str, *, metadata: Mapping[str, object] | None = None) -> Run:
    """The host says the user agreed. Resume the run and return what it became.

    Idempotent and single-shot: the transition out of `AWAITING_CONFIRMATION` is
    a compare-and-set in the store, so of any number of concurrent or repeated
    confirmations exactly one releases the work and the rest return the run as
    it now is.

    This records that the host supplied confirmation. It does not record that an
    authenticated person approved anything — Gantry cannot know that, and v0
    does not claim it.
    """
    run = _require(run_id)
    if run.status is not RunStatus.AWAITING_CONFIRMATION:
        return _already(run, ConfirmationStatus.CONFIRMED)

    parked = _pending.peek(run_id)
    if parked is None:
        raise ConfirmationError(
            f"run {run_id} is awaiting confirmation but cannot be resumed in this process; "
            "v0 resumes a parked run only where it was proposed"
        )
    _check_proposal(run, parked)

    record = _record(run, run_id).confirmed(metadata=metadata)
    # Durable before anything external happens, and atomic so that only one
    # caller is ever the reason work starts.
    if not _compare_and_set(
        run_id,
        RunStatus.AWAITING_CONFIRMATION,
        run.advanced(RunStatus.RUNNING, confirmation=record),
    ):
        return _already(_require(run_id), ConfirmationStatus.CONFIRMED)

    claimed = _pending.take(run_id)
    if claimed is None:  # pragma: no cover - another task took it between peek and take
        return _require(run_id)
    confirmed = _require(run_id)
    claimed.recorder.resumed(confirmed)
    return await claimed.resume()


async def decline(run_id: str, *, metadata: Mapping[str, object] | None = None) -> Run:
    """The host says no. The run becomes terminal and nothing external runs."""
    run = _require(run_id)
    if run.status is not RunStatus.AWAITING_CONFIRMATION:
        return _already(run, ConfirmationStatus.DECLINED)

    record = _record(run, run_id).declined(metadata=metadata)
    declined = run.advanced(RunStatus.CONFIRMATION_DECLINED, confirmation=record)
    if not _compare_and_set(run_id, RunStatus.AWAITING_CONFIRMATION, declined):
        return _already(_require(run_id), ConfirmationStatus.DECLINED)
    _pending.drop(run_id)
    return declined


def awaiting() -> tuple[str, ...]:
    """Run ids parked in this process, for a host that wants to list them."""
    with _pending.lock:
        return tuple(_pending.entries)


def _require(run_id: str) -> Run:
    run = _get_run(run_id)
    if run is None:
        raise ConfirmationError(f"no such run: {run_id}")
    return run


def _record(run: Run, run_id: str) -> ConfirmationRecord:
    if run.confirmation is not None:
        return run.confirmation
    return ConfirmationRecord(status=ConfirmationStatus.REQUIRED, run_id=run_id)


def _already(run: Run, wanted: ConfirmationStatus) -> Run:
    """A run that has moved on: idempotent where it can be, refused where not.

    Confirming twice is ordinary — a host retries, two operators click at once —
    and must not run the work twice. Confirming something already declined is a
    different request, and answering it quietly would mean the run executed
    after someone said no.
    """
    status = None if run.confirmation is None else run.confirmation.status
    if status is ConfirmationStatus.DECLINED and wanted is ConfirmationStatus.CONFIRMED:
        raise ConfirmationError(f"run {run.id} was declined and cannot be confirmed")
    if status is ConfirmationStatus.CONFIRMED and wanted is ConfirmationStatus.DECLINED:
        raise ConfirmationError(f"run {run.id} was already confirmed and cannot be declined")
    if status is None or status is ConfirmationStatus.NOT_REQUIRED:
        raise ConfirmationError(
            f"run {run.id} is {run.status.value} and never required confirmation"
        )
    return run


def _check_proposal(run: Run, parked: _Parked) -> None:
    """Confirmation is for one immutable proposal, not for a run id.

    The proposal travels in the parked continuation, so swapping the stored
    record cannot change what executes — but a mismatch means the two disagree
    about what was asked, and executing under that disagreement is exactly what
    §15 forbids.
    """
    recorded = None if run.confirmation is None else run.confirmation.proposal_hash
    if recorded != parked.proposal_hash:
        raise ConfirmationError(
            f"run {run.id} was confirmed against a different proposal "
            f"({recorded} != {parked.proposal_hash}); a changed proposal needs a new run"
        )


__all__ = [
    "MAX_PENDING",
    "ConfirmationError",
    "Resumable",
    "awaiting",
    "confirm",
    "decline",
    "gate",
    "park",
]
