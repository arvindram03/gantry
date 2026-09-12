# SPDX-License-Identifier: Apache-2.0
"""Portable verification contracts and structured check results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable

from gantry.artifact import Artifact
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult


class CheckSource(StrEnum):
    """Who committed Gantry to a verification requirement."""

    TRUSTED = "trusted"
    AGENT = "agent"


# The draft uses both names; expose both without creating two provenance types.
VerificationSource = CheckSource


@dataclass(frozen=True, slots=True, init=False)
class CheckResult:
    """One verification check and what it observed.

    `expected` and `actual` are recorded even when the check passes, so an
    accepted result still carries the evidence for why it was accepted.
    """

    name: str
    ok: bool
    expected: object | None = None
    actual: object | None = None
    message: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)
    supported: bool = True
    """Whether the provider could evaluate this check at all.

    A check that could not be evaluated fails, because a bound nobody measured
    is not a bound. But "the destination had no rows" and "I could not count
    the rows" are different facts, and collapsing them hides the second — which
    is a gap in the provider, not in the data.
    """

    source: CheckSource | str | None = None
    """Who supplied the requirement: ``trusted`` or ``agent``.

    Strings remain accepted for evidence written before provenance existed.
    New governed operations assign :class:`CheckSource` themselves; it is not
    accepted in agent verification input.
    """

    evidence_refs: tuple[str, ...] = ()
    """Bounded references to observations supporting the decision."""

    def __init__(
        self,
        name: str | None = None,
        ok: bool | None = None,
        expected: object | None = None,
        actual: object | None = None,
        message: str | None = None,
        metadata: Mapping[str, object] | None = None,
        supported: bool = True,
        source: CheckSource | str | None = None,
        evidence_refs: tuple[str, ...] = (),
        *,
        check: str | None = None,
        passed: bool | None = None,
        observed: object | None = None,
    ) -> None:
        """Accept both the v0.2 vocabulary and the pre-v0.2 field names."""
        resolved_name = check if check is not None else name
        resolved_ok = passed if passed is not None else ok
        if resolved_name is None:
            raise TypeError("check is required")
        if resolved_ok is None:
            raise TypeError("passed is required")
        if check is not None and name is not None and check != name:
            raise ValueError("check and name disagree")
        if passed is not None and ok is not None and passed != ok:
            raise ValueError("passed and ok disagree")
        if observed is not None and actual is not None and observed != actual:
            raise ValueError("observed and actual disagree")
        object.__setattr__(self, "name", resolved_name)
        object.__setattr__(self, "ok", resolved_ok)
        object.__setattr__(self, "expected", expected)
        object.__setattr__(self, "actual", observed if observed is not None else actual)
        object.__setattr__(self, "message", message)
        object.__setattr__(self, "metadata", {} if metadata is None else metadata)
        object.__setattr__(self, "supported", supported)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "evidence_refs", tuple(evidence_refs))

    @property
    def check(self) -> str:
        """Specification spelling for :attr:`name`."""
        return self.name

    @property
    def passed(self) -> bool:
        """Specification spelling for :attr:`ok`."""
        return self.ok

    @property
    def observed(self) -> object | None:
        """Specification spelling for :attr:`actual`."""
        return self.actual


@dataclass(frozen=True, slots=True, init=False)
class VerificationResult:
    """Whether the result may be believed, with the checks that decided it.

    A failing verification turns an engine success into
    `ResultStatus.VERIFICATION_FAILED`: the job ran, and the answer is still
    not usable. Build one with `passed()` or `failed()`.
    """

    ok: bool
    checks: tuple[CheckResult, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __init__(
        self,
        ok: bool | None = None,
        checks: tuple[CheckResult, ...] = (),
        metadata: Mapping[str, object] | None = None,
        *,
        passed: bool | None = None,
    ) -> None:
        """Accept ``passed=`` from v0.2 while retaining ``ok=`` compatibility."""
        resolved = passed if passed is not None else ok
        if resolved is None:
            raise TypeError("passed is required")
        if passed is not None and ok is not None and passed != ok:
            raise ValueError("passed and ok disagree")
        object.__setattr__(self, "ok", resolved)
        object.__setattr__(self, "checks", tuple(checks))
        object.__setattr__(self, "metadata", {} if metadata is None else metadata)

    @property
    def failed_checks(self) -> tuple[CheckResult, ...]:
        """The checks that decided against acceptance, for a caller to act on."""
        return tuple(check for check in self.checks if not check.ok)

    @property
    def unsupported_checks(self) -> tuple[CheckResult, ...]:
        """Checks the provider could not evaluate.

        Separate from `failed_checks` because the remedy is different: a failed
        check means fix the data or the query, an unsupported one means this
        provider cannot answer the question and something has to change about
        the policy or the backend.
        """
        return tuple(check for check in self.checks if not check.supported)

    @classmethod
    def passed(cls, *checks: CheckResult) -> VerificationResult:
        return cls(ok=True, checks=checks)

    @classmethod
    def failed(
        cls,
        message: str,
        *,
        name: str = "verification",
        expected: object | None = None,
        actual: object | None = None,
    ) -> VerificationResult:
        return cls(
            ok=False,
            checks=(
                CheckResult(
                    name=name,
                    ok=False,
                    expected=expected,
                    actual=actual,
                    message=message,
                ),
            ),
        )


@runtime_checkable
class Verifier(Protocol):
    """A check run after the engine succeeds, deciding whether to accept.

    Implement `verify` and pass instances to `wait` or `run`. It receives what
    was proposed (`artifact`, `context`) and what happened (`execution`,
    `result`), so it can compare the two rather than trusting either.
    """

    async def verify(
        self,
        *,
        artifact: Artifact,
        context: Context,
        execution: Execution,
        result: ExecutionResult,
    ) -> VerificationResult: ...
