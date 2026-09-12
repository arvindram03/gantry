# SPDX-License-Identifier: Apache-2.0
"""Portable verification contracts and structured check results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from gantry.artifact import Artifact
from gantry.context import Context
from gantry.execution import Execution, ExecutionResult


@dataclass(frozen=True, slots=True)
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

    source: str | None = None
    """Where the observation behind this check came from — `postgres`, `flink`.

    A check that passed against the engine's own account of itself is weaker
    evidence than one measured at the destination, and a reader cannot tell
    which they have unless the result says.
    """


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """Whether the result may be believed, with the checks that decided it.

    A failing verification turns an engine success into
    `ResultStatus.VERIFICATION_FAILED`: the job ran, and the answer is still
    not usable. Build one with `passed()` or `failed()`.
    """

    ok: bool
    checks: tuple[CheckResult, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)

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
