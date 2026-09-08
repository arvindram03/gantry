# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from gantry import (
    AdapterCapabilities,
    FailureKind,
    PolicyRequirements,
    ValidationResult,
    admit,
)


def test_admission_accepts_satisfied_requirements() -> None:
    decision = admit(
        ValidationResult.accepted(warnings=("estimate approximate",)),
        AdapterCapabilities(
            reconnect=True,
            cancellation=True,
            read_only_execution=True,
            metrics=True,
            result_reference=True,
        ),
        PolicyRequirements(
            read_only=True,
            max_runtime_seconds=600,
            require_reconnect=True,
            require_cancel=True,
            require_metrics=True,
            require_result_reference=True,
        ),
    )

    assert decision.allowed
    assert decision.reasons == ()


def test_admission_reports_every_missing_capability() -> None:
    policy = PolicyRequirements(
        allow_writes=True,
        max_runtime_seconds=10,
        max_cost_usd=20,
        require_cancel=True,
        require_reconnect=True,
        require_scoped_credentials=True,
        require_network_isolation=True,
        require_filesystem_isolation=True,
        require_ephemeral_environment=True,
        require_metrics=True,
        require_result_reference=True,
    )

    decision = admit(ValidationResult.accepted(), AdapterCapabilities(), policy)

    assert not decision.allowed
    assert {reason.kind for reason in decision.reasons} == {
        FailureKind.UNSUPPORTED_POLICY_REQUIREMENT
    }
    assert len(decision.reasons) == 11


def test_native_runtime_limit_satisfies_timeout_policy() -> None:
    decision = admit(
        ValidationResult.accepted(),
        AdapterCapabilities(runtime_limit=True),
        PolicyRequirements(max_runtime_seconds=30),
    )

    assert decision.allowed


def test_validation_errors_are_normalized() -> None:
    decision = admit(
        ValidationResult.rejected("syntax error", "target unavailable"),
        AdapterCapabilities(),
        PolicyRequirements(),
    )

    assert not decision.allowed
    assert [reason.kind for reason in decision.reasons] == [
        FailureKind.VALIDATION_ERROR,
        FailureKind.VALIDATION_ERROR,
    ]


def test_validation_failure_without_errors_still_has_a_reason() -> None:
    decision = admit(
        ValidationResult(ok=False),
        AdapterCapabilities(),
        PolicyRequirements(),
    )

    assert decision.reasons[0].message == "adapter validation failed without an error"
