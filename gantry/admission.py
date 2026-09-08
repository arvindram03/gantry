# SPDX-License-Identifier: Apache-2.0
"""Fail-closed admission and capability negotiation."""

from __future__ import annotations

from dataclasses import dataclass

from gantry.capabilities import AdapterCapabilities
from gantry.execution import ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.policy import PolicyRequirements


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    allowed: bool
    validation: ValidationResult
    capabilities: AdapterCapabilities
    reasons: tuple[Failure, ...] = ()


def _unsupported(name: str) -> Failure:
    return Failure(
        kind=FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
        retryable=False,
        message=f"adapter cannot enforce required capability: {name}",
    )


def admit(
    validation: ValidationResult,
    capabilities: AdapterCapabilities,
    policy: PolicyRequirements,
) -> AdmissionDecision:
    reasons = [
        Failure(
            kind=FailureKind.VALIDATION_ERROR,
            retryable=False,
            message=error,
        )
        for error in validation.errors
    ]
    requirements = (
        (policy.read_only, capabilities.read_only_execution, "read_only_execution"),
        (policy.allow_writes, capabilities.write_execution, "write_execution"),
        (policy.max_cost_usd is not None, capabilities.cost_limit, "cost_limit"),
        (policy.require_cancel, capabilities.cancellation, "cancellation"),
        (policy.require_reconnect, capabilities.reconnect, "reconnect"),
        (
            policy.require_scoped_credentials,
            capabilities.scoped_credentials,
            "scoped_credentials",
        ),
        (policy.require_network_isolation, capabilities.network_isolation, "network_isolation"),
        (
            policy.require_filesystem_isolation,
            capabilities.filesystem_isolation,
            "filesystem_isolation",
        ),
        (
            policy.require_ephemeral_environment,
            capabilities.ephemeral_environment,
            "ephemeral_environment",
        ),
        (policy.require_metrics, capabilities.metrics, "metrics"),
        (policy.require_result_reference, capabilities.result_reference, "result_reference"),
    )
    reasons.extend(
        _unsupported(name)
        for required, supported, name in requirements
        if required and not supported
    )

    if policy.max_runtime_seconds is not None:
        gantry_can_enforce = capabilities.reconnect and capabilities.cancellation
        if not capabilities.runtime_limit and not gantry_can_enforce:
            reasons.append(_unsupported("runtime_limit"))

    if not validation.ok and not validation.errors:
        reasons.append(
            Failure(
                kind=FailureKind.VALIDATION_ERROR,
                retryable=False,
                message="adapter validation failed without an error",
            )
        )
    return AdmissionDecision(
        allowed=validation.ok and not reasons,
        validation=validation,
        capabilities=capabilities,
        reasons=tuple(reasons),
    )
