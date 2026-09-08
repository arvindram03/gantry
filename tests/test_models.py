# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from gantry import (
    Artifact,
    CheckResult,
    Execution,
    ExecutionHandle,
    ExecutionState,
    ExecutionTarget,
    OutputKind,
    OutputRef,
    PolicyRequirements,
    VerificationResult,
)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Artifact(payload="x", kind=""),
        lambda: ExecutionTarget(kind=""),
        lambda: ExecutionHandle("", "sql", "bigquery", "native"),
        lambda: ExecutionHandle("run", "", "bigquery", "native"),
        lambda: ExecutionHandle("run", "sql", "", "native"),
        lambda: ExecutionHandle("run", "sql", "bigquery", ""),
        lambda: ExecutionHandle("run", "sql", "bigquery", "native", submitted_at=datetime.now()),
        lambda: OutputRef(OutputKind.TABLE, ""),
    ],
)
def test_required_identifiers_are_validated(factory: Callable[[], object]) -> None:
    with pytest.raises(ValueError):
        factory()


def test_artifact_preserves_engine_native_declarations() -> None:
    artifact = Artifact(
        kind="beam_python",
        payload="pipeline code",
        dependencies=("apache-beam==2.60",),
        declared_inputs=("logs",),
        declared_outputs=("summary",),
    )

    assert artifact.dependencies == ("apache-beam==2.60",)
    assert artifact.declared_inputs == ("logs",)


def test_conflicting_policy_is_rejected_during_construction() -> None:
    with pytest.raises(ValueError, match="read-only"):
        PolicyRequirements(read_only=True, allow_writes=True)


def test_policy_limits_must_be_sensible() -> None:
    with pytest.raises(ValueError, match="runtime"):
        PolicyRequirements(max_runtime_seconds=0)
    with pytest.raises(ValueError, match="cost"):
        PolicyRequirements(max_cost_usd=-1)


def test_verification_checks_are_structured() -> None:
    check = CheckResult("max_row_expansion", False, "<= 1.2x", "20x", "too large")
    verification = VerificationResult(ok=False, checks=(check,))

    assert verification.checks[0].actual == "20x"
    assert ExecutionState.SUBMITTED.value == "SUBMITTED"
    assert datetime.now(UTC).tzinfo is UTC


def test_execution_terminal_and_verification_factories() -> None:
    handle = ExecutionHandle("run", "sql", "bigquery", "native")

    assert not Execution(handle, ExecutionState.RUNNING).terminal
    assert Execution(handle, ExecutionState.SUCCEEDED).terminal
    assert VerificationResult.passed(CheckResult("schema", True)).ok
