# SPDX-License-Identifier: Apache-2.0
"""Governed batch and stream operations backed by the shared Flink runtime."""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from fnmatch import fnmatchcase
from hashlib import sha256
from typing import NoReturn

from gantry.context import Context
from gantry.execution import Execution, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.flink.api import FlinkRuntime
from gantry.flink.artifact import FlinkMode, FlinkSQLArtifact
from gantry.flink.execution import FlinkResult, StreamingHealth
from gantry.flink.metrics import FlinkMetrics
from gantry.flink.verification import FlinkHealthCheck
from gantry.handle import ExecutionHandle
from gantry.result import ResultStatus
from gantry.runtime import SubmissionError
from gantry.sql.classification import SQLOperation
from gantry.sql.dialect import ConservativeDialect
from gantry.tool import Tool
from gantry.verifier import CheckResult, CheckSource, VerificationResult
from gantry.verify import (
    MaterializationCheck,
    VerificationConflict,
    VerificationInputError,
    VerificationUnsupported,
    check_config,
    detect_conflicts,
    parse_agent_checks,
    sourced_result,
    validate_agent_checks,
    verification_schema,
)

_LEADING_COMMENTS = re.compile(r"\A(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*", re.DOTALL)
_IDENTIFIER = r'(?:[A-Za-z_][A-Za-z0-9_$-]*|"(?:""|[^"])+"|`(?:``|[^`])+`)'
_QUALIFIED_IDENTIFIER = rf"{_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER}){{0,2}}"
_INSERT = re.compile(
    rf"\AINSERT\s+(INTO|OVERWRITE)\s+(?:TABLE\s+)?({_QUALIFIED_IDENTIFIER})(?=\s|\()",
    re.IGNORECASE,
)


class FlinkJobKind(StrEnum):
    BATCH = "batch"
    STREAM = "stream"


class FlinkJobStatement(StrEnum):
    INSERT_INTO = "INSERT_INTO"
    INSERT_OVERWRITE = "INSERT_OVERWRITE"


@dataclass(frozen=True, slots=True)
class FlinkJobPlan:
    statement: FlinkJobStatement
    inputs: tuple[str, ...]
    output: str


class FlinkJobError(Exception):
    """A structured local admission or submission error."""

    def __init__(self, failure: Failure, status: ResultStatus = ResultStatus.REJECTED) -> None:
        self.failure = failure
        self.status = status
        super().__init__(failure.message)


class FlinkJob:
    """A configured native Flink SQL operation."""

    def __init__(
        self,
        runtime: FlinkRuntime,
        *,
        kind: FlinkJobKind,
        inputs: Collection[str],
        outputs: Collection[str],
        checks: Sequence[FlinkHealthCheck | MaterializationCheck] = (),
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        self._runtime = runtime
        self._kind = kind
        self._inputs = _patterns(inputs, "inputs")
        self._outputs = _patterns(outputs, "outputs")
        if not self._outputs:
            raise ValueError("at least one output must be allowed")
        if isinstance(timeout, bool) or (
            timeout is not None and (not isinstance(timeout, (int, float)) or timeout <= 0)
        ):
            raise ValueError("timeout must be a positive number or None")
        if isinstance(poll_interval, bool) or not isinstance(poll_interval, (int, float)):
            raise TypeError("poll interval must be numeric")
        if poll_interval < 0:
            raise ValueError("poll interval must not be negative")
        self._timeout = None if timeout is None else float(timeout)
        self._poll_interval = float(poll_interval)
        self._checks = tuple(checks)
        self._agent_checks: dict[str, tuple[FlinkHealthCheck | MaterializationCheck, ...]] = {}
        self._validate_checks()

    @property
    def kind(self) -> FlinkJobKind:
        return self._kind

    @property
    def input_schema(self) -> Mapping[str, object]:
        capabilities = self._agent_capabilities
        return {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "verify": verification_schema(capabilities),
            },
            "required": ["sql"],
            "additionalProperties": False,
        }

    def inspect(self, sql: str) -> FlinkJobPlan:
        return _parse_job(sql)

    async def validate(self, sql: str, *, context: Context | None = None) -> ValidationResult:
        try:
            plan = self.inspect(sql)
            self._admit(plan)
        except (FlinkJobError, TypeError, ValueError) as error:
            return ValidationResult.rejected(str(error))
        result = await self._runtime.validate(self._artifact(sql, plan), context=context)
        if not result.ok:
            return result
        return ValidationResult.accepted(
            warnings=result.warnings,
            metadata={
                **result.metadata,
                "mode": self._kind.value,
                "inputs": plan.inputs,
                "output": plan.output,
                "operation": plan.statement.value,
            },
        )

    async def submit(
        self,
        sql: str,
        *,
        context: Context | None = None,
        verify: Sequence[FlinkHealthCheck | MaterializationCheck] = (),
    ) -> ExecutionHandle:
        agent_checks = validate_agent_checks(verify, capabilities=self._agent_capabilities)
        detect_conflicts(self._checks, agent_checks)
        plan = self.inspect(sql)
        self._admit(plan)
        try:
            handle = await self._runtime.submit(self._artifact(sql, plan), context=context)
            proposal_hash = sha256(sql.encode()).hexdigest()
            handle = replace(
                handle,
                metadata={
                    **handle.metadata,
                    "gantry.proposal_hash": proposal_hash,
                    "gantry.agent_verification": [check_config(check) for check in agent_checks],
                },
            )
            self._agent_checks[handle.gantry_id] = agent_checks  # type: ignore[assignment]
            return handle
        except SubmissionError as error:
            failure = error.result.failure or Failure(
                FailureKind.SUBMISSION_ERROR,
                False,
                "Flink submission failed",
            )
            raise FlinkJobError(failure, error.result.status) from error

    async def status(self, handle: ExecutionHandle) -> Execution:
        return await self._runtime.status(handle)

    async def metrics(self, handle: ExecutionHandle) -> FlinkMetrics:
        return await self._runtime.metrics(handle)

    async def cancel(self, handle: ExecutionHandle) -> Execution:
        return await self._runtime.cancel(handle)

    async def wait(
        self,
        handle: ExecutionHandle,
        *,
        poll_interval_seconds: float | None = None,
        timeout_seconds: float | None = None,
    ) -> FlinkResult:
        poll_interval = (
            self._poll_interval if poll_interval_seconds is None else poll_interval_seconds
        )
        timeout = self._timeout if timeout_seconds is None else timeout_seconds
        entries = self._check_entries(handle)
        result = await self._runtime.wait(
            handle,
            checks=tuple(check for check, _ in entries if isinstance(check, FlinkHealthCheck)),
            poll_interval_seconds=poll_interval,
            timeout_seconds=timeout,
        )
        result = self._source_health_result(result, entries)
        if self._kind is FlinkJobKind.BATCH and result.ok:
            return _recorded(await self._verify_batch(result, entries))
        return _recorded(self._with_agent_commitment(result, handle))

    async def health(self, handle: ExecutionHandle) -> StreamingHealth:
        if self._kind is not FlinkJobKind.STREAM:
            raise AttributeError("health is available only for streaming jobs")
        checks = tuple(
            check for check, _ in self._check_entries(handle) if isinstance(check, FlinkHealthCheck)
        )
        return await self._runtime.health(handle, checks=checks)

    async def __call__(
        self,
        sql: str,
        *,
        context: Context | None = None,
        verify: Sequence[FlinkHealthCheck | MaterializationCheck] = (),
    ) -> FlinkResult:
        try:
            handle = await self.submit(sql, context=context, verify=verify)
        except VerificationUnsupported as error:
            return _failed(
                ResultStatus.VERIFICATION_UNSUPPORTED,
                Failure(FailureKind.VERIFICATION_UNSUPPORTED, False, str(error)),
            )
        except VerificationConflict as error:
            return _failed(
                ResultStatus.VERIFICATION_CONFLICT,
                Failure(FailureKind.VERIFICATION_CONFLICT, False, str(error)),
            )
        except VerificationInputError as error:
            return _failed(
                ResultStatus.REJECTED,
                Failure(FailureKind.VALIDATION_ERROR, False, str(error)),
            )
        except FlinkJobError as error:
            return _failed(error.status, error.failure)
        except (TypeError, ValueError) as error:
            return _failed(
                ResultStatus.REJECTED,
                Failure(FailureKind.VALIDATION_ERROR, False, str(error)),
            )
        return await self.wait(handle)

    def tool(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> Tool[FlinkResult]:
        default_name = f"flink_{self._kind.value}_job"
        default_description = (
            "Run an approved Flink batch job."
            if self._kind is FlinkJobKind.BATCH
            else "Run an approved Flink streaming job."
        )
        return Tool(
            name=name or default_name,
            description=description or default_description,
            input_schema=self.input_schema,
            _handler=self._invoke_tool,
        )

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> FlinkResult:
        unexpected = set(arguments) - {"sql", "verify"}
        if unexpected:
            names = ", ".join(sorted(unexpected))
            raise ValueError(f"unexpected Flink job tool arguments: {names}")
        sql = arguments.get("sql")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        try:
            checks = parse_agent_checks(
                arguments.get("verify"), capabilities=self._agent_capabilities
            )
        except VerificationUnsupported as error:
            return _failed(
                ResultStatus.VERIFICATION_UNSUPPORTED,
                Failure(FailureKind.VERIFICATION_UNSUPPORTED, False, str(error)),
            )
        except VerificationInputError as error:
            return _failed(
                ResultStatus.REJECTED,
                Failure(FailureKind.VALIDATION_ERROR, False, str(error)),
            )
        return await self(sql, verify=checks)  # type: ignore[arg-type]

    @property
    def _agent_capabilities(self) -> frozenset[str]:
        if self._kind is FlinkJobKind.STREAM:
            runtime = self._runtime.capabilities()
            capabilities: set[str] = set()
            if runtime.remote_status:
                capabilities.add("running")
            if runtime.metrics:
                capabilities.update({"restart_count", "watermark_lag"})
            return frozenset(capabilities)
        if not self._runtime.capabilities().result_reference:
            return frozenset()
        return frozenset({"output_exists", "not_empty", "row_count", "required_columns"})

    @property
    def _health_checks(self) -> tuple[FlinkHealthCheck, ...]:
        return tuple(check for check in self._checks if isinstance(check, FlinkHealthCheck))

    @property
    def _table_checks(self) -> tuple[MaterializationCheck, ...]:
        return tuple(check for check in self._checks if isinstance(check, MaterializationCheck))

    def _validate_checks(self) -> None:
        if self._kind is FlinkJobKind.STREAM:
            if any(not isinstance(check, FlinkHealthCheck) for check in self._checks):
                raise TypeError("stream checks must be Flink health checks")
            return
        if any(
            not isinstance(check, (FlinkHealthCheck, MaterializationCheck))
            for check in self._checks
        ):
            raise TypeError("batch checks must be Flink job or output checks")

    def _admit(self, plan: FlinkJobPlan) -> None:
        if (
            self._kind is FlinkJobKind.STREAM
            and plan.statement is FlinkJobStatement.INSERT_OVERWRITE
        ):
            _reject(
                FailureKind.OPERATION_NOT_ALLOWED,
                "stream jobs support INSERT INTO but not INSERT OVERWRITE",
            )
        denied_input = next(
            (name for name in plan.inputs if not _allowed(name, self._inputs)),
            None,
        )
        if denied_input is not None:
            _reject(FailureKind.INPUT_NOT_ALLOWED, f"input is not allowed: {denied_input}")
        if not _allowed(plan.output, self._outputs):
            _reject(FailureKind.OUTPUT_NOT_ALLOWED, f"output is not allowed: {plan.output}")

    def _artifact(self, sql: str, plan: FlinkJobPlan) -> FlinkSQLArtifact:
        mode = FlinkMode.BATCH if self._kind is FlinkJobKind.BATCH else FlinkMode.STREAMING
        return FlinkSQLArtifact(
            sql,
            mode=mode,
            declared_inputs=plan.inputs,
            declared_outputs=(plan.output,),
        )

    def _check_entries(
        self, handle: ExecutionHandle
    ) -> tuple[tuple[FlinkHealthCheck | MaterializationCheck, CheckSource], ...]:
        return (
            *((check, CheckSource.TRUSTED) for check in self._checks),
            *((check, CheckSource.AGENT) for check in self._agent_checks_for(handle)),
        )

    def _agent_checks_for(
        self, handle: ExecutionHandle
    ) -> tuple[FlinkHealthCheck | MaterializationCheck, ...]:
        local = self._agent_checks.get(handle.gantry_id)
        if local is not None:
            return local
        try:
            checks = parse_agent_checks(
                handle.metadata.get("gantry.agent_verification"),
                capabilities=self._agent_capabilities,
            )
        except VerificationInputError:
            return ()
        return checks  # type: ignore[return-value]

    def _source_health_result(
        self,
        result: FlinkResult,
        entries: Sequence[tuple[FlinkHealthCheck | MaterializationCheck, CheckSource]],
    ) -> FlinkResult:
        if result.verification is None:
            return result
        sources = [source for check, source in entries if isinstance(check, FlinkHealthCheck)]
        checks = tuple(
            sourced_result(check, source, observation_source="flink")
            for check, source in zip(result.verification.checks, sources, strict=False)
        )
        verification = replace(result.verification, checks=checks)
        health = (
            None if result.health is None else replace(result.health, verification=verification)
        )
        evidence = None if result.evidence is None else replace(result.evidence, checks=checks)
        return replace(result, verification=verification, health=health, evidence=evidence)

    def _with_agent_commitment(self, result: FlinkResult, handle: ExecutionHandle) -> FlinkResult:
        if result.evidence is None:
            return result
        agent = self._agent_checks_for(handle)
        proposal_hash = handle.metadata.get("gantry.proposal_hash")
        evidence = replace(
            result.evidence,
            proposal_hash=proposal_hash if isinstance(proposal_hash, str) else None,
            proposal={
                "hash": proposal_hash if isinstance(proposal_hash, str) else None,
                "agent_verification": [check_config(check) for check in agent],
            },
        )
        return replace(result, evidence=evidence)

    def _with_batch_evidence(
        self,
        result: FlinkResult,
        verification: VerificationResult,
    ) -> FlinkResult:
        evidence = (
            None
            if result.evidence is None
            else replace(
                result.evidence,
                decision=result.status.value,
                checks=verification.checks,
            )
        )
        updated = replace(result, verification=verification, evidence=evidence)
        return (
            self._with_agent_commitment(updated, result.handle)
            if result.handle is not None
            else updated
        )

    async def _verify_batch(
        self,
        result: FlinkResult,
        entries: Sequence[tuple[FlinkHealthCheck | MaterializationCheck, CheckSource]],
    ) -> FlinkResult:
        table_entries = tuple(
            (check, source) for check, source in entries if isinstance(check, MaterializationCheck)
        )
        table_checks = tuple(check for check, _ in table_entries)
        health_results: tuple[CheckResult, ...] = ()
        if result.verification is not None:
            health_results = result.verification.checks
        if not table_checks:
            verification = VerificationResult(
                ok=all(item.ok for item in health_results),
                checks=health_results,
            )
            return self._with_batch_evidence(result, verification)
        output_name = _output_name(result)
        if output_name is None:
            return _verification_failed(result, "Flink did not return an output reference")
        include_rows = any(
            bool(getattr(check, "requires_row_count", False)) for check in table_checks
        )
        try:
            table = await self._runtime.inspect_table(output_name, include_row_count=include_rows)
        except Exception as error:
            return _verification_failed(result, f"Flink output inspection failed: {error}")
        checks: tuple[CheckResult, ...] = (
            *health_results,
            *(
                sourced_result(check.evaluate(table), source, observation_source="flink output")
                for check, source in table_entries
            ),
        )
        verification = VerificationResult(ok=all(check.ok for check in checks), checks=checks)
        if verification.ok:
            return self._with_batch_evidence(result, verification)
        message = next(
            (check.message for check in checks if not check.ok and check.message),
            "Flink batch verification failed",
        )
        failed = replace(
            result,
            status=ResultStatus.VERIFICATION_FAILED,
            verification=verification,
            failure=Failure(FailureKind.VERIFICATION_FAILED, False, message),
        )
        return self._with_batch_evidence(failed, verification)


class FlinkBatchJob(FlinkJob):
    def __init__(
        self,
        runtime: FlinkRuntime,
        *,
        inputs: Collection[str],
        outputs: Collection[str],
        checks: Sequence[FlinkHealthCheck | MaterializationCheck] = (),
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        super().__init__(
            runtime,
            kind=FlinkJobKind.BATCH,
            inputs=inputs,
            outputs=outputs,
            checks=checks,
            timeout=timeout,
            poll_interval=poll_interval,
        )


class FlinkStreamJob(FlinkJob):
    def __init__(
        self,
        runtime: FlinkRuntime,
        *,
        inputs: Collection[str],
        outputs: Collection[str],
        checks: Sequence[FlinkHealthCheck] = (),
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        super().__init__(
            runtime,
            kind=FlinkJobKind.STREAM,
            inputs=inputs,
            outputs=outputs,
            checks=checks,
            timeout=timeout,
            poll_interval=poll_interval,
        )


def _recorded(result: FlinkResult) -> FlinkResult:
    """Record the run for a Flink job and hand it back on the result."""
    from dataclasses import replace

    from gantry.runs.lifecycle import run_from_evidence
    from gantry.runs.model import OperationKind

    kind = (
        (
            OperationKind.STREAM
            if str(result.execution.handle.metadata.get("mode", "")).lower() == "streaming"
            else OperationKind.BATCH
        )
        if result.execution is not None
        else OperationKind.BATCH
    )
    run = run_from_evidence(
        result.evidence,
        kind=kind,
        engine="flink",
        provider="flink",
        status=result.status,
        verification=result.verification,
    )
    return replace(result, run=run)


def _parse_job(sql: str) -> FlinkJobPlan:
    if not isinstance(sql, str):
        raise TypeError("Flink SQL must be a string")
    if not sql.strip():
        raise ValueError("Flink SQL must not be empty")
    dialect = ConservativeDialect()
    classification = dialect.classify(sql)
    if classification.statement_count != 1:
        _reject(FailureKind.OPERATION_NOT_ALLOWED, "Flink jobs require exactly one SQL statement")
    statement = _LEADING_COMMENTS.sub("", dialect.parse(sql).statements[0])
    match = _INSERT.match(statement)
    if classification.operation is not SQLOperation.INSERT or match is None:
        _reject(
            FailureKind.OPERATION_NOT_ALLOWED,
            "Flink jobs support only INSERT INTO or INSERT OVERWRITE statements",
        )
    output = _normalize_identifier(match.group(2))
    # Inputs are normalised the same way the output is. They were not, so a
    # quoted name arrived at the policy check still wearing its backticks and
    # matched nothing — which made every schema-qualified table unusable, and
    # anything outside `public` is schema-qualified.
    # Inputs are normalised the same way the output is. They were not, so a
    # quoted name reached the policy check still wearing its backticks and
    # matched nothing — which made every schema-qualified table unusable, and
    # anything outside `public` is schema-qualified.
    references = [
        _normalize_identifier(reference.qualified_name) for reference in classification.tables
    ]
    if references and references[0].lower() == output.lower():
        references.pop(0)
    operation = (
        FlinkJobStatement.INSERT_OVERWRITE
        if match.group(1).upper() == "OVERWRITE"
        else FlinkJobStatement.INSERT_INTO
    )
    return FlinkJobPlan(operation, tuple(references), output)


def _normalize_identifier(value: str) -> str:
    """Strip quoting from a possibly-qualified identifier.

    Splitting on "." has to respect the quotes. A JDBC catalog exposes a
    PostgreSQL table as one identifier that *contains* a dot —
    `analytics.orders` — and naive splitting turns it into two broken halves
    that match nothing and no longer resemble the name the user wrote.
    """
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    for character in value:
        if quote is not None:
            if character == quote:
                quote = None
            else:
                current.append(character)
            continue
        if character in {'"', "`"}:
            quote = character
            continue
        if character == ".":
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(character)
    parts.append("".join(current).strip())
    return ".".join(part for part in parts if part)


def _patterns(values: Collection[str], name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise TypeError(f"{name} must be a collection of patterns, not a string")
    result = tuple(values)
    if any(not isinstance(value, str) for value in result):
        raise TypeError(f"{name} must contain only strings")
    if any(not value.strip() for value in result):
        raise ValueError(f"{name} must not contain empty patterns")
    return tuple(value.lower() for value in result)


def _allowed(name: str, patterns: tuple[str, ...]) -> bool:
    normalized = name.lower()
    return any(fnmatchcase(normalized, pattern) for pattern in patterns)


def _reject(kind: FailureKind, message: str) -> NoReturn:
    raise FlinkJobError(Failure(kind, False, message))


def _failed(status: ResultStatus, failure: Failure) -> FlinkResult:
    return FlinkResult(status=status, failure=failure)


def _output_name(result: FlinkResult) -> str | None:
    if not result.outputs:
        return None
    name = result.outputs[0].metadata.get("name")
    return name if isinstance(name, str) else None


def _verification_failed(result: FlinkResult, message: str) -> FlinkResult:
    verification = VerificationResult.failed(message, name="output_inspection")
    return replace(
        result,
        status=ResultStatus.VERIFICATION_FAILED,
        verification=verification,
        failure=Failure(FailureKind.VERIFICATION_FAILED, False, message),
    )


__all__ = [
    "FlinkBatchJob",
    "FlinkJob",
    "FlinkJobError",
    "FlinkJobKind",
    "FlinkJobPlan",
    "FlinkJobStatement",
    "FlinkStreamJob",
]
