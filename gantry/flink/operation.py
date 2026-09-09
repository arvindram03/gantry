# SPDX-License-Identifier: Apache-2.0
"""Governed batch and stream operations backed by the shared Flink runtime."""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from fnmatch import fnmatchcase
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
from gantry.verifier import CheckResult, VerificationResult
from gantry.verify import MaterializationCheck

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
        self._validate_checks()

    @property
    def kind(self) -> FlinkJobKind:
        return self._kind

    @property
    def input_schema(self) -> Mapping[str, object]:
        return {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
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

    async def submit(self, sql: str, *, context: Context | None = None) -> ExecutionHandle:
        plan = self.inspect(sql)
        self._admit(plan)
        try:
            return await self._runtime.submit(self._artifact(sql, plan), context=context)
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
        result = await self._runtime.wait(
            handle,
            checks=self._health_checks,
            poll_interval_seconds=poll_interval,
            timeout_seconds=timeout,
        )
        if self._kind is FlinkJobKind.BATCH and result.ok:
            return await self._verify_batch(result)
        return result

    async def health(self, handle: ExecutionHandle) -> StreamingHealth:
        if self._kind is not FlinkJobKind.STREAM:
            raise AttributeError("health is available only for streaming jobs")
        return await self._runtime.health(handle, checks=self._health_checks)

    async def __call__(self, sql: str, *, context: Context | None = None) -> FlinkResult:
        try:
            handle = await self.submit(sql, context=context)
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
        unexpected = set(arguments) - {"sql"}
        if unexpected:
            names = ", ".join(sorted(unexpected))
            raise ValueError(f"unexpected Flink job tool arguments: {names}")
        sql = arguments.get("sql")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        return await self(sql)

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

    async def _verify_batch(self, result: FlinkResult) -> FlinkResult:
        table_checks = self._table_checks
        health_results: tuple[CheckResult, ...] = ()
        if result.verification is not None:
            health_results = result.verification.checks
        if not table_checks:
            verification = VerificationResult(
                ok=all(item.ok for item in health_results),
                checks=health_results,
            )
            return replace(result, verification=verification)
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
            *(check.evaluate(table) for check in table_checks),
        )
        verification = VerificationResult(ok=all(check.ok for check in checks), checks=checks)
        if verification.ok:
            return replace(result, verification=verification)
        message = next(
            (check.message for check in checks if not check.ok and check.message),
            "Flink batch verification failed",
        )
        return replace(
            result,
            status=ResultStatus.VERIFICATION_FAILED,
            verification=verification,
            failure=Failure(FailureKind.VERIFICATION_FAILED, False, message),
        )


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
