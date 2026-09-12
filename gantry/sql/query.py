# SPDX-License-Identifier: Apache-2.0
"""Configured, governed SQL query operation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gantry.context import Context
from gantry.failure import Failure, FailureKind
from gantry.result import ResultStatus
from gantry.sql.policy import SQLPolicy
from gantry.sql.result import SQLResult
from gantry.tool import Tool
from gantry.verifier import Verifier
from gantry.verify import (
    VerificationConflict,
    VerificationInputError,
    VerificationUnsupported,
    detect_conflicts,
    parse_agent_checks,
    validate_agent_checks,
    verification_schema,
)

if TYPE_CHECKING:
    from gantry.sql.api import SQLConnection
    from gantry.verify import MaterializationCheck


@dataclass(frozen=True, slots=True)
class SQLQuery:
    """A query policy configured once for direct calls or agent tools."""

    _connection: SQLConnection = field(repr=False)
    _policy: SQLPolicy = field(repr=False)
    _verify: tuple[Verifier | MaterializationCheck, ...] = field(default=(), repr=False)

    _agent_capabilities = frozenset({"not_empty", "row_count", "required_columns", "null_rate"})

    async def __call__(
        self,
        sql: str,
        *,
        context: Context | None = None,
        verify: Sequence[MaterializationCheck] = (),
    ) -> SQLResult:
        try:
            agent_checks = validate_agent_checks(
                tuple(verify), capabilities=self._agent_capabilities
            )
            trusted_checks = tuple(item for item in self._verify if not isinstance(item, Verifier))
            detect_conflicts(trusted_checks, agent_checks)
        except VerificationUnsupported as error:
            return _verification_rejection(error, unsupported=True)
        except (VerificationConflict, VerificationInputError) as error:
            return _verification_rejection(error, conflict=isinstance(error, VerificationConflict))
        return await self._connection._query(
            sql,
            policy=self._policy,
            context=context,
            trusted_verify=self._verify,
            agent_verify=agent_checks,  # type: ignore[arg-type]
        )

    def tool(
        self,
        *,
        name: str = "query_sql",
        description: str = "Run a governed read-only SQL query.",
    ) -> Tool[SQLResult]:
        """Return the narrow framework-neutral form of this query operation."""

        if not self._policy.read_only:
            raise ValueError(
                "query.tool() requires read_only=True; use db.materialize() for agent writes"
            )
        return Tool(
            name=name,
            description=description,
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "verify": verification_schema(self._agent_capabilities),
                },
                "required": ["sql"],
                "additionalProperties": False,
            },
            _handler=self._invoke_tool,
        )

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> SQLResult:
        _require_query_arguments(arguments)
        sql = arguments.get("sql")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        try:
            checks = parse_agent_checks(
                arguments.get("verify"), capabilities=self._agent_capabilities
            )
        except VerificationUnsupported as error:
            return _verification_rejection(error, unsupported=True)
        except VerificationInputError as error:
            return _verification_rejection(error)
        return await self(sql, verify=checks)  # type: ignore[arg-type]


def _require_query_arguments(arguments: Mapping[str, object]) -> None:
    unexpected = set(arguments) - {"sql", "verify"}
    if unexpected:
        names = ", ".join(sorted(unexpected))
        raise ValueError(f"unexpected query tool arguments: {names}")


def _verification_rejection(
    error: Exception, *, unsupported: bool = False, conflict: bool = False
) -> SQLResult:
    status = (
        ResultStatus.VERIFICATION_UNSUPPORTED
        if unsupported
        else ResultStatus.VERIFICATION_CONFLICT
        if conflict
        else ResultStatus.REJECTED
    )
    kind = (
        FailureKind.VERIFICATION_UNSUPPORTED
        if unsupported
        else FailureKind.VERIFICATION_CONFLICT
        if conflict
        else FailureKind.VALIDATION_ERROR
    )
    return SQLResult(status, failure=Failure(kind, False, str(error)))
