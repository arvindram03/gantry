# SPDX-License-Identifier: Apache-2.0
"""Configured, governed SQL query operation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gantry.context import Context
from gantry.failure import Failure, FailureKind
from gantry.runs.lifecycle import RunRecorder
from gantry.runs.model import OperationKind, Run
from gantry.runs.status import RunStatus
from gantry.sql.policy import SQLPolicy
from gantry.tool import Tool
from gantry.verifier import Verifier
from gantry.verify import (
    VerificationInputError,
    VerificationUnsupported,
    parse_agent_checks,
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
    ) -> Run:
        return await self._connection._query(
            sql,
            policy=self._policy,
            agent_capabilities=self._agent_capabilities,
            context=context,
            trusted_verify=self._verify,
            agent_verify=tuple(verify),
        )

    def tool(
        self,
        *,
        name: str = "query_sql",
        description: str = "Run a governed read-only SQL query.",
    ) -> Tool[Run]:
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

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> Run:
        _require_query_arguments(arguments)
        sql = arguments.get("sql")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        try:
            checks = parse_agent_checks(
                arguments.get("verify"), capabilities=self._agent_capabilities
            )
        except VerificationUnsupported as error:
            return self._refused(sql, error, RunStatus.VERIFICATION_UNSUPPORTED)
        except VerificationInputError as error:
            return self._refused(sql, error, RunStatus.POLICY_REJECTED)
        return await self(sql, verify=checks)  # type: ignore[arg-type]

    def _refused(self, sql: str, error: Exception, status: RunStatus) -> Run:
        """Record a run for a proposal refused before admission.

        A tool response carries a run id, so a malformed proposal has to get one
        too — an agent that is told "rejected" with nothing to refer to cannot
        be asked about it later.
        """
        recorder = RunRecorder(
            kind=OperationKind.QUERY,
            engine="sql",
            provider=self._connection.provider,
            proposal=sql,
        )
        kind = (
            FailureKind.VERIFICATION_UNSUPPORTED
            if status is RunStatus.VERIFICATION_UNSUPPORTED
            else FailureKind.VALIDATION_ERROR
        )
        return recorder.rejected((str(error),), status=status).with_failure(
            Failure(kind, False, str(error))
        )


def _require_query_arguments(arguments: Mapping[str, object]) -> None:
    unexpected = set(arguments) - {"sql", "verify"}
    if unexpected:
        names = ", ".join(sorted(unexpected))
        raise ValueError(f"unexpected query tool arguments: {names}")
