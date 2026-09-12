# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from gantry.context import Context
from gantry.failure import Failure, FailureKind
from gantry.nosql.pipeline import Pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.result import NoSQLResult
from gantry.nosql.verify import DocumentCheck
from gantry.result import ResultStatus
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


class _QueryConnection(Protocol):
    async def _query(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy,
        context: Context | None = None,
        trusted_verify: Sequence[Verifier | DocumentCheck] = (),
        agent_verify: Sequence[DocumentCheck] = (),
    ) -> NoSQLResult: ...


@dataclass(frozen=True, slots=True)
class NoSQLQuery:
    _connection: _QueryConnection
    _policy: NoSQLPolicy
    _verify: Sequence[Verifier | DocumentCheck]
    _agent_capabilities = frozenset({"not_empty", "document_count", "required_fields"})

    async def __call__(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        context: Context | None = None,
        verify: Sequence[DocumentCheck] = (),
    ) -> NoSQLResult:
        try:
            agent_checks = validate_agent_checks(verify, capabilities=self._agent_capabilities)
            trusted_checks = tuple(
                check for check in self._verify if not isinstance(check, Verifier)
            )
            detect_conflicts(trusted_checks, agent_checks)
        except VerificationUnsupported as error:
            return _rejected(error, unsupported=True)
        except (VerificationConflict, VerificationInputError) as error:
            return _rejected(error, conflict=isinstance(error, VerificationConflict))
        return await self._connection._query(
            collection,
            pipeline,
            policy=self._policy,
            context=context,
            trusted_verify=self._verify,
            agent_verify=agent_checks,  # type: ignore[arg-type]
        )

    @property
    def input_schema(self) -> Mapping[str, object]:
        return {
            "type": "object",
            "properties": {
                "collection": {"type": "string"},
                "pipeline": {
                    "type": ["object", "array"],
                    "description": "A MongoDB filter document or aggregation pipeline stages",
                },
                "verify": verification_schema(self._agent_capabilities),
            },
            "required": ["collection", "pipeline"],
            "additionalProperties": False,
        }

    def tool(
        self,
        *,
        name: str = "query_nosql",
        description: str = "Run a governed, read-only MongoDB query or aggregation pipeline.",
    ) -> Tool[NoSQLResult]:
        return Tool(
            name=name,
            description=description,
            input_schema=self.input_schema,
            _handler=self._invoke_tool,
        )

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> NoSQLResult:
        unexpected = set(arguments) - {"collection", "pipeline", "verify"}
        if unexpected:
            raise ValueError(f"unexpected query tool arguments: {', '.join(sorted(unexpected))}")
        collection = arguments.get("collection")
        if not isinstance(collection, str):
            raise TypeError("collection must be a string")
        pipeline = arguments.get("pipeline")
        if not (
            isinstance(pipeline, (Mapping, Sequence)) and not isinstance(pipeline, (str, bytes))
        ):
            raise TypeError("pipeline must be a mapping or a sequence of stage mappings")
        try:
            checks = parse_agent_checks(
                arguments.get("verify"), capabilities=self._agent_capabilities
            )
        except VerificationUnsupported as error:
            return _rejected(error, unsupported=True)
        except VerificationInputError as error:
            return _rejected(error)
        return await self(collection, pipeline, verify=checks)  # type: ignore[arg-type]


def _rejected(
    error: Exception, *, unsupported: bool = False, conflict: bool = False
) -> NoSQLResult:
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
    return NoSQLResult(status, failure=Failure(kind, False, str(error)))
