# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from gantry.context import Context
from gantry.failure import Failure, FailureKind
from gantry.nosql.pipeline import Pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.verify import DocumentCheck
from gantry.runs.model import Run
from gantry.runs.status import RunStatus
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
    ) -> Run: ...


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
    ) -> Run:
        try:
            agent_checks = validate_agent_checks(verify, capabilities=self._agent_capabilities)
            trusted_checks = tuple(
                check for check in self._verify if not isinstance(check, Verifier)
            )
            detect_conflicts(trusted_checks, agent_checks)
        except VerificationUnsupported as error:
            return _refused(self._connection, error, RunStatus.VERIFICATION_UNSUPPORTED)
        except VerificationConflict as error:
            return _refused(self._connection, error, RunStatus.VERIFICATION_CONFLICT)
        except VerificationInputError as error:
            return _refused(self._connection, error, RunStatus.POLICY_REJECTED)
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
    ) -> Tool[Run]:
        return Tool(
            name=name,
            description=description,
            input_schema=self.input_schema,
            _handler=self._invoke_tool,
        )

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> Run:
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
        except (VerificationUnsupported, VerificationInputError) as error:
            return _refused(
                self._connection,
                error,
                RunStatus.VERIFICATION_UNSUPPORTED
                if isinstance(error, VerificationUnsupported)
                else RunStatus.POLICY_REJECTED,
            )
        return await self(collection, pipeline, verify=checks)  # type: ignore[arg-type]


def _refused(connection: object, error: Exception, status: RunStatus) -> Run:
    """Record a run for a proposal refused at the tool boundary.

    A tool response carries a run id, so an agent told "rejected" has
    something to refer to.
    """
    from gantry.runs.lifecycle import RunRecorder
    from gantry.runs.model import OperationKind

    recorder = RunRecorder(
        kind=OperationKind.QUERY,
        engine="mongodb",
        provider=str(getattr(connection, "provider", "mongodb")),
    )
    kind = (
        FailureKind.VERIFICATION_UNSUPPORTED
        if status is RunStatus.VERIFICATION_UNSUPPORTED
        else FailureKind.VALIDATION_ERROR
    )
    return recorder.rejected((str(error),), status=status).with_failure(
        Failure(kind, False, str(error))
    )
