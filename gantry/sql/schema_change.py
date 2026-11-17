# SPDX-License-Identifier: Apache-2.0
"""Governed schema changes: creating and altering tables, indexes, views, functions…

`db.schema_change(schemas=[...])` is the operation a migration builds a schema
with. It takes one `SchemaStep` at a time: the statement, and what it changes,
named. The decision is made on the name, because a parser cannot reliably say
what a `CREATE TRIGGER` or a function body changes; the statement is still
read, and refused unless it is one schema statement whose every table the
parser can name lies within the schemas allowed.

Each step is a run, `OperationKind.SCHEMA`, decided by policy (a rule allows
it with `gantry.allow.schema(destinations=["app.*"])`) and run in its own
transaction on engines whose schema changes are transactional. A step a rule
would hold for a person is refused instead: steps are applied in order as
one plan, and a plan cannot wait half built. Approve the plan, then allow its
steps by actor.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING

from gantry.actor import current_actor, current_environment
from gantry.context import Context
from gantry.failure import Failure, FailureKind
from gantry.policy.request import PolicyRequest
from gantry.result import ResultStatus
from gantry.runs.lifecycle import RunRecorder
from gantry.runs.model import OperationKind, ResourceRef, Run
from gantry.runs.service import gate
from gantry.runs.status import RunStatus
from gantry.sql.classification import SQLOperation
from gantry.sql.policy import SQLPolicy
from gantry.sql.snapshot import ObjectKind

if TYPE_CHECKING:
    from gantry.sql.api import SQLConnection

#: The context key that tells an adapter a statement is a schema change, so it
#: validates it by running it in a transaction it rolls back.
SCHEMA_CHANGE = "gantry.sql.schema_change"


@dataclass(frozen=True, slots=True)
class SchemaStep:
    """One schema statement, and the object it creates or changes, named."""

    kind: ObjectKind
    schema: str
    """The schema the object lives in; for an extension, the schema it is installed in."""
    name: str
    sql: str
    table: str | None = None
    """The table it belongs to, for a constraint, index, trigger or policy."""

    @property
    def target(self) -> str:
        """What it changes, as policy names it: `schema.table`, or `schema.name`."""
        if self.kind is ObjectKind.EXTENSION:
            return f"extension:{self.name}"
        if self.kind is ObjectKind.SCHEMA:
            return f"{self.name}.*"
        return f"{self.schema}.{self.table or self.name}"


@dataclass(frozen=True, slots=True)
class SchemaChangePolicy:
    """Where schema changes may land, and which extensions may be created."""

    schemas: Collection[str]
    extensions: Collection[str] = field(default_factory=tuple)
    timeout_seconds: float = 60

    def __post_init__(self) -> None:
        for name in ("schemas", "extensions"):
            values = getattr(self, name)
            if isinstance(values, str):
                raise TypeError(f"{name} must be a collection, not a string")
            object.__setattr__(self, name, tuple(value.strip().lower() for value in values))
        if not self.schemas:
            raise ValueError("at least one schema must be allowed")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout must be positive")


@dataclass(frozen=True, slots=True)
class SQLSchemaChange:
    """A schema-change policy configured once, applied a step at a time."""

    _connection: SQLConnection = field(repr=False)
    _policy: SchemaChangePolicy = field(repr=False)

    async def __call__(self, step: SchemaStep, *, context: Context | None = None) -> Run:
        """Apply one step, and record the run. Refusals come back as a run."""
        connection = self._connection
        recorder = RunRecorder(
            kind=OperationKind.SCHEMA,
            engine="sql",
            provider=connection.provider,
            proposal=step.sql,
        )
        refusal = self._refusal(step)
        if refusal is not None:
            return _refuse(recorder, *refusal)
        request = PolicyRequest(
            actor=current_actor(),
            operation=OperationKind.SCHEMA,
            engine=connection.provider,
            inputs=(),
            outputs=(ResourceRef(system=connection.provider, resource=step.target),),
            environment=current_environment(),
            constraints={},
            unresolved=(),
        )
        if connection.policy is not None:
            from gantry.policy.evaluator import evaluate

            decision = evaluate(connection.policy, request)
            if decision.allowed and decision.confirmation.required:
                return _refuse(
                    recorder,
                    FailureKind.OPERATION_NOT_ALLOWED,
                    f"a rule asks a person to confirm schema changes to {step.target}, and a "
                    "step of a plan cannot wait half built; allow these changes without asking",
                )

        async def run() -> Run:
            return await self._execute(step, context=context, recorder=recorder)

        return await gate(recorder, policy=connection.policy, request=request, resume=run)

    def _refusal(self, step: SchemaStep) -> tuple[FailureKind, str] | None:
        connection = self._connection
        policy = self._policy
        if not connection.capabilities().schema_changes:
            return (
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                f"{connection.provider} cannot apply schema changes here",
            )
        if step.kind is ObjectKind.EXTENSION:
            if step.name.lower() not in policy.extensions:
                return (
                    FailureKind.DESTINATION_NOT_ALLOWED,
                    f"creating the extension {step.name} is not allowed",
                )
        elif step.schema.lower() not in policy.schemas:
            return (
                FailureKind.DESTINATION_NOT_ALLOWED,
                f"changing {step.target} is not allowed: {step.schema} is not a schema this "
                "operation may change",
            )
        classification = connection._dialect.classify(step.sql)
        if classification.operation is not SQLOperation.DDL:
            return (
                FailureKind.OPERATION_NOT_ALLOWED,
                f"a schema step is one schema statement; this is {classification.operation.value}",
            )
        outside = [
            reference.qualified_name
            for reference in classification.write_targets
            if reference.schema is not None and not self._allowed(reference.schema)
        ]
        if outside:
            return (
                FailureKind.DESTINATION_NOT_ALLOWED,
                f"the statement changes {', '.join(outside)}, outside the schemas allowed",
            )
        return None

    def _allowed(self, schema: str) -> bool:
        return any(fnmatchcase(schema.lower(), pattern) for pattern in self._policy.schemas)

    async def _execute(
        self, step: SchemaStep, *, context: Context | None, recorder: RunRecorder
    ) -> Run:
        connection = self._connection
        bounds = SQLPolicy(
            read_only=False, max_rows=1, timeout_seconds=self._policy.timeout_seconds
        )
        metadata = {**(context.metadata if context else {}), SCHEMA_CHANGE: True}
        result = await connection.execute(
            step.sql, policy=bounds, context=Context(metadata=metadata)
        )
        if result.status is ResultStatus.REJECTED:
            failure = result.failure
            return recorder.rejected(
                (failure.message,) if failure is not None else ("the statement was refused",),
                status=RunStatus.POLICY_REJECTED,
            ).with_failure(failure)
        recorder.running(result.handle)
        if result.status is not ResultStatus.ACCEPTED:
            return recorder.execution_failed(result.failure)
        recorder.verifying()
        return recorder.decided(
            verification=None,
            evidence=None,
            outputs=(ResourceRef(system=connection.provider, resource=step.target),),
            result_ref=None,
            inline=None,
            metrics={},
        )


def _refuse(recorder: RunRecorder, kind: FailureKind, message: str) -> Run:
    return recorder.rejected((message,), status=RunStatus.POLICY_REJECTED).with_failure(
        Failure(kind, False, message)
    )


__all__ = ["SCHEMA_CHANGE", "SQLSchemaChange", "SchemaChangePolicy", "SchemaStep"]
