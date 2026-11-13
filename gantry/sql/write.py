# SPDX-License-Identifier: Apache-2.0
"""Configured, governed SQL write operation.

`db.query()` reads, and `db.materialize()` builds a new table beside the ones
that exist. Neither changes a row somebody already has. This does: `INSERT`,
`UPDATE` and `DELETE` against tables the application named, one statement at a
time.

A write is the operation where "the agent got it slightly wrong" stops being a
bad answer and becomes missing data, so it is bounded on every side that can
be bounded before anything runs — which tables, which kinds of statement,
whether it is filtered — and on the one side that cannot, how many rows it
turns out to change, by running it in a transaction and undoing it if the
answer is too many.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from hashlib import sha256
from typing import TYPE_CHECKING

from gantry.context import Context
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.failure import Failure, FailureKind
from gantry.result import Result, ResultStatus
from gantry.runs.lifecycle import RunRecorder, stored_proposal
from gantry.runs.model import OperationKind, QueryResultRef, ResourceRef, Run
from gantry.runs.service import gate
from gantry.runs.status import RunStatus
from gantry.sql.affected import DRY_RUN, DRY_RUN_PREVIEW
from gantry.sql.classification import SQLClassification, SQLObjectRef, SQLOperation
from gantry.sql.inspection import write_request
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.tool import Tool

if TYPE_CHECKING:
    from gantry.sql.api import SQLConnection

#: The statements a write operation can be configured to allow, by the word a
#: person would use for them.
STATEMENTS: Mapping[str, SQLOperation] = {
    "insert": SQLOperation.INSERT,
    "update": SQLOperation.UPDATE,
    "delete": SQLOperation.DELETE,
    "merge": SQLOperation.MERGE,
}


@dataclass(frozen=True, slots=True)
class WritePolicy:
    """What a write may change, how, and how much.

    `destinations` must be non-empty: there is no default table to write to.
    `sources` is what the statement may read *besides* the table it changes,
    and is empty by default, so an `INSERT ... SELECT` from somewhere else has
    to be allowed by name. Both take `fnmatch` patterns, lower-cased here.
    `denied_tables` always wins, over both: a table named there is neither
    written nor read, however broad the pattern that would have allowed it.

    `statements` defaults to insert, update and delete. `merge` is accepted
    but has to be asked for: one statement that inserts, updates and deletes
    is three decisions, and should not arrive by default.

    `max_rows_affected` is the ceiling the adapter enforces by rolling back.
    `require_where` refuses an `UPDATE` or `DELETE` with no `WHERE` before it
    runs; the ceiling would catch it on a large table, but on a small one
    "every row" is under the limit and is still not what anyone meant.
    """

    destinations: Collection[str]
    sources: Collection[str] = ()
    denied_tables: Collection[str] = ()
    statements: Collection[str] = ("insert", "update", "delete")
    max_rows_affected: int = 100
    require_where: bool = True
    max_rows: int = 100
    timeout_seconds: float = 30

    def __post_init__(self) -> None:
        for field_name in ("destinations", "sources", "denied_tables", "statements"):
            values = getattr(self, field_name)
            if isinstance(values, str):
                raise TypeError(f"{field_name} must be a collection, not a string")
            if any(not isinstance(value, str) for value in values):
                raise TypeError(f"{field_name} must contain only strings")
            if any(not value.strip() for value in values):
                raise ValueError(f"{field_name} must not contain empty values")
            object.__setattr__(self, field_name, tuple(value.strip().lower() for value in values))
        if not self.destinations:
            raise ValueError("at least one write destination must be allowed")
        if not self.statements:
            raise ValueError("at least one kind of statement must be allowed")
        unknown = sorted(set(self.statements) - set(STATEMENTS))
        if unknown:
            raise ValueError(
                f"unknown statement kind: {', '.join(unknown)} "
                f"(expected any of {', '.join(STATEMENTS)})"
            )
        if not isinstance(self.require_where, bool):
            raise TypeError("require_where must be a boolean")
        for name in ("max_rows_affected", "max_rows"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if isinstance(self.timeout_seconds, bool) or not isinstance(
            self.timeout_seconds, (int, float)
        ):
            raise TypeError("timeout must be numeric")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout must be positive")

    def bounds(self) -> SQLPolicy:
        """The engine-facing bounds this policy puts around the statement."""
        return SQLPolicy(
            read_only=False,
            max_rows=self.max_rows,
            max_rows_affected=self.max_rows_affected,
            timeout_seconds=self.timeout_seconds,
        )


@dataclass(frozen=True, slots=True)
class SQLWrite:
    """A write policy configured once, for direct calls or as an agent tool."""

    _connection: SQLConnection = field(repr=False)
    _policy: WritePolicy = field(repr=False)

    async def __call__(self, sql: str, *, context: Context | None = None) -> Run:
        """Run one governed write, and record the run.

        Refusals come back as a run, not an exception: the statement that was
        not allowed is the one somebody asks about afterwards, and a record of
        it is the answer.
        """
        connection = self._connection
        recorder = RunRecorder(
            kind=OperationKind.WRITE,
            engine="sql",
            provider=connection.provider,
            proposal=sql,
        )
        try:
            classification = connection._dialect.classify(sql)
        except Exception as error:  # pragma: no cover - a dialect that raises is a dialect bug
            return _refuse(
                recorder, FailureKind.VALIDATION_ERROR, f"SQL could not be parsed: {error}"
            )

        refusal = self._refusal(classification)
        if refusal is not None:
            return _refuse(recorder, *refusal)

        request = write_request(
            classification, provider=connection.provider, policy=self._policy.bounds()
        )

        async def run() -> Run:
            return await self._execute(sql, classification, context=context, recorder=recorder)

        return await gate(recorder, policy=connection.policy, request=request, resume=run)

    async def resume(self, run: Run) -> Run:
        """Continue a confirmed run from the store, in a process that never parked it.

        For `gantry.runs.confirm(run_id, resume=write.resume)`. The statement
        is the one recorded on the run. This operation's bounds are applied
        again, as they stand now: a destination removed since the proposal was
        made refuses it, which is what removing it meant. The reusable policy
        is not asked again — the run was admitted, and an admitted run is not
        re-decided.
        """
        sql = stored_proposal(run, OperationKind.WRITE)
        recorder = RunRecorder.adopt(run)
        classification = self._connection._dialect.classify(sql)
        refusal = self._refusal(classification)
        if refusal is not None:
            return _refuse(recorder, *refusal)
        return await self._execute(sql, classification, context=None, recorder=recorder)

    async def dry_run(self, sql: str, *, preview: int = 0) -> dict[str, object]:
        """How many rows the statement would change, found by running it and undoing it.

        `check` answers from the statement; this answers from the data. The
        statement runs inside a transaction that is always rolled back, so
        nothing it does persists — but it does run: locks are taken for its
        duration, triggers fire and are undone with it, and a sequence it
        advances stays advanced. That is why this is a separate call a host
        makes on purpose, and not something `check` does quietly.

        A statement this operation or the policy would refuse is not run at
        all; the answer is then `check`'s, with `would_change` left `None`.
        No run is recorded.

        `preview` asks for up to that many of the changed rows as well, as
        `columns` and `rows` — what a person deciding on the write wants to
        see. Where the engine supports `RETURNING`, a statement without one
        is run with `RETURNING *` added, so the rows are the rows as the write
        leaves them. Where it does not, only the count comes back. The answer
        says which with `previewed`.
        """
        answer = self.check(sql)
        answer["would_change"] = None
        if answer["allowed"] is False:
            return answer
        bounds = self._policy.bounds()
        metadata: dict[str, object] = {DRY_RUN: True}
        run_sql = sql
        returning = self._returning(sql) if preview > 0 else None
        if returning is not None:
            run_sql = returning
            metadata[DRY_RUN_PREVIEW] = preview
        result = await self._connection.execute(
            run_sql, policy=bounds, context=Context(metadata=metadata)
        )
        if result.status is not ResultStatus.ACCEPTED:
            failure = result.failure
            return {
                **answer,
                "error": failure.message if failure is not None else "the dry run failed",
            }
        counted = result.metrics.rows_written
        answer = {
            **answer,
            "would_change": counted,
            "at_least": bool(result.metrics.native.get("at_least", False)),
            "over_limit": counted is not None and counted > self._policy.max_rows_affected,
        }
        if preview > 0:
            shown: InlineRows | None = None
            for output in result.outputs:
                inline = output.metadata.get("inline")
                if isinstance(inline, InlineRows):
                    shown = inline
                    break
            answer["previewed"] = shown is not None
            if shown is not None:
                answer["columns"] = list(shown.columns)
                answer["rows"] = [list(row) for row in shown.rows]
                answer["preview_truncated"] = shown.truncated
        return answer

    def _returning(self, sql: str) -> str | None:
        """The statement as a dry run should run it to see its rows, or `None` if it cannot.

        The rows come from `RETURNING`. A statement that already says it is
        run as written; one that does not gets `RETURNING *`, on an engine that
        supports it, for the statements that take it.
        """
        if not self._connection.capabilities().returning:
            return None
        classification = self._connection._dialect.classify(sql)
        if classification.returning:
            return sql
        if classification.operation not in (
            SQLOperation.INSERT,
            SQLOperation.UPDATE,
            SQLOperation.DELETE,
        ):
            return None
        return sql.rstrip().rstrip(";").rstrip() + " RETURNING *"

    def inspect(self, sql: str) -> SQLClassification:
        """What the statement would change and read, without running it."""
        return self._connection._dialect.classify(sql)

    def check(self, sql: str) -> dict[str, object]:
        """Whether this write would be admitted, and why, without running it.

        The same two decisions the real path makes, in the same order: this
        operation's own bounds, then the connection's reusable policy. Nothing
        reaches the engine and no run is recorded, so an agent can ask before
        it acts — but that also means the one bound only the engine can apply,
        how many rows the statement turns out to change, is not answered here.
        `allowed` is `None` when the bounds admit the statement and no policy
        is attached to say more.
        """
        from gantry.policy.evaluator import evaluate

        connection = self._connection
        classification = self.inspect(sql)
        written = {reference.qualified_name for reference in classification.write_targets}
        answer: dict[str, object] = {
            "operation": "write",
            "reads": [
                reference.qualified_name
                for reference in classification.tables
                if reference.qualified_name not in written
            ],
            "writes": sorted(written),
            "max_rows_affected": self._policy.max_rows_affected,
        }
        refusal = self._refusal(classification)
        if refusal is not None:
            return {
                **answer,
                "allowed": False,
                "because": [refusal[1]],
                "codes": [refusal[0].value],
            }
        if connection.policy is None:
            return {
                **answer,
                "allowed": None,
                "because": ["no policy is attached to this connection"],
            }
        request = write_request(
            classification, provider=connection.provider, policy=self._policy.bounds()
        )
        decision = evaluate(connection.policy, request)
        return {
            **answer,
            "allowed": decision.allowed,
            "because": list(decision.messages()),
            "codes": list(decision.codes),
            "rules": list(decision.matched_rules),
            "confirmation_required": decision.confirmation.required,
        }

    def tool(
        self,
        *,
        name: str = "write_sql",
        description: str | None = None,
    ) -> Tool[Run]:
        """Return the narrow framework-neutral form of this write operation.

        The description says what is allowed, because a model that knows the
        limit before it writes the statement writes one that fits.
        """
        kinds = ", ".join(kind.upper() for kind in self._policy.statements)
        return Tool(
            name=name,
            description=description
            or (
                f"Change rows with one governed SQL statement ({kinds}). At most "
                f"{self._policy.max_rows_affected} rows may be changed; a statement that "
                "would change more is rolled back and changes nothing."
                + (" UPDATE and DELETE need a WHERE clause." if self._policy.require_where else "")
            ),
            input_schema={
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
                "additionalProperties": False,
            },
            _handler=self._invoke_tool,
        )

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> Run:
        unexpected = set(arguments) - {"sql"}
        if unexpected:
            raise ValueError(f"unexpected write tool arguments: {', '.join(sorted(unexpected))}")
        sql = arguments.get("sql")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        return await self(sql)

    def _refusal(self, classification: SQLClassification) -> tuple[FailureKind, str] | None:
        """Why this operation will not run the statement, or `None` if it will.

        Everything here is decided from the statement alone, before the
        reusable policy is asked and long before the engine is. The order is
        the order a person would want the answers in: is it one statement, is
        it a write at all, is it a kind of write this allows, does it touch
        only what was named.
        """
        policy = self._policy
        operation = classification.operation
        if operation is SQLOperation.MULTI_STATEMENT:
            return (
                FailureKind.OPERATION_NOT_ALLOWED,
                "a write is one statement; several were submitted together",
            )
        if operation is SQLOperation.UNKNOWN:
            reason = classification.read_only_reason
            return (
                FailureKind.VALIDATION_ERROR,
                "the statement could not be classified safely" + (f": {reason}" if reason else ""),
            )
        allowed = {STATEMENTS[kind] for kind in policy.statements}
        if operation not in STATEMENTS.values():
            return (
                FailureKind.OPERATION_NOT_ALLOWED,
                f"{operation.value} is not a row write; this operation runs "
                f"{_listed(policy.statements)}",
            )
        if operation not in allowed:
            return (
                FailureKind.OPERATION_NOT_ALLOWED,
                f"{operation.value} is not allowed here; this operation runs "
                f"{_listed(policy.statements)}",
            )
        targets = classification.write_targets
        if len(targets) != 1:
            return (
                FailureKind.OPERATION_NOT_ALLOWED,
                "a write changes one table; this statement changes "
                f"{', '.join(target.qualified_name for target in targets)}",
            )
        target = targets[0]
        denied = [
            reference.qualified_name
            for reference in classification.tables
            if _matches(reference, policy.denied_tables)
        ]
        if denied:
            return (
                FailureKind.DESTINATION_NOT_ALLOWED
                if target.qualified_name in denied
                else FailureKind.SOURCE_NOT_ALLOWED,
                f"{', '.join(denied)} is denied to this operation",
            )
        if not _matches(target, policy.destinations):
            return (
                FailureKind.DESTINATION_NOT_ALLOWED,
                f"writing to {target.qualified_name} is not allowed",
            )
        outside = [
            reference.qualified_name
            for reference in classification.tables
            if reference != target and not _matches(reference, policy.sources)
        ]
        if outside:
            return (
                FailureKind.SOURCE_NOT_ALLOWED,
                f"reading {', '.join(outside)} is not allowed in a write",
            )
        if policy.require_where and classification.filtered is False:
            return (
                FailureKind.OPERATION_NOT_ALLOWED,
                f"{operation.value} without a WHERE clause would change every row of "
                f"{target.qualified_name}; add a WHERE clause",
            )
        return None

    async def _execute(
        self,
        sql: str,
        classification: SQLClassification,
        *,
        context: Context | None,
        recorder: RunRecorder,
    ) -> Run:
        """Everything after admission: run it bounded, then record what it did.

        Held back by the gate when a rule asks for confirmation, and run
        unchanged when the host confirms — the same statement, the same run.
        """
        connection = self._connection
        bounds = self._policy.bounds()
        result = await connection.execute(sql, policy=bounds, context=context)
        recorder.running(result.handle)
        if result.status is ResultStatus.REJECTED:
            # Refused at validation: a bound the adapter cannot enforce, or the
            # engine rejecting the statement before running it.
            failure = result.failure
            return recorder.rejected(
                (failure.message,) if failure is not None else ("the statement was refused",),
                status=RunStatus.POLICY_REJECTED,
            ).with_failure(failure)
        if result.status is not ResultStatus.ACCEPTED:
            # Includes a statement over its row limit, which was rolled back.
            return recorder.execution_failed(result.failure)

        affected = result.metrics.rows_written
        if affected is None:
            # An adapter that declared the bound and reported no count has not
            # applied it. Accepting would record a limit nobody checked.
            failure = Failure(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                False,
                "the adapter did not report how many rows the statement changed",
            )
            return recorder.execution_failed(failure)

        inline = _returned(result, self._policy.max_rows)
        target = classification.write_targets[0]
        recorder.verifying()
        return recorder.decided(
            verification=None,
            evidence=_evidence(sql, result, affected, inline),
            outputs=(ResourceRef(system=connection.provider, resource=target.qualified_name),),
            result_ref=QueryResultRef(
                rows=0 if inline is None else len(inline.rows),
                inline=inline is not None,
                truncated=inline is not None and inline.truncated,
                affected=affected,
            ),
            inline=inline,
            metrics={"rows_written": affected},
        )


def _refuse(recorder: RunRecorder, kind: FailureKind, message: str) -> Run:
    return recorder.rejected((message,), status=RunStatus.POLICY_REJECTED).with_failure(
        Failure(kind, False, message)
    )


def _listed(kinds: Collection[str]) -> str:
    names = [kind.upper() for kind in kinds]
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"


def _matches(reference: SQLObjectRef, patterns: Collection[str]) -> bool:
    """Whether a table matches any pattern, by its qualified name.

    A three-part name also matches on its last two, so a pattern written as
    `schema.table` still applies when the statement names the catalog.
    """
    qualified = reference.qualified_name.lower()
    candidates = [qualified]
    if reference.catalog is not None and reference.schema is not None:
        candidates.append(f"{reference.schema}.{reference.name}".lower())
    return any(fnmatchcase(candidate, pattern) for pattern in patterns for candidate in candidates)


def _returned(result: Result, max_rows: int) -> InlineRows | None:
    for output in result.outputs:
        candidate = output.metadata.get("inline")
        if isinstance(candidate, InlineRows):
            return InlineRows(
                candidate.columns,
                candidate.rows[:max_rows],
                truncated=candidate.truncated or len(candidate.rows) > max_rows,
            )
    return None


def _evidence(
    sql: str, result: Result, affected: int, inline: InlineRows | None
) -> EvidenceBundle | None:
    """A write's evidence, in the shape a query and a materialization emit."""
    handle = result.handle
    if handle is None:
        return None
    execution = result.execution
    observations = [
        Observation("rows_affected", affected, ObservationSource.ENGINE, unit="rows"),
    ]
    if execution is not None:
        observations.append(
            Observation("execution_state", execution.state.value, ObservationSource.ENGINE)
        )
    if result.metrics.runtime_seconds is not None:
        observations.append(
            Observation(
                "runtime_seconds",
                result.metrics.runtime_seconds,
                ObservationSource.ENGINE,
                unit="seconds",
            )
        )
    if inline is not None:
        observations.append(
            Observation("rows_returned", len(inline.rows), ObservationSource.OUTPUT, unit="rows")
        )
        observations.append(Observation("truncated", inline.truncated, ObservationSource.OUTPUT))
    digest = sha256(sql.encode()).hexdigest()
    return EvidenceBundle(
        run_id=handle.gantry_id,
        engine=handle.engine,
        operation="write",
        decision=ResultStatus.ACCEPTED.value,
        native_execution_id=handle.native_id,
        proposal_hash=digest,
        outputs=tuple(output.uri for output in result.outputs),
        started_at=None if execution is None else (execution.started_at or handle.submitted_at),
        finished_at=None if execution is None else execution.updated_at,
        execution={
            "status": "UNKNOWN" if execution is None else execution.state.value,
            "engine": handle.engine,
            "target": handle.target,
        },
        proposal={"hash": digest, "agent_verification": []},
        observations=tuple(observations),
        checks=(),
    )


__all__ = ["STATEMENTS", "SQLWrite", "WritePolicy"]
