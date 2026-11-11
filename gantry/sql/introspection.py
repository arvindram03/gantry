# SPDX-License-Identifier: Apache-2.0
"""The tools an agent needs before it writes a statement, and after it sends one.

An agent handed only `query` works blind: it guesses table names, guesses
columns, and finds out it was wrong by being refused or by reading an error it
cannot act on. These are the two questions it should be able to ask first —
what is here, and would this be allowed — plus the one it needs afterwards when
a run is held for confirmation.

None of them reach the database any way the query path does not. `describe` is
the adapter's own schema listing, and `check` is the policy evaluator answering
about a statement nobody ran.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from gantry.policy.evaluator import evaluate
from gantry.runs.status import RunStatus
from gantry.sql.inspection import query_request
from gantry.sql.policy import SQLPolicy
from gantry.sql.registry import resolve_dialect, resolve_provider
from gantry.tool import Tool


def describe_tool(
    connection: Any,
    *,
    name: str = "describe_schema",
    description: str = (
        "List the schemas, tables and columns on this connection. "
        "Call this before writing SQL rather than guessing names."
    ),
) -> Tool[dict[str, object]]:
    """A tool that answers "what is here".

    An agent that can read the schema stops guessing table names, which is
    most of what it is refused for. What it may actually read is a narrower
    question and the policy answers it: `check` says whether a statement would
    be allowed, and the query path refuses one that would not be.
    """

    async def handler(_: Mapping[str, object]) -> dict[str, object]:
        schema = await connection.describe()
        return {
            "schemas": list(schema.schemas),
            "tables": [
                {
                    "name": f"{table.schema}.{table.name}" if table.schema else table.name,
                    "kind": table.kind,
                    "columns": [
                        {"name": column.name, "type": column.type, "nullable": column.nullable}
                        for column in table.columns
                    ],
                    "primary_key": list(table.primary_key),
                }
                for table in schema.tables
            ],
        }

    return Tool(
        name=name,
        description=description,
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        _handler=handler,
    )


def check_tool(
    connection: Any,
    *,
    policy: SQLPolicy | None = None,
    name: str = "check_query",
    description: str = (
        "Say what a statement would read and write, and whether it would be "
        "allowed, without running it."
    ),
) -> Tool[dict[str, object]]:
    """A tool that answers "would this be allowed", without running anything.

    The same classifier and the same evaluator the real path uses, so what it
    says is what would happen. An agent that can ask before acting stops
    guessing, and a refusal it avoids is a round trip nobody spends.
    """
    bounds = policy or SQLPolicy()

    async def handler(arguments: Mapping[str, object]) -> dict[str, object]:
        sql = str(arguments.get("sql", ""))
        request = query_request(
            sql,
            dialect=resolve_dialect(resolve_provider(connection.provider).dialect),
            provider=connection.provider,
            policy=bounds,
        )
        reads = [ref.resource for ref in request.inputs]
        writes = [ref.resource for ref in request.outputs]
        if connection.policy is None:
            # Nothing reusable to evaluate against: say what it touches and
            # decline to guess at the verdict rather than implying approval.
            return {
                "reads": reads,
                "writes": writes,
                "allowed": None,
                "because": ["no policy is attached to this connection"],
            }
        decision = evaluate(connection.policy, request)
        return {
            "reads": reads,
            "writes": writes,
            "allowed": decision.allowed,
            "because": list(decision.messages()),
            "codes": list(decision.codes),
            "rules": list(decision.matched_rules),
        }

    return Tool(
        name=name,
        description=description,
        input_schema={
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"],
            "additionalProperties": False,
        },
        _handler=handler,
    )


def status_tool(
    *,
    name: str = "run_status",
    description: str = (
        "Look up a run by id. Use it when a run was held for confirmation, to "
        "find out whether a person approved it."
    ),
) -> Tool[dict[str, object]]:
    """A tool that answers "what happened to my run".

    Without it a parked run is a dead end: the agent is told its work is
    waiting for a person and has no way to learn that the person answered.
    """

    async def handler(arguments: Mapping[str, object]) -> dict[str, object]:
        from gantry import runs

        run_id = str(arguments.get("run_id", ""))
        run = runs.get(run_id)
        if run is None:
            return {"status": "UNKNOWN", "error": f"no run with id {run_id}"}
        answer: dict[str, object] = {
            "run_id": run.id,
            "status": run.status.value,
            "ok": run.ok,
        }
        if run.status is RunStatus.AWAITING_CONFIRMATION and run.confirmation is not None:
            answer["waiting_for"] = [reason.message for reason in run.confirmation.reasons]
        if run.failure is not None:
            answer["because"] = run.failure.message
        if run.result_ref is not None:
            answer["rows"] = run.result_ref.rows
        return answer

    return Tool(
        name=name,
        description=description,
        input_schema={
            "type": "object",
            "properties": {"run_id": {"type": "string"}},
            "required": ["run_id"],
            "additionalProperties": False,
        },
        _handler=handler,
    )


__all__ = ["check_tool", "describe_tool", "status_tool"]
