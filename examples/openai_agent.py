# SPDX-License-Identifier: Apache-2.0
"""A real OpenAI agent writing SQL against a governed warehouse.

Every other example in this directory hands Gantry the SQL a model *might*
write. This one lets a model actually write it, so the interesting part is not
the happy path — it is what the agent does when the governance layer says no.

    export OPENAI_API_KEY=sk-...
    psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
    python examples/openai_agent.py

Needs `pip install "data-gantry[postgres]" openai`.

The shape worth copying is three functions long:

- `as_openai_tool` turns a governed operation into an OpenAI function
  declaration. It is five lines, because `tool.input_schema` is already JSON
  Schema — there is no adapter to install and nothing to keep in sync.
- `tool_result` decides what the model reads back. A refusal is returned as a
  *result*, not raised: `POLICY_REJECTED` is information the model can act on,
  and an exception is not something it can reason about.
- `converse` is the ordinary tool-calling loop. Nothing in it knows about
  policy, because nothing in it needs to.

What the model cannot do is more interesting than what it can. It sees one
function with one argument. It cannot raise the row cap, reach another schema,
read the connection string, or turn the read into a write — not because the
prompt asks it not to, but because none of those are things it can say.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import gantry

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
LOCAL_URL = "postgresql://gantry:gantry@localhost:5432/gantry"

# Two questions, and the second one is the point of the file. A finance
# analyst asks both without thinking twice; only one of them is answerable.
QUESTIONS = [
    "Which region brought in the most paid revenue, and how much? One row per region.",
    "Great — now give me the email addresses of the customers in that region "
    "so I can send them a thank-you note.",
]


# ------------------------------------------------------------------ the tool


def build_tool(db: gantry.sql.SQLConnection) -> gantry.Tool[gantry.Run]:
    """Everything restrictive, decided once, by code that holds the credential."""
    return db.query(
        read_only=True,
        schemas=("analytics",),
        # The table the second question needs. It exists, `describe()` can see
        # it, and the tool cannot read it.
        denied_tables=("analytics_pii.customer_contacts",),
        max_rows=50,
        timeout=15,
    ).tool()


def as_openai_tool(tool: gantry.Tool[gantry.Run]) -> dict[str, Any]:
    """A governed operation as an OpenAI function declaration.

    Deliberately not strict-mode. The `verify` argument is a union of check
    shapes, expressed with `oneOf`, which `strict: true` does not accept —
    and dropping it to satisfy the schema would quietly remove the agent's
    ability to commit to what its own query should return.
    """
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": dict(tool.input_schema),
        },
    }


def tool_result(run: gantry.Run) -> str:
    """What the model reads back. A refusal is a result, not an exception.

    The run id goes in deliberately. It costs a few tokens and it means the
    model can quote an identifier that an operator can look up afterwards —
    `gantry.runs.get(run_id)` — instead of describing what happened from
    memory.
    """
    payload: dict[str, Any] = {"run_id": run.id, "status": run.status.value}

    if run.status is gantry.RunStatus.ACCEPTED:
        payload["columns"] = list(run.columns)
        payload["rows"] = [list(row) for row in run.rows]
        if run.truncated:
            payload["note"] = "truncated by the configured row limit"
    elif run.failure is not None:
        payload["refused_because"] = run.failure.message
        payload["retry_advice"] = (
            "this will not succeed if run again; answer with what you can"
            if not run.safe_to_retry
            else "the condition may pass; the same query may be run again"
        )
    return json.dumps(payload, default=str)


# ------------------------------------------------------------------ the loop


async def converse(
    client: Any,
    messages: list[dict[str, Any]],
    tool: gantry.Tool[gantry.Run],
    *,
    max_turns: int = 6,
) -> list[gantry.Run]:
    """An ordinary tool-calling loop. It knows nothing about policy."""
    declarations = [as_openai_tool(tool)]
    runs: list[gantry.Run] = []

    for _ in range(max_turns):
        completion = await asyncio.to_thread(
            client.chat.completions.create,
            model=MODEL,
            messages=messages,
            tools=declarations,
        )
        message = completion.choices[0].message
        messages.append(message.model_dump(exclude_none=True))

        if not message.tool_calls:
            print(f"\n  assistant: {message.content}\n")
            return runs

        for call in message.tool_calls:
            arguments = json.loads(call.function.arguments)
            print(f"  → {call.function.name}: {arguments.get('sql', '')[:110]}")

            run = await tool.invoke(arguments)
            runs.append(run)
            print(f"    {run.status.value}", end="")
            print(f" ({len(run.rows)} rows)" if run.rows else "")

            messages.append({"role": "tool", "tool_call_id": call.id, "content": tool_result(run)})

    print("\n  (gave up after the turn limit)")
    return runs


# --------------------------------------------------------------- the example


def schema_prompt(schema: gantry.sql.DatabaseSchema) -> str:
    """What the model is told exists.

    `describe()` reads metadata, never rows, so showing the agent the shape of
    the warehouse costs nothing and saves it guessing column names. Note that
    `analytics_pii` is included: knowing a table exists is not permission to
    read it, and letting the model discover the refusal is more honest than
    hiding the table and watching it invent one.
    """
    lines = []
    for table in schema.tables:
        if table.schema in {"analytics", "analytics_pii"}:
            columns = ", ".join(column.name for column in table.columns)
            lines.append(f"{table.schema}.{table.name}({columns})")
    return "\n".join(lines)


async def main() -> int:
    if not os.environ.get("OPENAI_API_KEY"):
        print("Set OPENAI_API_KEY to run this example.")
        return 1
    try:
        from openai import OpenAI  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        print("This example needs the OpenAI SDK: pip install openai")
        return 1

    db = gantry.sql.connect("postgres", url=os.environ.get("GANTRY_DATABASE_URL", LOCAL_URL))
    tool = build_tool(db)
    schema = await db.describe()

    print(f"connected: {db.provider}")
    arguments = tool.input_schema["properties"]
    assert isinstance(arguments, dict)
    print(f"tool: {tool.name}, the model may pass {sorted(arguments)}\n")

    client = OpenAI()
    messages: list[dict[str, Any]] = [
        {
            "role": "system",
            "content": (
                "You answer questions about a data warehouse by calling the "
                f"{tool.name} function. Write PostgreSQL. These tables exist:\n"
                f"{schema_prompt(schema)}\n"
                "If a query is refused, explain what you could not do and answer "
                "the rest. Do not try to work around a refusal."
            ),
        }
    ]

    runs: list[gantry.Run] = []
    for question in QUESTIONS:
        print(f"user: {question}")
        messages.append({"role": "user", "content": question})
        runs.extend(await converse(client, messages, tool))

    # What an auditor sees afterwards. The refusal is a first-class record, not
    # a line in a log that scrolled past.
    refused = [run for run in runs if run.status is gantry.RunStatus.POLICY_REJECTED]
    print(f"{len(runs)} runs, {len(refused)} refused")
    for run in refused:
        print()
        print(run.render())
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
