# Gantry

<img src="https://raw.githubusercontent.com/arvindram03/gantry/main/docs/assets/gantry-logo-512.jpg" alt="Gantry" width="110" align="right" />

**Let the agent write the SQL. Keep the authority to run it.**

[![PyPI](https://img.shields.io/pypi/v/data-gantry?color=2f6feb)](https://pypi.org/project/data-gantry/)
[![Python](https://img.shields.io/pypi/pyversions/data-gantry)](https://pypi.org/project/data-gantry/)
[![Docs](https://img.shields.io/badge/docs-gantry-2f6feb)](https://arvindram03.github.io/gantry/)
[![CI](https://github.com/arvindram03/gantry/actions/workflows/ci.yml/badge.svg)](https://github.com/arvindram03/gantry/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/arvindram03/gantry/blob/main/LICENSE)

Gantry is the layer between your agent and your database. Your application holds the
credentials and writes the policy and trusted checks. The agent gets a tool
whose inputs are `sql` and an optional declarative `verify` commitment.

Four questions, answered in this order, for every statement the agent writes:

| | |
| --- | --- |
| **Policy** | may this actor do this, to these tables, in this environment? |
| **Confirmation** | should a human be told before it happens? |
| **Execution** | did it run? |
| **Verification** | can the result be believed? |

Each answer lands on a durable **run** you can read back months later.

```bash
pip install data-gantry
```

The distribution is `data-gantry`; the import is `gantry`.

---

## Give your agent safe and governed DB access in 10 lines

The agent ends up with exactly one tool: ask the `analytics` schema a question in
SQL. Read-only, at most 100 rows, at most 30 seconds — and nothing else.

```python
import os
import gantry

db = gantry.sql.connect("postgres", url=os.environ["DATABASE_URL"])

query = db.query(
    read_only=True,  # writes are refused, not filtered
    schemas=["analytics"],  # nothing outside analytics
    max_rows=100,  # bounded result
    timeout=30,  # bounded runtime
)

tools = [query.tool()]  # ← hand this to your agent
```

That's the whole integration. `query.tool()` is framework-neutral — a name, a JSON
schema, and an async handler — so it drops into any agent loop:

```python
tool = query.tool()

tool.name, tool.description, tool.input_schema  # Anthropic, OpenAI, LangChain, or your own loop

run = await tool.invoke({"sql": "SELECT plan, COUNT(*) ...", "verify": [{"type": "not_empty"}]})

run.status  # RunStatus.ACCEPTED
run.rows  # (('free', 1250), ('team', 1250), ...)
run.id  # "run_01M2C…" — readable long after the conversation ends
```

## Supported systems

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://arvindram03.github.io/gantry/sql/)
[![Neon](https://img.shields.io/badge/Neon-00E599?style=for-the-badge&logo=neon&logoColor=black)](https://arvindram03.github.io/gantry/sql/)
[![Supabase](https://img.shields.io/badge/Supabase-3FCF8E?style=for-the-badge&logo=supabase&logoColor=black)](https://arvindram03.github.io/gantry/sql/)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://arvindram03.github.io/gantry/sql/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://arvindram03.github.io/gantry/sql/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white)](https://arvindram03.github.io/gantry/flink/)
[![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)](https://arvindram03.github.io/gantry/sql/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://arvindram03.github.io/gantry/sql/)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white)](https://arvindram03.github.io/gantry/nosql/)

## What the agent cannot do

The agent sees the SQL field and a constrained verification vocabulary. It cannot:

- **widen its own policy** — raise the row cap, extend the timeout, or add a schema
- **weaken trusted checks** — its checks are added to the application contract, never substituted
- **reach the connection or the credentials** — those stay in your code, never in the tool schema
- **turn a read into a write** — `query.tool()` refuses to be created unless the policy is read-only
- **smuggle in a second statement** — a multi-statement submission is refused as a batch
- **be trusted because the engine said yes** — a run that produced the wrong table comes back `REJECTED`, not success
- **wave through its own sensitive operation** — confirmation is a separate host-side call, absent from every tool schema

"It ran" and "it can be believed" are different questions, and only the first is the
engine's to answer. `run.ok` means both.

## What you can enforce

Two layers, both trusted configuration, and they compose toward *less* authority —
attaching a policy can never widen what a call site already bounded.

### On the operation

Set once, in your code, on `db.query(...)`:

| Policy | Default | What it does |
| --- | --- | --- |
| `read_only` | `True` | Refuses anything that is not a read |
| `schemas` | all | Allow-list of schemas the SQL may touch |
| `tables` | all | Allow-list of tables, qualified or bare |
| `denied_tables` | none | Deny-list; wins over any allow-list |
| `max_rows` | `1000` | Caps the result, and reports it as truncated |
| `timeout` | `30s` | Caps runtime |
| `max_bytes_scanned` | off | Refused before running, from the engine's own estimate |
| `max_cost_usd` | off | Refused before running, on engines that price a query |
| `allow_multiple_statements` | `False` | Whether a batch is a batch or a refusal |
| `checks` | none | Trusted checks that must pass before the result is accepted |

Schema, table and statement rules are enforced by Gantry. The bounds are enforced by
the engine — and a bound the adapter cannot apply is **refused rather than ignored**,
so `max_bytes_scanned` on a backend that cannot estimate bytes fails closed instead
of silently passing. The
[capability matrix](https://arvindram03.github.io/gantry/api/capabilities/) is
generated from the adapter source and lists which backend enforces what.

### As a reusable policy

One policy, written once, for every engine and every operation — who may do what, to
which tables, in which environment:

```python
policy = gantry.Policy(
    name="data-agents",
    rules=[
        gantry.allow.query(actors=["research-agent"], sources=["analytics.*"]),
        gantry.allow.materialize(sources=["raw.*"], destinations=["agent_scratch.*"]),
        gantry.deny.materialize(destinations=["prod.*"]),
    ],
)

db = gantry.sql.connect("postgres", url=DATABASE_URL, policy=policy)
mongo = gantry.nosql.connect("mongodb", uri=URI, database="analytics", policy=policy)
stream = gantry.stream.connect("flink", endpoint=GATEWAY, policy=policy)
```

- **Deny wins, and silence denies.** No rule matched means refused, so a policy is a
  list of what may happen rather than a list of what may not.
- **Gantry decides what the proposal touches**, not the agent, and every resource is
  authorized on its own. Allowing `analytics.*` does not authorize the `finance` table
  joined to it; a `$lookup` reaching an unauthorized collection, or an `INSERT` hidden
  in a query, is refused before anything runs.
- **The actor is yours to assert.** It comes from
  `gantry.actor.context(...)` in your code, never from a tool argument — an agent
  that could name itself could name someone else.
- **Refusals are structured.** `run.admission.codes` is
  `("DESTINATION_DENIED",)`, not a string to grep, and the exact policy version that
  decided is recorded on the run.

## Ask me first

Some work is allowed and still deserves a question. A rule can say so without
turning the answer into a refusal:

```python
gantry.allow.materialize(
    sources=["raw.*"],
    destinations=["prod.*"],
    require_confirmation=True,
    confirmation_message="This will write to production data.",
)
```

The run parks, durable and allowed, having touched nothing — no connection opened,
no job submitted, no table created:

```python
run = await build(sql)

if run.status is gantry.RunStatus.AWAITING_CONFIRMATION:
    print(run.confirmation.message)  # "This will write to production data."
    run = await gantry.runs.confirm(run.id)  # or gantry.runs.decline(run.id)
```

The agent learns that it must ask and gets no way to answer: `gantry.runs.confirm`
appears in no tool schema, and a tool call carrying `confirmed=True` is refused
rather than ignored. Confirming resumes the same run and the same statement;
declining ends it without executing.

This is a user-interaction gate, not an authentication one. Gantry records that your
application supplied confirmation before execution — it never claims a particular
authenticated person approved anything, which is why no field names one.

## Every run is a record you can read back

```python
gantry.runs.configure(gantry.runs.SQLiteRunStore(".gantry/runs.db"))

# …in another process, holding only the id
print(gantry.runs.get(run_id).render())
```

```text
Run run_01M2CRNRC9CH6MATYFFQCABRA9

Actor
  agent:research-agent

Operation
  materialize

Admission
  ✗ refused
  policy: data-agents
    ✓ read raw.orders
    ✗ write prod.orders
    DESTINATION_DENIED
      writing prod.orders is denied by rule deny-materialize-2

Decision
  POLICY_REJECTED
```

Who asked, what they asked for, which policy version allowed or refused it, what the
engine did, which checks ran, and what was decided — without the conversation that
produced it.

## Letting an agent write

Reads are the easy half. To produce data the agent gets a create-only path — one
`CREATE TABLE AS`, to a destination you named, that must not already exist:

```python
build = db.materialize(
    sources=["analytics.*"],
    destinations=["reporting.*"],
    checks=[gantry.verify.row_count(max=1_000_000)],
)

tools = [query.tool(), build.tool()]
```

No `DROP`, no `REPLACE`, no writing outside `reporting`, and no accepted result until
the destination has been checked. The agent can make the contract *stricter* at
invocation time — `{"verify": [{"type": "not_empty"}]}` — and never looser.

## It's a library, not a proxy — your data doesn't route through it

You add an import, not a service. Gantry decides whether a statement may run and
whether its result can be believed — it is not a stop on the route your data takes.

- **Bulk output never passes through it.** The engine writes where it was told to and
  you get an `OutputRef`. Only the `max_rows` you set come back inline.
- **There is no server.** A library in your process: nothing to deploy, no proxy in
  front of the database, no broker between the agent and the engine.
- **Credentials stay in your code.** What reaches the adapter excludes provider
  configuration; what reaches the agent is a JSON schema of SQL and verification.
- **Long jobs are handles, not held-open calls.** `submit()` returns an
  `ExecutionHandle` carrying the engine's own job id, so a forty-minute Flink job does
  not need the agent — or the process that launched it — to still be alive.

---

## Next

- **[Documentation](https://arvindram03.github.io/gantry/)** — guides and the full API reference
- **[Examples](https://github.com/arvindram03/gantry/tree/main/examples)** — runnable scenarios, indexed by what you are trying to do
- **[Capability matrix](https://arvindram03.github.io/gantry/api/capabilities/)** — what each backend enforces
- **[Security](https://github.com/arvindram03/gantry/blob/main/SECURITY.md)** — what Gantry does and does not protect

Gantry is Apache 2.0 licensed. See [LICENSE](https://github.com/arvindram03/gantry/blob/main/LICENSE).
