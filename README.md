<img src="https://raw.githubusercontent.com/arvindram03/gantry/main/docs/assets/gantry-logo-512.jpg" alt="Gantry" width="110" align="right" />

# Gantry

**Let the agent write the SQL. Keep the authority to run it.**

[![PyPI](https://img.shields.io/pypi/v/data-gantry?color=2f6feb)](https://pypi.org/project/data-gantry/)
[![Python](https://img.shields.io/pypi/pyversions/data-gantry)](https://pypi.org/project/data-gantry/)
[![Docs](https://img.shields.io/badge/docs-gantry-2f6feb)](https://arvindram03.github.io/gantry/)
[![CI](https://github.com/arvindram03/gantry/actions/workflows/ci.yml/badge.svg)](https://github.com/arvindram03/gantry/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/arvindram03/gantry/blob/main/LICENSE)

Gantry is the layer between your agent and your database. Your application holds the
credentials and writes the policy. The agent gets a tool whose only input is `sql`.

Every statement is classified, checked against the policy, validated by the engine,
bounded, executed, and **verified** before its result is marked accepted.

```bash
pip install data-gantry
```

The distribution is `data-gantry`; the import is `gantry`.

---

## Give an agent read-only SQL on one schema — in 10 lines

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

# Anthropic, OpenAI, LangChain, or your own loop:
schema = {
    "name": tool.name,  # "query_sql"
    "description": tool.description,
    "input_schema": tool.input_schema,  # one property: sql
}

# When the model calls it:
result = await tool.invoke({"sql": "SELECT plan, COUNT(*) ..."})

result.status  # ACCEPTED
result.inline.rows  # (('free', 1250), ('team', 1250), ...)
```

## What the agent cannot do

It sees one string field. It cannot widen the schema allow-list, raise the row cap,
extend the timeout, reach the connection, or swap credentials — those live in your
code, not in the tool schema. It cannot turn a read into a write: `query.tool()`
refuses to be created at all unless the policy is read-only.

And it cannot have a wrong answer accepted just because the engine returned success.

```python
result.status is gantry.ResultStatus.ACCEPTED  # ran *and* passed verification
```

A statement that runs perfectly and produces the wrong table comes back
`VERIFICATION_FAILED`, not success. "It ran" and "it can be believed" are
different questions, and only the first one is the engine's to answer.

## Supported systems

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://arvindram03.github.io/gantry/sql/)
[![Neon](https://img.shields.io/badge/Neon-00E599?style=for-the-badge&logo=neon&logoColor=black)](https://arvindram03.github.io/gantry/sql/)
[![Supabase](https://img.shields.io/badge/Supabase-3FCF8E?style=for-the-badge&logo=supabase&logoColor=black)](https://arvindram03.github.io/gantry/sql/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://arvindram03.github.io/gantry/sql/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white)](https://arvindram03.github.io/gantry/flink/)
[![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)](https://arvindram03.github.io/gantry/sql/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://arvindram03.github.io/gantry/sql/)

| | What Gantry does there |
| --- | --- |
| **PostgreSQL**, Neon, Supabase | Governed queries, bounded inline results |
| **DuckDB** | Governed queries, local materialization |
| **Apache Flink** | Batch and streaming jobs, durable handles, cancellation |
| **BigQuery** \* | Governed queries, output references, materialization |
| **Snowflake** \* | Governed queries, reconnectable jobs |

\* The adapter ships and declares its capabilities, but has not yet been exercised
against a live account. Everything else is tested against a real engine on every change.

A policy is only admitted when the adapter can actually enforce it — asking for
`read_only=True` on a backend that cannot hold a read-only session is refused rather
than quietly trusted. The [capability matrix](https://arvindram03.github.io/gantry/api/capabilities/)
is generated from the adapter source, so it cannot drift from what the code does.

## Letting an agent write

Reads are the easy half. When an agent needs to produce data, Gantry gives it a
create-only path — one `CREATE TABLE AS`, to a destination you named, that must not
already exist:

```python
build = db.materialize(
    sources=["analytics.*"],
    destinations=["reporting.*"],
    verify=[gantry.verify.row_count(max=1_000_000)],
)

tools = [query.tool(), build.tool()]
```

No `DROP`, no `REPLACE`, no writing outside `reporting`, and no accepted result
until the destination has been checked.

## Not in your data path

Gantry governs the statement and reads back references — it never streams your rows
through itself. A 40-minute Flink job is a durable handle you can poll, reconnect to
from another process, and cancel; not a tool call you have to hold open.

---

## Next

- **[Documentation](https://arvindram03.github.io/gantry/)** — guides and the full API reference
- **[Examples](https://github.com/arvindram03/gantry/tree/main/examples)** — seven runnable scenarios, indexed by what you are trying to do
- **[Capability matrix](https://arvindram03.github.io/gantry/api/capabilities/)** — what each backend enforces
- **[Security](https://github.com/arvindram03/gantry/blob/main/SECURITY.md)** — what Gantry does and does not protect

Gantry is Apache 2.0 licensed. See [LICENSE](https://github.com/arvindram03/gantry/blob/main/LICENSE).
