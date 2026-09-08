<p align="center">
  <img src="docs/assets/gantry-logo-512.jpg" alt="Gantry" width="110">
</p>

<h1 align="center">Gantry</h1>

<p align="center">
  <strong>Safe SQL tools for AI agents.</strong>
</p>

<p align="center">
  Connect Postgres, Supabase, Neon, BigQuery, Snowflake, Flink SQL and more.
  Give agents governed access to real data without handing them unrestricted credentials.
</p>

```bash
pip install gantry
```

## Try it

```python
import os
import gantry

db = gantry.sql.connect(
    "postgres",
    url=os.environ["DATABASE_URL"],
)

tool = db.as_tool(
    read_only=True,
    max_rows=100,
    timeout=30,
)

result = await tool(
    "SELECT id, email FROM customers ORDER BY created_at DESC"
)
```

Your agent gets `tool`.

It does **not** get your database credentials or an unrestricted connection.

**Postgres · Supabase · Neon · BigQuery · Snowflake · Flink SQL**

## Built for agents touching real data

An agent generating SQL against a real database needs a different boundary than a human using a SQL console.

Gantry puts that boundary around execution.

```text
Agent
  │
  │ generated SQL
  ▼
Gantry
  │
  ├── classify
  ├── validate
  ├── enforce policy
  ├── execute with scoped credentials
  ├── bound results
  └── observe
  │
  ▼
Your SQL system
```

Gantry is designed around a simple rule:

> **Agent output is a proposal, not an instruction.**

Generated SQL must pass the configured execution policy before it reaches the target.

## Safety

### Keep credentials out of the agent

The agent receives a Gantry tool:

```python
tool = db.as_tool(read_only=True)
```

It does not receive:

```text
database password
connection object
service account
cloud credentials
provider SDK
```

Credentials stay inside the configured adapter.

### Restrict what SQL can do

```python
tool = db.as_tool(
    read_only=True,
    allowed_schemas=["analytics"],
    allowed_tables=["customers", "orders"],
    max_rows=100,
    timeout=15,
)
```

If the agent generates:

```sql
DELETE FROM customers;
```

Gantry rejects it before execution.

```text
REJECTED

DELETE is not allowed by read-only policy.
```

If it attempts to access something outside its scope:

```sql
SELECT * FROM payroll.employees;
```

it can be rejected as well.

```text
REJECTED

Schema "payroll" is not allowed.
```

### Bound expensive queries

Engines that expose resource estimates can enforce additional limits.

For example:

```python
tool = bigquery.as_tool(
    read_only=True,
    max_rows=100,
    max_bytes_scanned=10_000_000_000,
    max_cost_usd=1.00,
)
```

A query outside those limits is rejected rather than silently consuming the resource.

### Bound what comes back

Agents should not accidentally pull millions of rows into their context.

```python
tool = db.as_tool(
    max_rows=100,
)
```

Small results can be returned inline.

Large results remain in the underlying data system and are represented by a reference.

```text
BigQuery table
S3 / GCS object
Snowflake table
Kafka topic
...
```

### Fail closed

Policies are guarantees, not hints.

If Gantry cannot enforce a requested constraint for a particular target, admission fails.

```text
Policy requires cost limit
        ↓
Adapter cannot enforce cost limit
        ↓
REJECTED
```

Gantry does not silently remove the constraint and execute anyway.

### Defense in depth

Gantry policy is not a replacement for database security.

Use the underlying system's controls as the hard security boundary:

```text
read-only database roles
scoped IAM
service accounts
authorized datasets
network policies
statement timeouts
resource quotas
```

The intended model is:

```text
Agent
  ↓
Gantry policy
  ↓
scoped engine credentials
  ↓
native database / cloud controls
  ↓
execution
```

Static SQL validation alone should never be treated as sufficient isolation.

## Works with your agent

Gantry is framework-neutral.

```python
tool = db.as_tool(read_only=True)
```

Give that tool to the agent framework you already use.

```text
OpenAI Agents
Claude
LangGraph
Agno
MCP
your own agent runtime
```

Or call it directly:

```python
result = await tool(
    "SELECT COUNT(*) FROM orders WHERE status = 'failed'"
)

if result.ok:
    print(result.inline.rows)
```

## More SQL systems

### Supabase

```python
db = gantry.sql.connect(
    "supabase",
    url=os.environ["DATABASE_URL"],
)

tool = db.as_tool(read_only=True)
```

### Neon

```python
db = gantry.sql.connect(
    "neon",
    url=os.environ["DATABASE_URL"],
)

tool = db.as_tool(read_only=True)
```

### BigQuery

```python
db = gantry.sql.connect(
    "bigquery",
    project="acme-prod",
    dataset="analytics",
)

tool = db.as_tool(
    read_only=True,
    max_bytes_scanned=10_000_000_000,
)
```

### Snowflake

```python
db = gantry.sql.connect(
    "snowflake",
    account="acme",
    database="ANALYTICS",
    warehouse="AGENT_WH",
)

tool = db.as_tool(
    read_only=True,
    timeout=30,
)
```

The SQL remains native to each system.

Gantry does not introduce a query DSL or transpile SQL between engines.

## Long-running SQL

The same boundary can apply to long-running SQL jobs such as Flink SQL.

```python
flink = gantry.flink.connect(
    endpoint="https://sql-gateway.acme.internal",
)

handle = await flink.submit(
    """
    INSERT INTO clean_events
    SELECT *
    FROM raw_events
    WHERE event_type IS NOT NULL
    """
)
```

Observe it later:

```python
execution = await flink.status(handle)

print(execution.state)
```

```text
RUNNING
```

Or cancel it:

```python
await flink.cancel(handle)
```

The underlying Flink job continues to own the actual streaming data path.

## Gantry is not in the data path

Your database or execution engine reads and writes data directly.

```text
source
   ↓
database / warehouse / Flink
   ↓
destination
```

Gantry submits work and observes execution. It does not proxy datasets or streaming records.

Only explicitly bounded query results pass back through Gantry.

## The model

```text
                    Gantry

Agent ──SQL──→ validate → policy → execute
                                      │
                                      ▼
                              SQL / data engine
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                       data                     output

                                      │
                            status / metrics / ref
                                      │
                                      ▼
                                    Gantry
                                      │
                                      ▼
                                    Agent
```

## Documentation

- [SQL providers](docs/sql.md)
- [Safety and policies](docs/policy.md)
- [Flink SQL](docs/flink.md)
- [Custom SQL adapters](docs/adapters.md)

## Development

```bash
uv sync
make check
```

Python 3.12+ · Apache-2.0
