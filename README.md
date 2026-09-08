<p align="center">
  <img src="docs/assets/gantry-logo-512.jpg" alt="Gantry" width="110">
</p>

<h1 align="center">Gantry</h1>

<p align="center"><strong>Give AI agents safe access to anything that speaks SQL.</strong></p>

Gantry is the execution layer between agents and existing SQL systems. Agents generate native
SQL; Gantry validates it, applies policy, executes it on the target engine, and keeps the result
or long-running job observable.

```bash
pip install gantry
```

## Start with Postgres

Install the PostgreSQL driver:

```bash
pip install "gantry[postgres]"
```

Create a governed, framework-neutral tool for an agent:

```python
import os

import gantry

db = gantry.sql.connect(
    "postgres",
    url=os.environ["DATABASE_URL"],
)

sql_tool = db.as_tool(read_only=True)

result = await sql_tool(
    "SELECT id, email FROM customers ORDER BY created_at DESC"
)

if result.ok and result.inline:
    print(result.inline.rows)
```

The agent receives `sql_tool`, not the database connection or its credentials. The same object
can be wrapped by the agent framework or application you already use.

## One interface across SQL systems

Gantry keeps the governed lifecycle consistent while each target keeps its own SQL dialect and
configuration:

```python
postgres = gantry.sql.connect(
    "postgres",
    url=os.environ["DATABASE_URL"],
)

bigquery = gantry.sql.connect(
    "bigquery",
    project="acme-prod",
    location="US",
)

snowflake = gantry.sql.connect(
    "snowflake",
    account="acme",
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    database="ANALYTICS",
    warehouse="COMPUTE_WH",
    read_only=True,
)

flink = gantry.flink.connect(
    "https://sql-gateway.acme.internal",
    jobmanager_endpoint="https://flink.acme.internal",
    token=os.environ["FLINK_TOKEN"],
)
```

Postgres, BigQuery, and Snowflake expose `validate`, `submit`, `status`, `wait`, `cancel`, and
bounded query results through `gantry.sql`. Flink SQL exposes the same execution lifecycle
through `gantry.flink`, with streaming-aware health checks.

SQL always stays native to its target. Gantry does not introduce a query DSL, transpile SQL, or
pretend engine-specific behavior is portable. PostgreSQL SQL goes to PostgreSQL, BigQuery SQL
goes to BigQuery, Snowflake SQL goes to Snowflake, and Flink SQL goes to Flink's planner.

## Common guarantees

- **Scoped access:** adapters keep credentials private and rely on target-native roles and
  permissions.
- **Validation:** statements are classified, checked against policy, and validated by the target
  where supported.
- **Execution limits:** policies can bound runtime, rows, scanned bytes, cost, schemas, and tables
  according to adapter capabilities.
- **Durable state where needed:** reconnectable engines retain native job or query IDs so work
  can be observed and cancelled after the submitting process exits.
- **Bounded outputs:** small query results are capped; large results stay in their system and are
  returned as references.
- **Normalized failures:** authentication, validation, timeout, resource, connector, and engine
  failures use a small common taxonomy while preserving native details.

Policy admission fails closed when an adapter cannot enforce a requested guarantee. Native
database roles, IAM, and scoped service identities remain the primary security boundary.

## Read-only means read-only

A write submitted through a read-only tool is rejected before execution:

```python
sql_tool = db.as_tool(read_only=True, max_rows=100, timeout=15)

result = await sql_tool("DELETE FROM customers")

assert result.status is gantry.ResultStatus.REJECTED
print(result.failure.message)
# DELETE is not allowed by read-only policy
```

## Long-running Flink SQL

The same execution model also covers SQL jobs that are meant to keep running:

```python
result = await flink.run(
    sql="""
        INSERT INTO clean_events
        SELECT *
        FROM raw_events
        WHERE event_type IS NOT NULL
    """,
    declared_outputs=("clean_events",),
    checks=(
        gantry.flink.MaxRestartCount(3),
        gantry.flink.MaxWatermarkLag("60s"),
    ),
)

assert result.status is gantry.ResultStatus.ACCEPTED
assert result.execution.state is gantry.ExecutionState.RUNNING
```

For streaming mode, `RUNNING` plus passing health checks means the job is accepted; Gantry does
not wait for an artificial terminal success. The durable handle contains the native Flink JobID
for later `status`, `health`, and `cancel` calls.

## Gantry is not in the data path

Underlying engines read sources and write destinations directly. Gantry submits SQL and observes
job state; it does not proxy datasets or streaming records. Only explicitly bounded inline query
results pass back to the caller. Everything larger remains in the target system and is represented
by an output reference.

See [Gantry SQL](docs/sql.md) for provider configuration and policy details, and
[Gantry Flink SQL](docs/flink.md) for long-running SQL execution and health checks.

## Development

```bash
uv sync
make check
```

Python 3.12+ · Apache-2.0
