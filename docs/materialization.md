# SQL materialization

SQL materialization creates a new derived table or view from native SQL while keeping source
access, destination authority, resource limits, and acceptance criteria in trusted application
configuration.

The public API is operation-first:

```python
import gantry

warehouse = gantry.sql.connect("bigquery", project="acme")

materialize = warehouse.materialize(
    sources=["raw.*", "reference.country_codes"],
    destinations=["agent_scratch.*"],
    create_only=True,
    max_bytes_scanned=10_000_000_000,
    timeout=300,
    verify=[
        gantry.verify.destination_exists(),
        gantry.verify.row_count(min=1),
        gantry.verify.required_columns(["customer_id", "outstanding_balance"]),
    ],
)
```

Any application code can call the configured operation directly:

```python
result = await materialize(
    """
    CREATE TABLE agent_scratch.high_risk_customers AS
    SELECT customer_id, SUM(balance) AS outstanding_balance
    FROM raw.invoices
    WHERE status = 'unpaid'
    GROUP BY customer_id
    """
)
```

An agent receives only its narrow tool form:

```python
tool = materialize.tool()

tool.name  # "materialize_sql"
tool.input_schema  # only {"sql": "..."}
result = await tool.invoke(sql="CREATE TABLE agent_scratch.out AS SELECT * FROM raw.input")
```

The connection, credentials, policy, limits, and verification checks do not appear in the tool
schema.

## Admission

Before submitting SQL, Gantry conservatively inspects:

- the operation;
- every source object;
- the destination object;
- replacement behavior; and
- mandatory adapter features such as scan limits and destination introspection.

Materialization v0 accepts one native `CREATE TABLE ... AS SELECT ...` statement. Adapters may
also accept `CREATE VIEW ... AS SELECT ...`. The destination must include a schema or dataset.

Source and destination patterns are case-insensitive globs:

```python
sources = ["raw.*"]
destinations = ["agent_scratch.*"]
```

`CREATE OR REPLACE`, `IF NOT EXISTS`, multi-statement SQL, mutation statements, table-valued
source expressions, and uninspectable query shapes fail closed. Comma joins are deliberately
unsupported in v0; use explicit `JOIN` syntax so every source can be identified reliably.

Create-only admission checks that the destination does not already exist. The original native
statement is then submitted unchanged, so the engine's own create semantics and permissions
remain a second boundary against replacement races.

## Execution and recovery

`await materialize(sql)` performs admission, execution, observation, and verification. Separate
submission when the engine exposes a durable job:

```python
handle = await materialize.submit(sql)

# Recreate the configured materializer in another worker if needed.
materialize = warehouse.materialize(
    sources=["raw.*"],
    destinations=["agent_scratch.*"],
    verify=[gantry.verify.destination_exists()],
)
result = await materialize.wait(handle)
```

The same configured operation also exposes `materialize.status(handle)` and
`materialize.cancel(handle)` so a caller does not need the underlying connection to observe or
cancel its submitted work.

BigQuery handles contain the provider-native job ID and the non-secret destination metadata needed
to reconnect. BigQuery allocates that job ID before submission and attempts lookup after an
ambiguous submission response; it does not automatically submit the CTAS a second time.

DuckDB implements the same contract with process-local execution. Its handles are not durable
across processes, which is reflected by `materialize.capabilities.durable_jobs == False`.

## Verification and acceptance

Engine success and Gantry acceptance are distinct:

```text
SUCCEEDED + verification passed = ACCEPTED
SUCCEEDED + verification failed = VERIFICATION_FAILED
```

Built-in checks are intentionally small:

- `gantry.verify.destination_exists()`
- `gantry.verify.row_count(min=..., max=...)`
- `gantry.verify.required_columns([...])`

Verification is fixed when the materializer is constructed; the caller submitting SQL cannot
weaken it. A failed verification does not delete the created object in v0. The result retains its
output reference so operators can inspect or clean it up explicitly.

## Results and failures

`MaterializationResult` exposes:

```python
result.status
result.output
result.uri
result.execution
result.verification
result.failure
result.handle
```

The successful output is an `OutputRef`; Gantry does not fetch the materialized dataset. The
underlying engine reads sources and writes the destination directly.

Materialization adds these normalized admission failures:

```text
SOURCE_NOT_ALLOWED
DESTINATION_NOT_ALLOWED
DESTINATION_EXISTS
OPERATION_NOT_ALLOWED
```

It also uses the common failure taxonomy for unsupported requirements, cost limits, timeouts,
submission and engine failures, cancellation, and verification failure.

## Provider support

| Provider | Materialization v0 | Job model |
| --- | --- | --- |
| BigQuery | Tables and views | Durable, reconnectable job ID |
| DuckDB | Tables and views | Process-local task |
| PostgreSQL / Neon / Supabase | Not yet enabled | Fails closed |
| Snowflake | Not yet enabled | Fails closed |

Use native IAM, roles, dataset permissions, quotas, and network controls with Gantry policy. Static
SQL inspection is an admission layer, not the sole security boundary.
