# Gantry SQL

Gantry SQL is a governed execution boundary for agent-generated, engine-native SQL. It does not
expose a database connection or invent a portable query language.

## Connect and expose a tool

Install only the provider driver you need, then connect once:

```python
import gantry

db = gantry.sql.connect("neon", url=database_url)
tool = db.as_tool(
    operations=("describe", "query"),
    read_only=True,
    allowed_schemas=("analytics",),
    max_rows=500,
    timeout=30,
)
```

The callable form is the smallest framework-neutral integration:

```python
result = await tool("SELECT customer_id, COUNT(*) FROM analytics.payments GROUP BY 1")
```

`tool.invoke("describe")` and `tool.invoke("explain", sql=...)` provide the optional operation
surface. Framework packages can wrap the same object without adding an agent SDK dependency to
Gantry.

## Safety boundary

Every query follows the same path:

```text
classify → policy check → native validate/dry run → admit → execute → bound → verify
```

Classification is conservative and advisory. Read-only execution is admitted only when the
adapter has a stronger execution boundary as well:

- PostgreSQL, Neon, and Supabase use a database read-only transaction.
- DuckDB requires `read_only=True` on an existing database file.
- BigQuery checks the engine's dry-run statement type before submitting the same immutable SQL.
- Snowflake requires a genuinely read-only role and an explicit `read_only=True` assertion.

Database roles, IAM, authorized datasets, and scoped credentials remain the primary security
boundary. Gantry does not replace native authorization.

Allowed and denied table policies are an additional admission check. Use them together with
native permissions rather than as a substitute for those permissions.

## Bounded and reconnectable results

Adapters fetch at most `max_rows + 1` records to detect truncation. `SQLResult.inline` therefore
never contains more than the configured limit. Warehouse adapters also return provider result
references so large data stays outside agent context.

For asynchronous jobs, persist the returned handle and reconnect through the same provider:

```python
handle = await db.submit(sql, policy=gantry.sql.SQLPolicy(max_rows=500))
execution = await db.status(handle)
result = await db.wait(handle)
```

BigQuery and Snowflake handles retain provider-native job IDs. Their status, cancellation, and
result paths do not depend on an in-memory task. PostgreSQL and DuckDB jobs are intentionally
process-local.

## Provider matrix

| Provider | Extra | Dialect | Execution model |
| --- | --- | --- | --- |
| PostgreSQL | `gantry[postgres]` | PostgreSQL | Local async task, native transaction |
| Neon | `gantry[postgres]` | PostgreSQL | Shared PostgreSQL adapter |
| Supabase | `gantry[postgres]` | PostgreSQL | Shared PostgreSQL adapter |
| BigQuery | `gantry[bigquery]` | BigQuery | Reconnectable warehouse job + dry run |
| Snowflake | `gantry[snowflake]` | Snowflake | Reconnectable query ID |
| DuckDB | `gantry[duckdb]` | DuckDB | Local task, bounded cursor |

## Custom providers

An internal provider can reuse a built-in dialect while supplying its own adapter:

```python
gantry.sql.register(
    "internal-warehouse",
    adapter=InternalWarehouseAdapter(),
    dialect="postgres",
)

db = gantry.sql.connect("internal-warehouse", tenant="finance")
```

Custom adapters implement the `SQLAdapter` protocol and declare only the capabilities they can
actually enforce or observe. Admission fails closed when a policy requires anything else.
