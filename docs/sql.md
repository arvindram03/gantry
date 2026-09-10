# Gantry SQL

Gantry SQL is a governed execution boundary for agent-generated, engine-native SQL. It does not
expose a database connection or invent a portable query language.

## Configure once

Install only the provider driver you need, then connect once:

```python
import gantry

db = gantry.sql.connect("neon", url=database_url)
query = db.query(
    read_only=True,
    schemas=("analytics",),
    max_rows=500,
    timeout=30,
)
```

Call the governed operation directly:

```python
result = await query("SELECT customer_id, COUNT(*) FROM analytics.payments GROUP BY 1")
```

When the engine provides an output reference, access its URI without downloading the full result:

```python
uri = (await query("SELECT * FROM analytics.payments")).uri
```

`SQLResult.uri` is the first non-inline output URI. It is `None` for providers that return only
bounded inline rows; inspect `result.inline` in that case.

Or expose its narrow framework-neutral form to an agent:

```python
tool = query.tool()

tool.name  # "query_sql"
tool.input_schema  # only {"sql": "..."}
result = await tool.invoke(sql="SELECT COUNT(*) FROM analytics.payments")
```

`db.describe()` and `db.explain(sql)` remain direct application operations. Query policy never
appears in the agent tool schema.

## Worked examples

`examples/` holds a runnable file per scenario, with an index in
[examples/README.md](https://github.com/arvindram03/gantry/tree/main/examples)
that starts from what you are trying to do:

| I want to… | Example |
|---|---|
| Let an agent answer questions about a database | `agent_sql.py` |
| Try this with nothing to set up | `local_duckdb.py` |
| Let an agent build a table, and check it before trusting it | `materialize_and_verify.py` |
| Run something expensive without holding a request open | `long_running_query.py` |
| Run a continuous job, and know whether it is healthy | `streaming_flink.py` |

`examples/agent_sql.py` is a runnable version of the above against local
PostgreSQL, Neon, or Supabase — the same code, a different provider name and
URL. `examples/README.md` covers the connection details that differ, including
two that cause intermittent failures rather than clean ones:

- **Neon** requires TLS, and a compute scaled to zero takes seconds to wake, so
  the first connection is slow rather than broken. Its pooled endpoint handles
  server-side prepared statements — verified against a live instance.
- **Supabase** should be reached through its transaction pooler (`:6543`). The
  adapter disables asyncpg's statement cache there automatically, because a
  transaction-pooled backend is often not the one that prepared the statement.
  The session pooler holds a backend per client connection and runs out of
  clients under concurrent use; the direct host is IPv6-only.

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

Create-only derived datasets use the separate [`db.materialize(...)`](materialization.md)
operation. Both configured operations are directly callable and expose `.tool()` when an agent
needs them.
