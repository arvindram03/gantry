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
tool.input_schema  # SQL plus supported declarative verification
result = await tool.invoke(
    sql="SELECT COUNT(*) FROM analytics.payments",
    verify=[{"type": "not_empty"}],
)
```

`db.describe()` and `db.explain(sql)` remain direct application operations. Query policy never
appears in the agent tool schema. Agent verification is additive and cannot
change that policy or remove trusted checks.

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

What the classifier reads is the statement. It follows every `FROM` list, join and
subquery to name the tables a statement touches, and it refuses outright a statement that
calls a built-in which reaches a table by name or runs a string as SQL — `table_to_xml`,
`query_to_xml`, `dblink` and their relatives — because nothing in the text says what those
touch. It cannot see inside a user-defined function, which can do the same. Give the
connection a role that can reach only what agents should reach, and treat the allow-list as
the check that explains a refusal, not the wall that prevents one.

## Writes

`db.query()` reads and `db.materialize()` builds a new table. `db.write()` is for
changing rows that already exist — `INSERT`, `UPDATE` and `DELETE`, one statement
at a time, against tables you named:

```python
write = db.write(
    destinations=["app.tickets", "agent_scratch.*"],
    max_rows_affected=25,
)

run = await write("UPDATE app.tickets SET status = 'closed' WHERE id = 4182")
run.rows_affected  # 1

tools = [query.tool(), write.tool()]
```

A write is bounded on every side that can be decided before it runs, and on the
one that cannot:

| Bound | Default | What it does |
| --- | --- | --- |
| `destinations` | required | The tables that may be changed. There is no default place to write. |
| `sources` | none | What a statement may read *besides* the table it changes. An `INSERT ... SELECT` from somewhere else has to be allowed by name. |
| `statements` | insert, update, delete | Which kinds run. `merge` has to be asked for. |
| `require_where` | on | An `UPDATE` or `DELETE` with no `WHERE` is refused before it runs. |
| `max_rows_affected` | `100` | The statement runs in a transaction and is **rolled back** if it changed more. |
| `max_rows` | `100` | How many `RETURNING` rows come back. |

The row ceiling is the one that matters most, because it is the one nobody can
know in advance. It is not a report: a `DELETE` that would have removed 40,000
rows under a limit of 25 removes none, and the run says how many it would have
removed.

```text
EXECUTION_FAILED
  the statement would have changed 40000 rows; at most 25 rows are allowed,
  so it was rolled back and nothing was changed
```

A write is its own operation in policy, so a rule that allows queries does not
allow it, and it can be denied, scoped to an environment or held for
confirmation like any other:

```python
gantry.allow.write(destinations=["agent_scratch.*"])
gantry.allow.write(
    destinations=["app.tickets"],
    constraints={"max_rows_affected": 25},
    require_confirmation=True,
    confirmation_message="This changes customer tickets.",
)
gantry.deny.write(destinations=["billing.*"])
```

What is and is not covered:

- **One statement, one table.** A data-modifying CTE that changes a second table
  is refused, as is MySQL's multi-table `UPDATE` unless every table in the join
  is a destination.
- **The count is the statement's own.** Rows changed on its behalf — a cascading
  delete, a trigger — are the database's doing and are not in `rows_affected` or
  under the ceiling. MySQL counts rows *changed*, so an `UPDATE` that sets a row
  to the value it already has does not count it.
- **Rollback needs a transaction.** PostgreSQL, Neon, Supabase, MySQL on InnoDB
  and DuckDB have one. BigQuery and Snowflake do not declare the capability yet,
  so a write there is refused rather than run unbounded. A MyISAM table cannot be
  rolled back and should not be a destination.
- **A function can still write elsewhere.** Classification reads the statement,
  not the bodies of the functions it calls. The database role is the boundary.

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
| MySQL | `gantry[mysql]` | MySQL | Local async task, read-only transaction, killed on timeout |
| BigQuery | `gantry[bigquery]` | BigQuery | Reconnectable warehouse job + dry run |
| Snowflake | `gantry[snowflake]` | Snowflake | Reconnectable query ID |
| DuckDB | `gantry[duckdb]` | DuckDB | Local task, bounded cursor |

### MySQL

MySQL is close enough to the shared contract to need no new concepts, and
different enough in three places that assuming PostgreSQL would be wrong.

A schema is a database. `schemas=["analytics"]` allows the `analytics` database,
and `information_schema.tables.table_catalog` is the literal `def` on every row,
so Gantry reports no catalog rather than inventing one.

`max_execution_time` bounds `SELECT` and nothing else — a
`CREATE TABLE ... AS SELECT` runs to completion under it. So `timeout` is
enforced twice: by the server variable for reads, and by a deadline plus
`KILL QUERY` from a second connection for everything else. A timed-out
materialization leaves no half-built destination, because MySQL 8 rolls the DDL
back.

`EXPLAIN` cannot describe DDL, so native validation uses `PREPARE` and
`DEALLOCATE`, which resolves the statement without running it.

The dialect differs too, and `gantry.sql.MySQLDialect` carries the difference:
MySQL escapes backslashes inside ordinary strings, so `'a\'; SELECT 2'` is one
statement there and two under PostgreSQL's rules, and MySQL has no
dollar-quoting.

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
