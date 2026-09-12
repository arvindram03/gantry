# SQL execution policy

Gantry treats agent-generated SQL as a proposal. A statement must pass the configured policy and
the adapter's native validation before Gantry submits it to the target system.

Policy is a guardrail around execution, not a replacement for database authorization. Always use
least-privilege roles, scoped IAM, network controls, and provider resource limits underneath it.

## Configure an operation

Policy is fixed when trusted application code creates a query operation:

```python
query = db.query(
    read_only=True,
    schemas=("analytics",),
    tables=("analytics.customers", "analytics.orders"),
    denied_tables=("analytics.payroll",),
    max_rows=100,
    timeout=15,
    max_bytes_scanned=10_000_000_000,
    max_cost_usd=1.00,
)
```

Call it directly or expose its narrow tool form:

```python
result = await query("SELECT customer_id, total FROM analytics.orders")
tool = query.tool()
```

The agent sees only the tool's `sql` argument. It does not receive the connection, provider client,
credentials, or any policy fields.

`SQLPolicy` remains the internal authority model used by adapters and lower-level execution APIs:

```python
policy = gantry.sql.SQLPolicy(
    read_only=True,
    allowed_schemas=("analytics",),
    max_rows=100,
    timeout_seconds=15,
)
```

## Policy fields

| Field | Default | Guarantee |
| --- | ---: | --- |
| `read_only` | `True` | Reject write statements and require a native read-only execution boundary. |
| `schemas` | empty | When set, every referenced table must be qualified with an allowed schema. |
| `tables` | empty | When set, every referenced table must match an allowed base or qualified name. |
| `denied_tables` | empty | Reject matching base or qualified table names. |
| `max_rows` | `1000` | Materialize at most this many rows in the agent result. |
| `timeout` | `30` | Require a native timeout or a reconnectable job Gantry can monitor and cancel. |
| `max_bytes_scanned` | `None` | Reject work whose native estimate exceeds the byte limit. |
| `max_cost_usd` | `None` | Reject work whose native estimate exceeds the cost limit. |
| `allow_multiple_statements` | `False` | Reject more than one SQL statement by default. |

These are the arguments to `db.query(...)`; adapters receive their normalized `SQLPolicy`
equivalents. Schema and table names are compared case-insensitively. An allowlist that cannot be
evaluated safely rejects the statement. For example, when `schemas` is set, use
`analytics.orders` rather than an unqualified `orders` reference.

## How admission works

Every query follows the same path:

```text
classify SQL
    ↓
check policy scope and operation
    ↓
ask the adapter for native validation or estimates
    ↓
compare requested guarantees with adapter capabilities
    ↓
execute or reject
```

The local classifier is conservative. Unknown operations, disallowed multiple statements, and
unsafe writes fail before submission. It does not try to translate SQL or replace the target's
parser and planner.

Adapters declare the guarantees they can actually enforce. If a policy requires a row limit,
timeout, byte estimate, cost estimate, or read-only session that the adapter cannot provide,
admission fails closed instead of weakening the policy.

## Read-only execution

`read_only=True` has two layers:

1. Gantry rejects SQL classified as a write.
2. The adapter must provide a target-native read-only boundary.

For PostgreSQL, Neon, and Supabase, queries run in a read-only transaction. BigQuery validates
the native dry-run statement type. Snowflake requires a genuinely read-only role and an explicit
`read_only=True` connection assertion. Static classification alone is never treated as sufficient
isolation.

For governed writes that create derived data, configure a separate materialization operation:

```python
materialize = db.materialize(
    sources=("analytics.*",),
    destinations=("agent_scratch.*",),
    timeout=15,
)
```

This succeeds only if the adapter declares the required materialization support. Gantry does not
grant database permissions; the configured identity must already have them.

## Result and resource bounds

For inline results, adapters fetch at most `max_rows + 1` rows. The extra row is used only to set
`result.inline.truncated`; no more than `max_rows` rows are returned to the agent.

Warehouse and asynchronous adapters can return an `OutputRef` instead of materializing a large
result. The data remains in the target system.

Byte and cost limits depend on native estimates. For example, BigQuery can enforce
`max_bytes_scanned`; cost enforcement additionally requires a configured `price_per_tb_usd`.
If the target cannot produce the required estimate, the query is rejected.

## Handling rejection

Policy rejection is structured and does not raise from the callable tool:

```python
result = await query("DELETE FROM analytics.orders")

if result.status is gantry.ResultStatus.REJECTED:
    print(result.failure.kind)
    print(result.failure.message)
```

Operational failures use normalized kinds such as `AUTH_ERROR`, `OBJECT_NOT_FOUND`,
`SYNTAX_ERROR`, `TIMEOUT`, and `ENGINE_ERROR`, while native details remain available for
diagnosis.

## Agent exposure

Calling `.tool()` removes configuration from the model-visible interface:

```python
tool = query.tool()

tool.name  # "query_sql"
tool.description
tool.input_schema  # SQL plus allowlisted agent verification
```

Policy and trusted checks are still absent from that schema. `query.tool()` requires
`read_only=True`. Expose agent writes through a separately scoped `db.materialize(...)`
operation.

Keep schema discovery and explanation in trusted application code through `db.describe()` and
`db.explain(sql)` unless a separate integration deliberately exposes them.

## Production checklist

- Use a dedicated agent role or service identity.
- Grant access only to required catalogs, schemas, tables, and views.
- Keep `read_only=True` unless writes are an explicit product requirement.
- Set row and timeout limits for every exposed query tool.
- Use byte and cost limits where the provider can estimate them.
- Keep credentials in server-side configuration, never prompts or tool arguments.
- Log rejection and normalized failure metadata without logging secrets.
- Test representative allowed, denied, expensive, and malformed statements before deployment.
