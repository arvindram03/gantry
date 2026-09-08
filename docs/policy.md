# SQL execution policy

Gantry treats agent-generated SQL as a proposal. A statement must pass the configured policy and
the adapter's native validation before Gantry submits it to the target system.

Policy is a guardrail around execution, not a replacement for database authorization. Always use
least-privilege roles, scoped IAM, network controls, and provider resource limits underneath it.

## Configure a tool

The simplest way to apply policy is when creating the agent tool:

```python
tool = db.as_tool(
    read_only=True,
    allowed_schemas=("analytics",),
    allowed_tables=("analytics.customers", "analytics.orders"),
    denied_tables=("analytics.payroll",),
    max_rows=100,
    timeout=15,
    max_bytes_scanned=10_000_000_000,
    max_cost_usd=1.00,
)
```

The tool owns this policy. The agent receives a small `describe` and `query` surface, but it does
not receive the connection, provider client, or credentials.

For application code, construct the same policy explicitly:

```python
policy = gantry.sql.SQLPolicy(
    read_only=True,
    allowed_schemas=("analytics",),
    max_rows=100,
    timeout_seconds=15,
)

result = await db.query(
    "SELECT customer_id, total FROM analytics.orders",
    policy=policy,
)
```

`db.as_tool(timeout=...)` maps to `SQLPolicy.timeout_seconds`.

## Policy fields

| Field | Default | Guarantee |
| --- | ---: | --- |
| `read_only` | `True` | Reject write statements and require a native read-only execution boundary. |
| `allowed_schemas` | empty | When set, every referenced table must be qualified with an allowed schema. |
| `allowed_tables` | empty | When set, every referenced table must match an allowed base or qualified name. |
| `denied_tables` | empty | Reject matching base or qualified table names. |
| `max_rows` | `1000` | Materialize at most this many rows in the agent result. |
| `timeout_seconds` | `30` | Require a native timeout or a reconnectable job Gantry can monitor and cancel. |
| `max_bytes_scanned` | `None` | Reject work whose native estimate exceeds the byte limit. |
| `max_cost_usd` | `None` | Reject work whose native estimate exceeds the cost limit. |
| `allow_multiple_statements` | `False` | Reject more than one SQL statement by default. |

Schema and table names are compared case-insensitively. An allowlist that cannot be evaluated
safely rejects the statement. For example, when `allowed_schemas` is set, use
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

To permit writes deliberately, use an appropriately scoped database identity and opt in:

```python
write_tool = db.as_tool(
    read_only=False,
    allowed_schemas=("agent_scratch",),
    allowed_tables=("agent_scratch.results",),
    timeout=15,
)
```

This succeeds only if the adapter declares write support. Gantry does not grant database
permissions; the configured identity must already have them.

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
result = await tool("DELETE FROM analytics.orders")

if result.status is gantry.ResultStatus.REJECTED:
    print(result.failure.kind)
    print(result.failure.message)
```

Operational failures use normalized kinds such as `AUTH_ERROR`, `OBJECT_NOT_FOUND`,
`SYNTAX_ERROR`, `TIMEOUT`, and `ENGINE_ERROR`, while native details remain available for
diagnosis.

## Expose only the operations the agent needs

The default tool operations are `describe` and `query`. They can be narrowed further:

```python
schema_tool = db.as_tool(
    operations=("describe",),
    read_only=True,
)
```

Available operations are `describe`, `query`, and `explain`. Omitting an operation removes it
from the tool schema and causes direct attempts to invoke it to fail.

## Production checklist

- Use a dedicated agent role or service identity.
- Grant access only to required catalogs, schemas, tables, and views.
- Keep `read_only=True` unless writes are an explicit product requirement.
- Set row and timeout limits for every exposed query tool.
- Use byte and cost limits where the provider can estimate them.
- Keep credentials in server-side configuration, never prompts or tool arguments.
- Log rejection and normalized failure metadata without logging secrets.
- Test representative allowed, denied, expensive, and malformed statements before deployment.
