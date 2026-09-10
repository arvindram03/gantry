# Capability matrix

A policy is a claim about what should be allowed. A capability is a claim about
what the adapter can actually enforce. Gantry refuses a statement when the two
disagree — declaring `read_only=True` against an adapter that cannot hold a
read-only session is rejected rather than trusted, because a promise the engine
does not keep is worse than no promise at all.

The tables below are generated from the source by
[`scripts/capability_matrix.py`](https://github.com/arvindram03/gantry/blob/main/scripts/capability_matrix.py),
and a test fails when they drift from it. Regenerate with:

```bash
python scripts/capability_matrix.py --write
```

<!-- generated: capability matrix -->

### What a policy field needs

| Policy field | Capability required | Refusal when absent |
| --- | --- | --- |
| `read_only` | `read_only_session` | adapter cannot enforce a read-only session |
| `max_rows` | `row_limit` | adapter cannot enforce the row limit |
| `timeout_seconds` | `statement_timeout, or reconnect and cancellation` | adapter cannot enforce or monitor the statement timeout |
| `max_bytes_scanned` | `bytes_scanned` | adapter cannot enforce maximum bytes scanned |
| `max_cost_usd` | `cost_limit` | adapter cannot enforce maximum cost |

### What each provider declares

| Capability | PostgreSQL, Neon, Supabase | BigQuery | Snowflake | DuckDB |
| --- | --- | --- | --- | --- |
| `describe_schema` | yes | yes | yes | yes |
| `explain` | yes | yes | yes | yes |
| `dry_run` | no | yes | no | no |
| `async_jobs` | no | yes | yes | no |
| `reconnect` | no | yes | yes | no |
| `cancellation` | yes | yes | yes | yes |
| `read_only_session` | yes | yes | read-only conn | read-only conn |
| `write_execution` | yes | yes | writable conn | writable conn |
| `statement_timeout` | yes | no | yes | yes |
| `row_limit` | yes | yes | yes | yes |
| `cost_estimate` | no | conditional | no | no |
| `cost_limit` | no | conditional | no | no |
| `bytes_scanned` | no | yes | no | no |
| `query_metrics` | yes | yes | yes | yes |
| `result_reference` | no | yes | yes | no |
| `create_table_as` | no | yes | no | writable conn |
| `create_view_as` | no | yes | no | writable conn |
| `destination_introspection` | no | yes | no | yes |
| `materialization_reference` | no | yes | no | writable conn |

A cell reading *read-only conn* or *writable conn* is declared conditionally: the adapter has the capability only when the connection was opened that way. `gantry.sql.connect("duckdb", path=..., read_only=True)` is what makes DuckDB able to hold a read-only session, and a connection that was not opened read-only is refused rather than trusted.

<!-- /generated -->

## What this is not

Capabilities describe enforcement, not permission. Database roles, IAM and
scoped credentials remain the primary control — a connection handed to Gantry
can do whatever that connection is allowed to do. See
[SECURITY.md](https://github.com/arvindram03/gantry/blob/main/SECURITY.md).
