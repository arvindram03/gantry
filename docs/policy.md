# Policy

Gantry treats agent-generated work as a proposal. Policy decides whether it may
happen at all, before anything reaches an engine.

> **Policy controls authority. Verification controls acceptance. Evidence explains why.**

Policy is a guardrail around execution, not a replacement for database
authorization. Always use least-privilege roles, scoped IAM, network controls,
and provider resource limits underneath it.

## Two layers

Small deployments need one operation with bounds on it. Larger ones need the
same rules across many operations and engines. Both are trusted configuration,
and they compose:

```text
simple use  ->  operation configuration    db.query(schemas=[...], max_rows=100)
larger use  ->  reusable policy            gantry.sql.connect(..., policy=policy)
```

Attaching a policy never relaxes what a call site configured. The effective
authority is the narrower of the two, always.

## A reusable policy

```python
policy = gantry.Policy(
    name="data-agents",
    rules=[
        gantry.allow.query(sources=["analytics.*"]),
        gantry.allow.materialize(sources=["raw.*"], destinations=["agent_scratch.*"]),
        gantry.deny.materialize(destinations=["prod.*"]),
    ],
)

db = gantry.sql.connect("postgres", url=DATABASE_URL, policy=policy)
mongo = gantry.nosql.connect("mongodb", uri=URI, database="analytics", policy=policy)
stream = gantry.stream.connect("flink", endpoint=GATEWAY, policy=policy)
```

Execution is unchanged. Evaluation is automatic:

```python
query = db.query()
run = await query("SELECT * FROM analytics.customers", verify=[gantry.verify.row_count(min=1)])
```

### Semantics

```text
a matching DENY      ->  DENY
otherwise an ALLOW   ->  ALLOW
otherwise            ->  DENY
```

An explicit policy is default-deny, and every resource the proposal touches must
be authorized on its own:

```sql
SELECT ... FROM analytics.orders o JOIN finance.customers c ON ...
```

Allowing `analytics.*` does not authorize `finance.customers`. The whole proposal
is denied if any resource it touches is outside authority.

Reads compose across rules — two rules each naming one schema together authorize
a join over both. Writes do not: a rule grants write authority only when its own
`sources` cover everything the proposal reads, so rules allowing `raw.* ->
scratch_a.*` and `other.* -> scratch_b.*` never combine into permission to move
`raw` data into `scratch_b`.

Rules narrow on each dimension they name — `actors`, `operations`, `engines`,
`sources`, `destinations`, `environments`, `constraints` — and grant nothing on a
dimension they do not. One asymmetry is deliberate: an allow rule that names no
`destinations` authorizes **no** writes. Reading the wrong table leaks; writing
the wrong table destroys something, so write authority is never granted by
omission.

### Resource patterns

Three forms, no regex:

| Pattern | Matches |
| --- | --- |
| `events` | exactly that resource |
| `analytics.*` | anything inside the `analytics` namespace, at any depth |
| `derived_*` | resources in this namespace whose name starts that way |

Providers normalize native resources to stable names, and patterns are written
against those:

```text
PostgreSQL   analytics.customers
BigQuery     project.dataset.table
MongoDB      database.collection
Flink        catalog.database.table
```

Matching is case-insensitive. An unqualified name is matched as written — Gantry
does not resolve a search path or a default schema to decide whether a rule
applies, so `customers` does not match `analytics.*`. Flink is the one place
Gantry fills anything in: a bare table name is qualified with the connection's
`default_catalog` and `default_database`, so the spelling the SQL happened to use
cannot decide the outcome. A Flink identifier that already contains a dot is used
exactly as written, because Flink allows one identifier to contain dots and
counting them cannot tell a qualified name from a name with a dot in it.

### Constraints

A rule's `constraints` are ceilings the operation must already be under:

```python
gantry.allow.query(sources=["analytics.*"], constraints={"max_rows": 1_000})
```

```text
policy max_rows      1000
operation max_rows    100
effective             100
```

A rule that bounds something the operation did not bound cannot apply: nothing
would hold the ceiling, so the request is denied with `CONSTRAINT_EXCEEDED`.
Trusted constraints compose toward less authority, never more.

## Actor and environment

Both come from the application, through trusted context:

```python
with gantry.actor.context(
    actor=gantry.actor.actor("agent", "research-agent"),
    environment="prod",
):
    run = await query(sql)
```

Neither is a tool argument. An agent that could name its own actor could name
someone else's, and one that could name its own environment could call
production staging. No environment is not `dev`: a rule scoped to an environment
does not apply when the application set none.

## Resource extraction

The agent does not authoritatively declare what its proposal touches. Gantry
derives it:

```sql
INSERT INTO prod.orders SELECT * FROM raw.orders
```

```text
input   raw.orders
output  prod.orders
```

The same holds for MongoDB `$lookup`, `$out` and `$merge`, and for Flink SQL
sources and sinks. Where the effect cannot be determined safely — an
unclassifiable statement, a multi-statement submission whose reads and writes are
no longer separable — admission fails closed with `RESOURCE_UNRESOLVED`.

Policy governs the resources inspection names. A statement that touches no table
— `SELECT 1` — has nothing for a rule to match, and is admitted when a rule
otherwise applies to the actor, operation and environment. Engine-native side
effects that are not table references, such as a function that reads a file, are
the classifier's business rather than policy's: `read_only` refuses the ones it
recognizes. Least-privilege database roles remain the layer underneath, not an
optional extra.

## Reason codes

Every decision is structured. A denial names the resource and the rule:

```python
run.admission.allowed        # False
run.admission.codes          # ("DESTINATION_DENIED",)
run.admission.matched_rules  # ("deny-materialize-2",)
run.admission.reasons        # ("writing prod.orders is denied by rule deny-materialize-2",)
```

| Code | Means |
| --- | --- |
| `NO_MATCHING_ALLOW` | nothing in the policy authorizes this |
| `EXPLICIT_DENY` | a deny rule matched |
| `ACTOR_DENIED` | no rule allows this actor, or a deny named it |
| `OPERATION_DENIED` | no rule allows this operation kind |
| `SOURCE_DENIED` | a deny rule named a source that is read |
| `DESTINATION_DENIED` | a deny rule named a destination that is written |
| `ENVIRONMENT_DENIED` | the environment is outside every rule, or a deny named it |
| `CONSTRAINT_EXCEEDED` | the operation's limits exceed every rule's ceiling |
| `RESOURCE_UNRESOLVED` | a policy-relevant effect could not be determined |
| `POLICY_INVALID` | the policy could not be evaluated |

## What lands on the run

`run.admission` carries the decision, whether it allowed or refused:

```text
Run run_01M2…

Actor
  agent:etl-agent

Operation
  materialize

Admission
  ✗ refused
  policy: data-agents
    ✓ read raw.orders
    ✗ write prod.orders
    DESTINATION_DENIED
      writing prod.orders is denied by rule deny-materialize-2

Decision
  POLICY_REJECTED
```

The policy name and the version that decided are both recorded. v0 does not
re-evaluate work that was already admitted, so a run stays explainable after the
rules change: editing them produces a different `sha256:…`, and the run keeps
pointing at the one that decided it.

Inputs are recorded on an admitted run; outputs are recorded when the run
produces them. A destination that was authorized is not a destination that
exists, so the one a refused proposal wanted appears in
`run.admission.request["outputs"]` rather than in `run.outputs`.

## Invalid policies

A policy that cannot mean anything raises `PolicyConfigurationError` where it is
written, not at admission:

```python
gantry.allow.query(sources=["ana*lytics"])   # a star inside a name
gantry.allow.materialize(destinations=[])    # could never authorize a write
gantry.allow.query(constraints={"max_joins": 3})  # not a bounded constraint
```

## Per-operation configuration

A policy object is not required. Bounds configured on the operation govern
admission on their own, and keep applying when a policy is attached:

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

The agent sees only the tool's `sql` argument. It does not receive the
connection, provider client, credentials, policy, or any policy fields — a tool
call carrying `policy` is refused rather than ignored.

`SQLPolicy` remains the internal authority model used by adapters and
lower-level execution APIs:

```python
policy = gantry.sql.SQLPolicy(
    read_only=True,
    allowed_schemas=("analytics",),
    max_rows=100,
    timeout_seconds=15,
)
```

### Operation fields

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

Every operation follows the same path:

```text
classify the proposal
    ↓
resolve what it touches: inputs, outputs, effects
    ↓
evaluate the reusable policy, if one is attached
    ↓
check the operation's own scope and bounds
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
