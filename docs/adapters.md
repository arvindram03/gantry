# Custom SQL adapters

A custom adapter connects Gantry's governed SQL interface to an internal database, warehouse,
query service, or managed platform. SQL stays native to that target. The adapter is responsible
for execution and for declaring only the guarantees it can enforce.

## The contract

A SQL adapter implements eight methods:

```python
from typing import Protocol

from gantry import Context, Execution, ExecutionHandle, ExecutionResult, ValidationResult
from gantry.sql import (
    DatabaseSchema,
    ExplainResult,
    SQLCapabilities,
    SQLPolicy,
    SQLTarget,
)


class SQLAdapter(Protocol):
    def capabilities(self) -> SQLCapabilities: ...

    async def describe(self, target: SQLTarget) -> DatabaseSchema: ...

    async def validate(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
        policy: SQLPolicy,
    ) -> ValidationResult: ...

    async def explain(self, sql: str, target: SQLTarget) -> ExplainResult: ...

    async def submit(
        self,
        sql: str,
        target: SQLTarget,
        context: Context,
    ) -> ExecutionHandle: ...

    async def status(self, handle: ExecutionHandle) -> Execution: ...

    async def result(self, handle: ExecutionHandle) -> ExecutionResult: ...

    async def cancel(
        self,
        handle: ExecutionHandle,
        mode: str = "default",
    ) -> Execution: ...
```

The protocol is structural: the adapter does not need to inherit from a Gantry base class.

## Declare capabilities accurately

Capabilities participate directly in admission:

```python
def capabilities(self) -> SQLCapabilities:
    return SQLCapabilities(
        describe_schema=True,
        explain=True,
        async_jobs=True,
        reconnect=True,
        cancellation=True,
        read_only_session=True,
        write_execution=False,
        statement_timeout=True,
        row_limit=True,
        bytes_scanned=True,
        query_metrics=True,
        result_reference=True,
    )
```

Set a field to `True` only when the adapter has a concrete implementation for it. For example:

- `read_only_session` requires an engine-enforced read-only session, transaction, or equivalent.
- `row_limit` means the adapter never returns more than `policy.max_rows` inline rows.
- `statement_timeout` means the target or adapter enforces `policy.timeout_seconds`.
- `reconnect` means a fresh adapter process can recover the job from the handle.
- `result_reference` means large outputs can stay in the target system.
- `cost_limit` requires a usable native estimate, not merely post-execution billing data.

If a requested policy guarantee has no matching capability, Gantry rejects the run before
submission.

## Validation and policy

Gantry performs common classification and scope checks before calling the adapter. The adapter's
`validate` method should add target-native validation—for example a parse request, dry run, or
`EXPLAIN`—without executing the statement.

```python
async def validate(self, sql, target, context, policy):
    try:
        await self._client.explain(sql)
    except AcmeSyntaxError as error:
        return gantry.ValidationResult.rejected(str(error))
    return gantry.ValidationResult.accepted()
```

If `max_bytes_scanned` or `max_cost_usd` is supported, return the estimate from `explain`:

```python
async def explain(self, sql, target):
    estimate = await self._client.dry_run(sql)
    return gantry.sql.ExplainResult(
        supported=True,
        estimated_bytes=estimate.bytes,
        estimated_cost=estimate.cost_usd,
        native={"plan_id": estimate.plan_id},
    )
```

Never mutate or rewrite the submitted SQL during validation. The validated, engine-native
statement should be the statement that is executed.

## Submit and return a durable handle

`submit` receives the active `SQLPolicy` in `context.metadata["gantry.sql.policy"]`. Enforce its
timeout and result bound at the native execution boundary.

```python
from uuid import uuid4


async def submit(self, sql, target, context):
    policy = context.metadata["gantry.sql.policy"]
    job = await self._client.submit(
        sql,
        timeout_seconds=policy.timeout_seconds,
        max_rows=policy.max_rows,
    )
    return gantry.ExecutionHandle(
        gantry_id=f"run_{uuid4().hex}",
        engine="sql",
        target=target.provider,
        native_id=job.id,
        metadata={"max_rows": policy.max_rows},
    )
```

The handle must satisfy these invariants:

- `target` equals the registered provider name.
- `native_id` is the provider's durable query or job ID when reconnect is declared.
- Metadata contains only the minimum non-secret information needed for recovery.
- Credentials, tokens, connection strings, raw clients, and SQL result rows never enter it.

For process-local engines, declare `reconnect=False` and keep in-flight task state inside the
adapter.

## Status, results, cancellation, and failures

Map provider state to Gantry's small lifecycle:

```text
PENDING · SUBMITTED · RUNNING · SUCCEEDED · FAILED · CANCELLED · UNKNOWN
```

Preserve detailed provider state under `Execution.native`. Return normalized metrics where they
are available.

`result` returns an `ExecutionResult`. Inline rows must be bounded before construction:

```python
inline = gantry.sql.InlineRows(
    columns=("customer_id", "total"),
    rows=tuple(native_rows[: policy.max_rows]),
    truncated=len(native_rows) > policy.max_rows,
)

return gantry.ExecutionResult.succeeded(
    handle,
    outputs=(
        gantry.OutputRef(
            gantry.OutputKind.INLINE,
            f"inline://{handle.gantry_id}",
            metadata={"inline": inline},
        ),
    ),
)
```

Prefer bounded fetching from the provider (`max_rows + 1`) rather than downloading an unbounded
result and slicing it afterward. For large results, return an `OutputRef` such as a warehouse
table or object URI and keep the data outside Gantry.

Failures should use `FailureKind` and retain safe native diagnostic details:

```python
failure = gantry.Failure(
    kind=gantry.FailureKind.AUTH_ERROR,
    retryable=False,
    message="target rejected the configured identity",
    native_code=error.code,
)
```

An execution and its result must return the exact handle supplied by the caller. Cancellation
should be idempotent where the provider permits it.

## Register a preconfigured adapter

When one adapter instance owns one target configuration:

```python
adapter = AcmeAdapter(
    endpoint=os.environ["ACME_ENDPOINT"],
    token=os.environ["ACME_TOKEN"],
)

gantry.sql.register(
    "acme",
    adapter=adapter,
    dialect="postgres",
)

db = gantry.sql.connect("acme")
tool = db.as_tool(read_only=True, max_rows=100, timeout=15)
```

The adapter remains private inside `SQLConnection`; the agent receives only `tool`.

## Register an adapter factory

Use `register_provider` when each connection should create an adapter from provider config:

```python
from collections.abc import Mapping


def validate_config(config: Mapping[str, object]) -> None:
    allowed = {"endpoint", "token"}
    unknown = set(config) - allowed
    if unknown:
        raise ValueError(f"unknown Acme config: {', '.join(sorted(unknown))}")
    if not isinstance(config.get("endpoint"), str):
        raise ValueError("Acme endpoint is required")


gantry.sql.register_provider(
    "acme",
    dialect="postgres",
    driver="acme-sdk",
    adapter_factory=AcmeAdapter,
    validate_config=validate_config,
)

db = gantry.sql.connect(
    "acme",
    endpoint=os.environ["ACME_ENDPOINT"],
    token=os.environ["ACME_TOKEN"],
)
```

Validate configuration before creating a network client. Keep secrets only in `SQLTarget.config`
and adapter-private state; do not expose a config property on the connection or tool.

## Reuse or register a dialect

Providers can reuse a built-in conservative dialect classifier:

```text
postgres · mysql · sqlserver · bigquery · snowflake · duckdb
```

If the target requires different parsing or object discovery, implement `SQLDialect` and register
it before the provider:

```python
gantry.sql.register_dialect("acme-sql", AcmeDialect())
```

A custom dialect implements `parse`, `classify`, and `referenced_objects`. It classifies and
extracts references for policy checks; it must not transpile the query.

## Adapter checklist

- Validate target configuration and fail on unknown fields.
- Keep clients and credentials private.
- Declare only capabilities with real enforcement.
- Validate with the native parser, planner, dry run, or `EXPLAIN`.
- Execute the exact SQL that was validated.
- Enforce `max_rows` before materializing agent-visible results.
- Return references for large outputs.
- Use durable native IDs when declaring reconnect support.
- Normalize state, metrics, and failures while retaining safe native details.
- Test rejection, timeout, truncation, cancellation, recovery, and credential isolation.
