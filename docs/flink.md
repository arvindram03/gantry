# Flink SQL: batch and stream

Gantry exposes Flink through the execution model you need:

```python
gantry.batch.connect("flink", ...)
gantry.stream.connect("flink", ...)
```

Both surfaces run native Flink SQL through the same internal adapter. Flink still owns planning,
connectors, execution, and data movement. Gantry owns admission, durable job identity, observation,
verification, normalized failures, and the narrow interface exposed to an agent.

## Batch

```python
import gantry

batch = gantry.batch.connect(
    "flink",
    endpoint="https://sql-gateway.acme.internal",
    jobmanager_endpoint="https://jobmanager.acme.internal",
    token=os.environ["FLINK_TOKEN"],
)

daily_orders = batch.job(
    inputs=["raw.orders"],
    outputs=["analytics.daily_orders"],
    checks=[
        gantry.verify.output_exists(),
        gantry.verify.row_count(min=1),
        gantry.verify.required_columns(["order_date", "orders"]),
    ],
    timeout=300,
)

result = await daily_orders("""
INSERT INTO analytics.daily_orders
SELECT CAST(order_time AS DATE), COUNT(*)
FROM raw.orders
GROUP BY CAST(order_time AS DATE)
""")
```

Batch supports one `INSERT INTO ... SELECT` or `INSERT OVERWRITE ... SELECT` statement. Acceptance
requires the Flink job to finish successfully and every configured output check to pass.

## Stream

```python
stream = gantry.stream.connect(
    "flink",
    endpoint="https://sql-gateway.acme.internal",
    jobmanager_endpoint="https://jobmanager.acme.internal",
    token=os.environ["FLINK_TOKEN"],
)

clean_events = stream.job(
    inputs=["raw.events"],
    outputs=["clean.events"],
    checks=[
        gantry.verify.running(),
        gantry.verify.restart_count(max=3),
        gantry.verify.watermark_lag(max_seconds=60),
    ],
    timeout=120,
)

result = await clean_events("""
INSERT INTO clean.events
SELECT * FROM raw.events WHERE event_type IS NOT NULL
""")
```

A stream is accepted when it reaches `RUNNING` and passes its health checks. It remains active after
Gantry returns. Stream v0 rejects `INSERT OVERWRITE`.

## Direct calls and agent tools

Configured jobs are both callable and tool-ready:

```python
result = await clean_events(sql)
tools = [clean_events.tool()]
```

The tool schema contains only `sql`. Endpoint, credentials, batch/stream mode, input scope, output
scope, timeouts, and checks remain trusted application configuration and cannot be changed by the
agent.

## Scope enforcement

Gantry extracts the actual source and sink objects from each proposal before contacting Flink:

```python
job = stream.job(inputs=["raw.*"], outputs=["clean.*"])
```

- `INSERT INTO clean.events SELECT * FROM raw.events` is in scope.
- Reading `finance.events` fails with `INPUT_NOT_ALLOWED`.
- Writing `finance.events` fails with `OUTPUT_NOT_ALLOWED`.
- DDL, multiple statements, and streaming overwrite fail with `OPERATION_NOT_ALLOWED`.

Flink's planner remains authoritative for dialect syntax, catalogs, columns, connector discovery,
and sink resolution. Gantry submits `EXPLAIN PLAN FOR ...` after local admission and before running
the statement.

## Durable lifecycle

```python
handle = await clean_events.submit(sql)

status = await clean_events.status(handle)
metrics = await clean_events.metrics(handle)
health = await clean_events.health(handle)
cancelled = await clean_events.cancel(handle)
```

The handle contains the native Flink JobID and non-secret metadata. A fresh process can construct
the same configured job and observe or cancel that handle; it does not need the original SQL Gateway
session.

Normalized metrics are `runtime_seconds`, `records_in`, `records_out`, `restart_count`, and
`watermark_lag_seconds`. Raw Flink details remain available in execution and metric metadata.

## Output references

Gantry returns a reference rather than moving sink data through itself. By default, a batch sink
such as `analytics.daily_orders` becomes `flink-table:///analytics.daily_orders`; a stream sink has
stream output semantics. Supply authoritative URIs when the logical table maps elsewhere:

```python
stream = gantry.stream.connect(
    "flink",
    endpoint=FLINK_ENDPOINT,
    output_refs={"clean.events": "kafka://clean-events"},
)

result = await stream.job(inputs=["raw.events"], outputs=["clean.events"])(sql)
print(result.uri)  # kafka://clean-events
```

## Connection options

The SQL Gateway URL is `endpoint`. `jobmanager_endpoint` defaults to the same origin when a proxy
serves both REST APIs. Supported authentication inputs are `token`,
`basic_auth=(username, password)`, string `headers`, an `ssl.SSLContext`, or a custom HTTP transport.
You can also set `api_version`, `default_catalog`, `default_database`, `session_properties`,
`execution_config`, `request_timeout`, `validation_timeout`, and `submission_timeout`.

## Local integration test

```bash
docker compose -f examples/flink/docker-compose.yml up -d
pytest tests/test_flink_live.py -q
```

The adapter intentionally does not provide a transformation DSL, PyFlink execution, JAR upload,
savepoints, checkpoint management, cluster deployment, catalog management, or connector
installation.
