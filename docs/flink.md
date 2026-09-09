# Gantry Flink SQL

`gantry.flink` is a small control surface around an existing Apache Flink cluster. Flink owns
planning, connectors, execution, data movement, and sink writes. Gantry validates and submits
Flink SQL, captures the native JobID, observes and cancels the job, and evaluates lightweight
health checks.

## Running against a real cluster

`tests/test_flink_live.py` exercises the adapter against an actual Flink SQL
Gateway, and `examples/flink/docker-compose.yml` brings one up:

```bash
docker compose -f examples/flink/docker-compose.yml up -d
pytest tests/test_flink_live.py -q
```

They skip when no gateway is reachable, so a clone without one still passes.

Three details cost real time to find, and none of them are visible from a
fake transport:

- **`rest.address` and `rest.port` must point the gateway at the JobManager.**
  Without them it submits to its own loopback and every job fails with
  `Connection refused` after exhausting retries — long after the statement was
  accepted.
- **The gateway's default catalog is per-session and in-memory.** A table
  created in one session is invisible to the next, so Gantry's one-statement
  boundary leaves nowhere to create one. A file catalog store plus a JDBC
  catalog gives the statement tables that outlive the session.
- **A batch job takes longer to deploy than the 30-second default timeout**, so
  `submission_timeout` needs raising for anything real.


## Connect

Flink exposes two REST surfaces with different responsibilities:

- SQL Gateway accepts sessions and SQL statements.
- JobManager REST reports durable job state and metrics and accepts cancellation by JobID.

Pass the SQL Gateway URL as `endpoint`. If JobManager REST has a different URL, configure it
explicitly:

```python
import gantry

flink = gantry.flink.connect(
    "https://sql-gateway.acme.internal",
    jobmanager_endpoint="https://jobmanager.acme.internal",
    request_timeout=30,
    token=os.environ["FLINK_TOKEN"],
)
```

If a gateway or reverse proxy exposes both APIs at one origin, `jobmanager_endpoint` defaults to
`endpoint`. Supported authentication inputs are `token`, `basic_auth=(username, password)`,
string `headers`, an `ssl.SSLContext` for TLS or mTLS, or a custom HTTP `transport` for platform
identity. Configuration and credentials stay inside the adapter and do not appear in handles or
agent tool schemas.

The SQL Gateway REST version defaults to `v2` and can be changed with `api_version`.

## Validate and submit

Use a string for the smallest path:

```python
validation = await flink.validate("INSERT INTO clean SELECT * FROM raw")
handle = await flink.submit("INSERT INTO clean SELECT * FROM raw")
```

Use an artifact when mode or declarations matter:

```python
artifact = gantry.flink.FlinkSQLArtifact(
    sql="INSERT INTO clean_events SELECT * FROM raw_events",
    mode="streaming",
    declared_inputs=("raw_events",),
    declared_outputs=("clean_events",),
)

validation = await flink.validate(artifact)
handle = await flink.submit(artifact)
```

Gantry v0 accepts exactly one `INSERT INTO` or `INSERT OVERWRITE` statement. It performs a
conservative local statement check, then submits `EXPLAIN PLAN FOR ...` to Flink. Flink's planner
is authoritative for syntax, catalogs, databases, tables, connector discovery, and sink
resolution. Validation does not execute the insert.

`default_catalog` and `default_database` are applied to each SQL Gateway session with `USE`
statements. `session_properties` and `execution_config` accept string mappings. Gantry forces
detached submission unless `execution.attached` is explicitly overridden, allowing lifecycle
operations to use the returned JobID after the Gateway session closes.

## Run and recover

`run()` validates, submits, and then waits for readiness or completion:

```python
result = await flink.run(
    artifact,
    poll_interval_seconds=1,
    timeout_seconds=120,
)
```

For streaming mode, readiness is `RUNNING`; a healthy streaming job is returned as
`ResultStatus.ACCEPTED`. For batch mode, Gantry waits for Flink's `FINISHED` state. A streaming
job is not expected to reach `SUCCEEDED`.

The handle stores the Flink JobID and non-secret output metadata. It does not depend on an
in-memory task, SQL Gateway session, or submitting process:

```python
# This can be a fresh process with the same target configuration.
flink = gantry.flink.connect(
    "https://sql-gateway.acme.internal",
    jobmanager_endpoint="https://jobmanager.acme.internal",
    token=os.environ["FLINK_TOKEN"],
)

execution = await flink.status(saved_handle)
health = await flink.health(saved_handle)
cancelled = await flink.cancel(saved_handle)
```

Native Flink state remains in `execution.native`. Gantry normalizes the lifecycle to `PENDING`,
`RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, or `UNKNOWN` and maps common failures such as
authentication, missing jobs, connector errors, resource errors, user-code errors, and engine
errors.

## Health and metrics

The initial metric envelope contains:

```text
records_in
records_out
runtime_seconds
restart_count
watermark_lag_seconds
native
```

Metric availability depends on the Flink cluster, reporters, and job topology. Missing metrics
remain `None`; a check that requires a missing metric fails closed. Job-level restart and uptime
metrics and aggregated vertex record, rate, and watermark metrics remain under `native` as well.

```python
checks = (
    gantry.flink.MaxRestartCount(3),
    gantry.flink.MaxWatermarkLag("60s"),
    gantry.flink.MinOutputRate(1.0),
)

health = await flink.health(handle, checks=checks)
verification = await flink.verify(handle, checks=checks)
```

Durations for `MaxWatermarkLag` accept numbers in seconds or strings ending in `ms`, `s`, `m`,
or `h`.

## Output references

Gantry never reads sink records. Declared outputs become references such as
`flink-table:///clean_events`. Applications can supply authoritative references:

```python
flink = gantry.flink.connect(
    "https://sql-gateway.acme.internal",
    output_refs={
        "clean_events": "kafka://clean-events",
    },
)
```

If an artifact has no `declared_outputs`, Gantry conservatively uses the insert target as the
logical output name. The reference describes where the output lives; data never flows through
Gantry.

## Agent tool

```python
tool = flink.as_tool(
    checks=(gantry.flink.MaxRestartCount(3),),
    timeout=120,
)

result = await tool(
    "INSERT INTO clean_events SELECT * FROM raw_events",
    declared_outputs=("clean_events",),
)
```

The framework-neutral tool supports `validate`, `run`, `status`, `health`, and `cancel`. It owns
the connection privately and exposes neither raw HTTP clients nor credentials.

## v0 boundary

The adapter does not provide PyFlink execution, JAR upload, savepoints, checkpoint management,
autoscaling, cluster deployment, catalog management, connector installation, or a transformation
DSL.

See the Apache Flink documentation for the
[SQL Gateway REST API](https://nightlies.apache.org/flink/flink-docs-stable/docs/sql/interfaces/sql-gateway/rest/),
[JobManager REST API](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/),
and [metric REST endpoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/).
