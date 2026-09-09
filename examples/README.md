# Examples

Each file is runnable and does one thing. Start with the row that matches what
you are trying to do.

| I want to… | Example | Needs |
|---|---|---|
| Let an agent answer questions about a database | [`agent_sql.py`](agent_sql.py) | PostgreSQL (local, Neon, or Supabase) |
| Try all of this with nothing to set up | [`local_duckdb.py`](local_duckdb.py) | a file on disk |
| Let an agent **build** a table, and check it before trusting it | [`materialize_and_verify.py`](materialize_and_verify.py) | a file on disk |
| Run something expensive without holding a request open | [`long_running_query.py`](long_running_query.py) | PostgreSQL |
| Run a continuous job, and know whether it is healthy | [`streaming_flink.py`](streaming_flink.py) | a Flink cluster |

If you are only reading one, read `local_duckdb.py`. It needs nothing, and the
boundary it demonstrates is the same one every other example relies on.

## What they have in common

Every example splits the same way, and it is the reason the library exists:

- **You** configure the connection and the policy. That is application code, it
  holds the credential, and it decides what is allowed.
- **The agent** gets `.tool()` — one input, `sql`. It cannot widen the policy,
  reach another schema, or see the connection string, because none of those are
  arguments it can pass.

The other recurring idea is that *the engine succeeding* and *the answer being
usable* are different questions. A `CREATE TABLE AS` that matched no rows
succeeds. A streaming job that has restarted forty times is `RUNNING`. Gantry's
job is the second question — see `materialize_and_verify.py` for the clearest
case of the two disagreeing.

## Which provider?

`agent_sql.py` runs unchanged against local PostgreSQL, Neon, and Supabase. The
connection details differ in ways that fail confusingly, so they are written out
below.

## `agent_sql.py`## `agent_sql.py` — a governed SQL tool for an agent

The same code against local PostgreSQL, Neon, or Supabase. Only the provider
name and the URL change.

It shows the split the library exists for: **you** configure the connection and
the policy, holding the credential; **the agent** gets a tool whose only input
is `sql`. It cannot widen the policy, reach another schema, or see the
connection string, because none of those are arguments it can pass.

### Run it

```bash
pip install "gantry[postgres]"
psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
python examples/agent_sql.py
```

### Local PostgreSQL

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=gantry -e POSTGRES_USER=gantry \
  -e POSTGRES_DB=gantry -p 5432:5432 postgres:16-alpine

export GANTRY_DATABASE_URL="postgresql://gantry:gantry@localhost:5432/gantry"
python examples/agent_sql.py
```

### Neon

Verified against a live instance (PostgreSQL 18.6) on both endpoints. Either
works; the direct endpoint is the simpler default.

```bash
export GANTRY_PROVIDER=neon
export GANTRY_DATABASE_URL="postgresql://USER:PASSWORD@ep-xxx.REGION.aws.neon.tech/dbname?sslmode=require"
psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
python examples/agent_sql.py
```

| Endpoint | Host | 40 concurrent queries |
|---|---|---|
| Direct | `ep-xxx.REGION.aws.neon.tech` | **0/40 failed**, `max_connections` 901 |
| Pooled | `ep-xxx-pooler.REGION.aws.neon.tech` | **0/40 failed** |

- **TLS is required.** Neon's own connection string carries `sslmode=require`,
  which asyncpg honours; the example also passes `ssl="require"` so a URL
  without it still works. `channel_binding=require` is accepted and ignored.
- **Both endpoints are dual-stack** — `A` and `AAAA` records — so neither needs
  IPv6, unlike Supabase's direct host.
- **Neither needs a statement-cache workaround.** Neon's pooler supports
  protocol-level prepared statements, so the adapter leaves both endpoints
  alone. Do not carry Supabase's `:6543` workaround across; the URLs look alike
  and the answer is different.
- **A compute scaled to zero takes a few seconds to wake**, so the example
  allows a 30-second connect timeout. Not measured here: the instance tested
  was warm, connecting in ~0.7 s on the direct endpoint every time.

Use the pooled endpoint if you expect many more concurrent clients than a single
compute should hold open; the direct endpoint is otherwise one less thing
between you and the database.

### Supabase

Verified against a live project (PostgreSQL 17.6) on the direct host and both
poolers.

```bash
export GANTRY_PROVIDER=supabase
export GANTRY_DATABASE_URL="postgresql://postgres.PROJECT_REF:PASSWORD@REGION.pooler.supabase.com:6543/postgres"
psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
python examples/agent_sql.py
```

**Use the transaction pooler (`:6543`).** Measured, 40 concurrent queries:

| Endpoint | Result |
|---|---|
| Transaction pooler `:6543` | **0/40 failed** |
| Session pooler `:5432` | **25/40 failed** — `max clients reached` |
| Direct `db.REF.supabase.co:5432` | 0/40, but IPv6-only |

The session pooler holds a backend for the life of each client connection. This
library opens a connection per query, so concurrent queries exhaust the pool
almost immediately — the session pooler is the wrong shape for this workload,
not merely slower.

The direct host publishes an `AAAA` record and **no `A` record at all**. It
works from a machine with IPv6 and fails from one without — including plenty of
CI runners — with a resolution error that never mentions IPv6.

**You do not need `statement_cache_size=0`; the adapter sets it for you** on
`:6543`. asyncpg prepares every statement server-side, and a transaction-pooled
backend is often not the one that prepared it.

That workaround is widely described as obsolete, and it is worth knowing why it
is not. Testing it here, 40 concurrent queries passed with the cache on, and so
did one connection reused across 25 transactions — then an ordinary
`describe()` failed on the very next run. Whether it bites depends on which
backend the pooler hands you, so **a passing test is not evidence** and only the
pooling mode is. Pass `statement_cache_size` explicitly to override.

### Running the tests against a hosted database

The live PostgreSQL tests point wherever you send them, which is a quick check
that a provider preset and its TLS settings are right:

```bash
GANTRY_TEST_POSTGRES_PROVIDER=neon \
GANTRY_TEST_POSTGRES_URL="postgresql://..." \
  pytest tests/test_postgres_live.py -q
```

They skip when nothing is reachable, so a clone with no database still passes.
