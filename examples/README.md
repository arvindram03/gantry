# Examples

## `agent_sql.py` — a governed SQL tool for an agent

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

Verified against a live Neon instance (PostgreSQL 18.6, pooled endpoint).

```bash
export GANTRY_PROVIDER=neon
export GANTRY_DATABASE_URL="postgresql://USER:PASSWORD@ep-xxx-pooler.REGION.aws.neon.tech/dbname?sslmode=require&channel_binding=require"
psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
python examples/agent_sql.py
```

- **TLS is required.** Neon's own connection string carries
  `sslmode=require`, which asyncpg honours; the example also passes
  `ssl="require"` so it works with a URL that omits it. `channel_binding=require`
  is accepted and ignored by asyncpg.
- **The pooled endpoint is fine as-is.** The `-pooler` host is PgBouncer in
  transaction mode, but Neon's supports protocol-level prepared statements, so
  asyncpg's statement cache works. Measured: 30 concurrent distinct queries,
  no failures, with the cache on. **This is the opposite of Supabase below** —
  do not carry that workaround over.
- **A compute scaled to zero takes a few seconds to wake**, so the example
  allows a 30-second connect timeout. Not measured here: the instance tested
  was already warm and connected in ~0.6 s.

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
