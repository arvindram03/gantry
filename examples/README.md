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

```bash
export GANTRY_PROVIDER=supabase
export GANTRY_DATABASE_URL="postgresql://postgres.PROJECT_REF:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres"
python examples/agent_sql.py
```

Supabase offers three endpoints and the choice matters here:

| Endpoint | Port | Notes |
|---|---|---|
| Direct | 5432 | `db.PROJECT_REF.supabase.co`. Often IPv6-only |
| Session pooler | 5432 | `aws-0-REGION.pooler.supabase.com`, user `postgres.PROJECT_REF` |
| Transaction pooler | 6543 | Same host, best for many short-lived connections |

**On the transaction pooler, pass `statement_cache_size=0`.** asyncpg prepares
every statement server-side, and transaction-mode pooling does not keep a
session alive long enough for those to survive — without it you get
intermittent `prepared statement does not exist` failures under load rather
than a clean error at connect time. The example does this for you.

The session pooler and direct connections have no such constraint, and keep
statement caching.

**Not verified against a live Supabase project.** Unlike the Neon notes above,
this is from Supabase's documented pooler behaviour rather than measurement. If
you have a project, `tests/test_postgres_live.py` pointed at it is the quickest
way to confirm — and if the transaction pooler turns out to handle prepared
statements the way Neon's now does, this workaround should go.

### Running the tests against a hosted database

The live PostgreSQL tests point wherever you send them, which is a quick check
that a provider preset and its TLS settings are right:

```bash
GANTRY_TEST_POSTGRES_PROVIDER=neon \
GANTRY_TEST_POSTGRES_URL="postgresql://..." \
  pytest tests/test_postgres_live.py -q
```

They skip when nothing is reachable, so a clone with no database still passes.
