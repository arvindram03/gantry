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

Verified against a live Supabase project (PostgreSQL 17.6, direct endpoint).

```bash
export GANTRY_PROVIDER=supabase
export GANTRY_DATABASE_URL="postgresql://postgres:PASSWORD@db.PROJECT_REF.supabase.co:5432/postgres"
psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
python examples/agent_sql.py
```

Three endpoints, and the choice changes what you must pass:

| Endpoint | Host / port | Statement cache | Verified |
|---|---|---|---|
| Direct | `db.REF.supabase.co:5432` | keep it | **yes** |
| Session pooler | `REGION.pooler.supabase.com:5432`, user `postgres.REF` | keep it | no |
| Transaction pooler | same host, `:6543` | **`statement_cache_size=0`** | no |

- **TLS is required** on all three.
- **The direct host is IPv6-only.** Measured: it publishes an `AAAA` record and
  **no `A` record at all**. It works from a machine with IPv6 and fails from one
  without — including plenty of CI runners — with a name-resolution error that
  does not mention IPv6. Use a pooler endpoint from IPv4-only networks.
- **`statement_cache_size=0` belongs to the transaction pooler, not to
  Supabase.** Only port 6543 recycles the session between statements, which is
  what stops a server-side prepared statement from surviving; asyncpg prepares
  every statement. On the direct host the cache is fine — measured, 30
  concurrent distinct queries, no failures — and turning it off there gives up
  caching for nothing. The example keys this off the port for that reason.

The transaction-pooler row is still from Supabase's documentation rather than
measurement: the project tested was reached directly. **Note that the equivalent
claim proved false for Neon**, whose pooler does support protocol-level prepared
statements — so if you run against Supabase's 6543 endpoint and it behaves the
same way, this workaround should go.

### Running the tests against a hosted database

The live PostgreSQL tests point wherever you send them, which is a quick check
that a provider preset and its TLS settings are right:

```bash
GANTRY_TEST_POSTGRES_PROVIDER=neon \
GANTRY_TEST_POSTGRES_URL="postgresql://..." \
  pytest tests/test_postgres_live.py -q
```

They skip when nothing is reachable, so a clone with no database still passes.
