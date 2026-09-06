# Benchmarks

Measured on the local Docker Compose stack (`make dev-up`), Apple Silicon,
PostgreSQL 16. These are development-machine figures for tracking regressions,
not capacity planning numbers.

Re-run with the commands shown. Update this file when a measurement moves.

## Day 6 — discovery and profiling

Source: `public.orders`, 100,000,000 rows, 10 GB including indexes.

| Operation | Time | Budget |
|---|---|---|
| `discover()` — all tables in a schema | 73 ms | — |
| `profile()` — one 100M-row table | 7 ms | < 60 s |

### Cost is independent of table size

The point is not that profiling is fast, but that its cost does not grow with
the data. Discovery reads `pg_class`; profiling reads `pg_stats` and an indexed
`min`/`max`. All are statistics PostgreSQL already maintains from its own
sample, so no table is ever scanned.

| Table | Rows | `profile()` |
|---|---|---|
| `public.customers` | 1,000,000 | 1.54 ms |
| `public.orders` | 100,000,088 | 1.41 ms |

A hundredfold increase in rows produces no increase in cost; the difference
between the two figures is measurement noise.

**Caveat:** this depends on the source having current statistics. When
`ANALYZE` has not run, `stale_statistics` is set on the manifest so a planner
can tell "no skew" from "no information" — profiling stays fast either way, but
its output is only as good as the sample behind it.

## Seeding

Server-side generation, committed in 5M-row batches so WAL can recycle.

| Rows | Time |
|---|---|
| 1,000,000 | 4.3 s |
| 100,000,000 | ~17 min |

```bash
uv run gantry seed --rows 100000000
```

## Day 7 — partitioning and reads

Same 100M-row source. Partition bounds come from the planner's equi-depth
histogram, so planning reads no rows at all.

| Operation | Result |
|---|---|
| Plan 21 partitions over 100M rows | instant (pure function of the manifest) |
| Row coverage | 100,000,000 — exact, no gaps, no overlap |
| Balance (max/min partition) | **1.35×** |
| Stream one 4M-row partition | 6.5 s — 622,000 rows/sec |

### Why 1.35× and not 1.0×

Histogram resolution sets the floor. PostgreSQL's default
`default_statistics_target` produces 100 equi-depth buckets, so 21 partitions
span either 4 or 5 buckets each — about 1.25× before sampling error. Raising
`default_statistics_target` on the key column buys finer boundaries if a
migration needs them.

An earlier implementation spaced cuts across the *interior* boundaries instead
of across all buckets, which left the edge partitions covering a different
number of buckets than the rest and produced **6.08×**. Coverage was exact in
both cases, which is why balance is measured rather than assumed.

## Day 8 — target writes

962,507 rows (one partition of the 100M-row source), Postgres to Postgres
across two containers. Target for the day was ≥ 50k rows/sec.

| Path | Time | Throughput |
|---|---|---|
| `write_batch` — rows through Python | 10.0 s | **96,044 rows/sec** |
| `copy_partition` — bytes only | 5.8 s | **166,238 rows/sec** |

Both clear the target. The split of the Python path is where the cost actually
sits:

| Stage | Time | Throughput |
|---|---|---|
| read (server-side cursor) | 1.8 s | 527,299 rows/sec |
| write (`unnest` + `ON CONFLICT`) | 8.2 s | 117,434 rows/sec |

### Why two paths

`write_batch` materialises the partition as Python objects. That is fine for
correctness tests and for CDC batches, and unacceptable for a five-million-row
partition — the memory is unbounded and the orchestration layer ends up on the
data path.

`copy_partition` moves the same rows as COPY byte streams through a bounded
queue into a staging table, then merges with one `INSERT ... SELECT`. Python
moves buffers; the engines move rows. It is 1.7× faster and, more importantly,
its memory does not scale with partition size.

### Replay costs almost as much as the first write

| | First write | Replay |
|---|---|---|
| `copy_partition` | 5.5 s | 4.5 s |

A replay changes zero rows but still transfers and stages every one of them to
discover that. Retries are cheap in *effect*, not in *work* — which is an
argument for partitions small enough that replaying one is not expensive.

## Day 10 — M1 scale run

PostgreSQL to PostgreSQL, two containers, from an empty target.

| | |
|---|---|
| Rows | **101,000,000** (100M orders + 1M customers) |
| Wall time | **462.7 s** (7m 43s) |
| Throughput | **218,271 rows/sec** |
| Partitions | 55, all complete |
| Checkpoints | 61 |
| Target vs source | exact — 100,000,000 distinct `order_id` |

```bash
gantry start examples/postgres-to-postgres/movement.yaml --backend temporal
```

### Dispatch backend matters more than expected

Identical work, identical adapters, 1M rows:

| Backend | Throughput |
|---|---|
| Temporal (parallel activities) | 284,433 rows/sec |
| Leased queue (one worker) | 96,044 rows/sec |

The gap is concurrency, not efficiency: the queue path ran one worker at a
time, while Temporal dispatched up to 8 activities in parallel. A multi-worker
queue would close most of it. The figure to take from this is that partition
concurrency dominates, which is why Day 17's adaptive controller tunes it.

### What the earlier interrupted runs cost

The first attempt at this run was interrupted twice by bugs and finished on a
different backend, reporting `7,419 rows/sec`. That number measures re-copying
already-complete partitions to discover they were no-ops, not copying data.
Recorded here because it is the kind of figure that looks like a benchmark and
is not.

## Day 12 — checksums and localisation

### Locating one corrupted row in 10,000,000

| | |
|---|---|
| Comparisons | **27** |
| log₂(10M) | 23 |
| Wall time | 33.5 s |
| Result | key `7654321`, exactly |

The extra four comparisons over the theoretical minimum are the descent into
enumeration once a range falls under 2,000 rows.

The comparisons are not equal in cost: the first scans ten million rows on each
side, the next five million, and so on. Total work is therefore about twice the
table, not 27 full scans — the same shape as the comparison count, one level
down.

### Detect, locate, repair, re-verify

One value changed (not deleted, so no row count can see it) in a 1,000,000-row
target:

| Step | Result |
|---|---|
| `gantry verify` | `chunk_checksum failed` — 1 differing in 19 comparisons, key `543210` |
| `gantry repair … public.customers/00004` | **2.3 s** |
| `gantry verify` | 18/18 checks passed |

Repair costs seconds because it re-copies one partition, not the migration.
That is only safe because the copy is idempotent.

## Day 15 — snapshot and CDC together (M3)

A 50,000-row table snapshotted while writes continued throughout, then caught
up:

| | |
|---|---|
| Snapshot | 50,000 rows in 0.4 s, with 80 live writes during it |
| Catch-up | 279 changes applied in 1.5 s |
| Final lag | **1.48 s** (threshold 2.00 s) |
| Convergence | source and target checksums **identical** |

Verified by checksum rather than row count: a count cannot see a row that is
present on both sides and stale, which is precisely what a mishandled handoff
produces.

**What these lag figures do and do not show.** The broker is a single-node
KRaft Kafka in the same Docker network as the consumer, with no replication and
no network between them. The number demonstrates that the handoff converges and
that lag is measured against the source's own clock; it is not a claim about
lag under production topology, replication, or load.

## Not yet measured

- Adaptive concurrency under target pressure (v1.1)

## v1.2 Day 0 — packaged jobs vs the relay

Docker, one machine, warm images, loopback networking, 1M narrow rows. Not a
production measurement; the point was to decide an architecture, and it did.

| Path | 1M rows | rows/sec |
|---|---|---|
| Containerised job: COPY pipe + staging + upsert merge | **6.30 s** | ~159k |
| The same, split across 8 concurrent partition jobs | **4.41 s** | ~227k |
| v1 relay (from the rehearsal; includes planning and verification) | 8.1 s | ~125k |

Transport only, for the choice between mechanisms:

| Transport | 1M rows | rows/sec |
|---|---|---|
| `COPY … TO STDOUT \| COPY … FROM STDIN` | 1.81 s | ~552k |
| `postgres_fdw`, `fetch_size 50000` | 2.60 s | ~385k |
| `postgres_fdw`, default `fetch_size 100` | — | ~280k |

**`postgres_fdw` was rejected on these numbers**, not on taste: it is ~30%
slower than a `COPY` pipe and needs an extension, a foreign server, a user
mapping and the privileges for each. Its default `fetch_size` of 100 rows per
round trip is badly wrong for bulk work and would matter more over a real
network than over loopback.

**Container startup: 0.22 s** steady-state, 0.27 s mean over five runs. This is
the number the partition-granular checkpoint guarantee rests on — sixty-one
partitions costs about thirteen seconds of startup in total. The same choice on
Dataflow costs minutes per job, which is why the checkpoint unit differs by
runner and the guarantee table has to say so.

**What these numbers are not.** No Kubernetes, no Dataflow, no cold image pull,
no real network latency between source and target, and narrow rows only.

## v1.2 Day 2 — the generated job, measured through the real path

Same machine and shape as Day 0, but now through `compile_snapshot_job` and
`DockerRunner` rather than a script written by hand: partition bounds from the
planner, `--single-transaction`, the staging table, and the counting merge that
reports what committed. Three runs, 1M narrow rows.

| Path | 1M rows | rows/sec |
|---|---|---|
| Generated job, one partition | **5.52–5.65 s** | ~177–181k |
| Generated job, 8 concurrent partitions | **5.18–5.29 s** | ~189–193k |
| Day 0 hand-written script, one partition | 6.30 s | ~159k |
| Day 0 hand-written script, 8 concurrent | 4.41 s | ~227k |

The single-job case is slightly *faster* than the hand-written Day 0 script, so
the counting merge and the staging table cost nothing measurable.

**The 8-way speedup did not reproduce.** Day 0 measured a 30% gain from
splitting; the real path gains about 6%. The cause is not established, and the
candidates — Docker CLI submissions serialising at the daemon, contention on
the single target, uneven partition bounds from the planner where Day 0 used
hand-picked ranges — have not been separated. It is recorded here as an open
question rather than explained away, because the partition count is a knob
operators will reach for and this says it currently buys much less than Day 0
implied.

Each run cross-checks the reported counts against the target's own row count;
they agreed exactly (1,000,000 both ways) on every run, which is the same
attestation a checkpoint rests on.

## v1.2 Day 3 — does the Python JDBC path need Java?

Day 0 left this as an open `[A]`, because it decides whether Beam is affordable
rather than merely possible. Measured with apache-beam 2.76.0 on Python 3.12,
Direct runner, against the dev stack's Postgres.

| Step | Result |
|---|---|
| `from apache_beam.io.jdbc import ReadFromJdbc` | **OK** — the Python side is a stub |
| Running the pipeline | **Fails without a JRE**: `Service failed to start up with error 1`, after 3 attempts, 5.8 s |
| What it does first | **Downloads two JARs from Maven Central at runtime** |

The JARs are `beam-sdks-java-extensions-schemaio-expansion-service-2.76.0.jar`
and `postgresql-42.2.16.jar`, fetched to `~/.apache_beam/cache/jars/`. Beam
prints its own warning about this: *"Apache Beam is downloading dependencies
from a public repository at runtime. This may pose security risks."*

**The answer is yes.** `apache_beam.io.jdbc` is a cross-language transform; the
Python API is a facade over a Java expansion service, and using it means a JRE,
a JAR cache, and — unless the JARs are pre-staged — a Maven Central fetch at
execution time, in the process that holds the database credentials.

**What this does and does not settle.** It does not block the `beam` job kind:
under the v1.2 model a job is a container, so the JRE and the pre-staged JARs
are the image's problem, not Gantry's, and Gantry gains no Beam dependency
either way. What it settles is the *cost*: the `beam` image carries a Java
runtime and a JDBC driver next to production credentials, against 411 MB of
`postgres:16-alpine` for the `sql` kind, and pre-staging is not optional
hardening but a requirement for a job that must not reach the public internet
while running.

It also narrows the honest options for a Beam job that reads Postgres:

| Option | Java | Cost |
|---|---|---|
| `JdbcIO` via the expansion service | **required** | large image, pre-staged JARs, a second runtime to operate |
| A plain Python `DoFn` using `psycopg` | not required | loses `JdbcIO`, and a per-row Python read against a `COPY` baseline of ~180k rows/sec |

Neither is free, and the choice is not obvious. Recorded here so Day 4's
guarantee table and the cut-line decision rest on a measurement.
