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

## Not yet measured

- Checksum computation by key range (Day 12)
- CDC apply rate and lag (Days 13–15)
