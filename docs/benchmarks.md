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

## Not yet measured

- Bulk copy throughput (Day 8 — target ≥ 50k rows/sec)
- Checksum computation by key range (Day 12)
- CDC apply rate and lag (Days 13–15)
