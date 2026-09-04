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

## Not yet measured

- Bulk copy throughput (Day 8 — target ≥ 50k rows/sec)
- Partition planning on a 100M-row table (Day 7)
- Checksum computation by key range (Day 12)
- CDC apply rate and lag (Days 13–15)
