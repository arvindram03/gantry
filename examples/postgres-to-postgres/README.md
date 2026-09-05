# Movement: PostgreSQL to PostgreSQL

A snapshot of a partitioned table, verified, with a corrupted row located and
repaired without a full re-copy.

[`movement.yaml`](movement.yaml) declares two Datasets, their keys, how they
partition, and what must be verified. Partition bounds are deliberately absent:
they come from the Dataset manifest at plan time, so they reflect the data
rather than a guess.

## Run it

```bash
make dev-up
uv run alembic upgrade head
uv run gantry seed --scenario orders --rows 1000000

uv run gantry plan   examples/postgres-to-postgres/movement.yaml
uv run gantry start  examples/postgres-to-postgres/movement.yaml
uv run gantry status orders-snapshot
```

`start` runs the Movement to completion and emits a `MovementResult` carrying
row counts, per-partition checkpoints and the verification record.

## Break it, then repair it

```bash
psql postgresql://gantry:gantry@localhost:15433/gantry \
  -c "UPDATE public.orders SET amount = amount + 1 WHERE order_id = 1"

uv run gantry verify examples/postgres-to-postgres/movement.yaml
```

Verification fails and names the partition. Checksums are order-independent
sums of row hashes, so the comparison does not depend on either side returning
rows in the same order, and the search that locates the bad row is a binary
drill-down — 27 comparisons to find one corrupt row in ten million.

```bash
uv run gantry repair examples/postgres-to-postgres/movement.yaml public.orders/00000
uv run gantry verify examples/postgres-to-postgres/movement.yaml
```

Repair re-copies **only** the partition that failed.

## Pause, resume, abort

```bash
uv run gantry pause  orders-snapshot --reason "source under load"
uv run gantry resume orders-snapshot --reason "load subsided"
uv run gantry abort  orders-snapshot --reason "superseded"
```

Pausing stops handing out work; partitions already in flight run to their
checkpoint rather than being cut off mid-copy.

## Which scheduler runs it

```bash
uv run gantry start examples/postgres-to-postgres/movement.yaml --backend queue
```

Temporal is the default. The Postgres leased queue implements the same
interface and needs no extra infrastructure. The runtime's guarantees do not
move between them: both deliver at-least-once, so the activity commits before
it checkpoints either way.
