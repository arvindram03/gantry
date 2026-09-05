<p align="center">
  <img src="docs/assets/gantry-logo-512.jpg" alt="Gantry" width="260">
</p>

<p align="center">
  <strong>Open-source reliability and execution layer for data movement and analysis.</strong>
</p>

Gantry sits above the transport and processing infrastructure you already run. It does not
own the bytes on the wire — it owns the execution contract around them: checkpoints, replay,
ordering boundaries, idempotency, verification, policy, provenance and recovery.

> Agents may plan and re-plan. Deterministic infrastructure enforces guarantees.

## The problem

An engine reporting `SUCCESS` tells you a query ran. It does not tell you whether the join
multiplied its input, whether the rows you read were complete, or whether the number you are
about to act on came from the data you think it did.

That gap is usually filled by convention — a runbook, a reviewer, an analyst who remembers.
Gantry fills it with code that runs every time:

```text
well-formed  ENGINE: SUCCESS  GANTRY: PASSED  rowExpansion 1.0000x   → 4 findings
expanding    ENGINE: SUCCESS  GANTRY: FAILED  rowExpansion 2.0000x   → findings withheld
                              the join expanded 40,000 rows to 80,000 (2.00x)
```

Same SQL, same engine, same data. One of them means something.

## When do you need Gantry?

Gantry is for the moment when "the job succeeded" stops being enough.

**You need it when any of these are true.**

*You are moving data and cannot afford to guess.* You are replicating a table
into a warehouse, migrating between databases, or keeping a copy in sync. The
copy job reports success. Nobody can tell you whether the target actually
matches the source, and the only way to find out is a full comparison that
takes hours.

*A worker dies and you do not know what to do.* Something crashed halfway
through a hundred-million-row copy. Restarting from the beginning wastes a day.
Restarting from "wherever it got to" risks duplicating or skipping rows,
because nothing recorded where it got to in a way you can trust.

*Something is wrong with one chunk and you have to redo everything.* A checksum
is off. The table is fine except for one range of keys, but the tooling only
knows how to copy the whole thing again.

*An analysis returned a number and you cannot check it.* A query ran and
produced a figure someone is about to act on. You cannot easily tell whether a
join silently multiplied the rows, whether it matched almost nothing, or which
version of the data it read.

*You want an agent or an LLM near your data, carefully.* You would like a model
to investigate an incident or write an analysis, but not to be able to read raw
customer rows, run something unbounded, or mark its own conclusion as verified.
Asking it nicely in a prompt is not a control.

*"Where did this number come from?" takes a day to answer.* A dashboard figure
is disputed. Tracing it back through pipeline runs, table versions and query
history is manual archaeology.

**You probably do not need it when:**

- You already have a warehouse-native tool that covers your case end to end
- Your data is small enough that a full re-copy and a full re-check are cheap
- You are looking for a scheduler or an orchestrator — Gantry is not one, and
  it runs *on* Temporal rather than replacing it
- You want a query engine — Gantry compiles onto PostgreSQL and DuckDB rather
  than competing with them

**What Gantry is not.** It does not store your data, replace your database or
warehouse, replace your orchestrator, or make an LLM trustworthy. It makes what
an LLM (or a person, or a cron job) does with your data *checkable*.

## How do you use it?

Three things you write, and one thing you read.

### 1. Describe your data — a Dataset

```yaml
apiVersion: gantry.dev/v1alpha1
kind: Dataset
metadata:
  name: public.orders
physical:
  adapter: postgres
  reference: public.orders
schema:
  keys: [order_id]
semantics:
  timeField: created_at        # discovery cannot know which column orders time
access:
  agentPolicy: aggregate_or_masked
```

Most of this is discovered for you — `gantry discover` reads the catalog and
profiles the table. You only declare what a catalog cannot know: which column
carries time, which fields are sensitive, and what an agent may reach.

### 2. Move it — a Movement

```yaml
apiVersion: gantry.dev/v1alpha1
kind: Movement
metadata:
  name: orders-snapshot
source:
  adapter: postgres
  connectionRef: source
destination:
  adapter: postgres
  connectionRef: target
strategy:
  mode: snapshot                 # or snapshot_then_stream, to keep it in sync
datasets:
  - name: public.orders
    source: public.orders
    target: public.orders
    key:
      columns: [order_id]
    partitioning:
      strategy: range
      column: order_id
      rowsPerPartition: 2000000
    verification:
      required: [row_count, chunk_checksum]     # what must be true afterwards
```

```bash
gantry plan   movement.yaml    # compile an immutable, content-addressed plan
gantry start  movement.yaml    # run it, checkpointing every partition
gantry verify movement.yaml    # check the target against the source
gantry repair movement.yaml public.orders/00004   # fix one partition
```

You do not write partition boundaries. They come from the data at plan time.
You write what must be **true** at the end, and Gantry decides how to check it.

### 3. Analyse it — an Analysis

```yaml
apiVersion: gantry.dev/v1alpha1
kind: Analysis
metadata:
  name: checkout-regression
inputs:
  - dataset: public.request_logs
  - dataset: public.deploy_events
window:
  start: "2026-09-03T00:00:00Z"
  end:   "2026-09-04T00:00:00Z"
normalize:
  fields:
    service:
      aliases: [svc, service_name]   # the two sources name it differently
joins:
  - left: public.request_logs
    right: public.deploy_events
    on: [service]
    temporal:
      strategy: nearest_preceding    # attribute each request to the live deploy
      maxDistance: 24h
signals: [row_count, p95_latency, error_rate, database_calls_per_request]
verify:
  - rowExpansion: {max: 1.1}         # the join must not multiply the rows
  - nullRate: {field: commit_sha, max: 0.05}
```

You describe *what to compute*, not how. Signals come from a fixed vocabulary
rather than being SQL you write — a spec that accepted arbitrary SQL would make
Gantry a query language, and your engine already is one.

`verify:` is the part that matters. It is what makes the difference between "the
query ran" and "the answer can be believed".

### 4. Read the answer — a Result

```bash
gantry results get        checkout-regression.analysis   # the findings
gantry results explain    checkout-regression.analysis   # the exact SQL that ran
gantry results provenance checkout-regression.analysis   # where the data came from
gantry results refresh    checkout-regression.analysis   # does it still hold?
```

Every finding carries the measurements behind it and says where its confidence
came from — measured, statistical, or a model's opinion, labelled as such.
`provenance` walks from a finding back to the artifact, the exact Dataset
versions read, and the checkpoint the Movement that produced them had reached.

### Letting an agent use it

```python
async with gantry.connect(source_url=...) as session:
    await session.datasets.describe("public.orders")  # allowed
    await session.datasets.query("public.orders", ...)  # allowed: an aggregate
    await session.datasets.sample("public.orders", ...)  # denied by default
```

The same ladder from the CLI:

```bash
gantry policy show                      # the policy in force
gantry dataset access public.orders     # what an agent may do with this table
```

The rules are enforced by a function every data path must call, not by a prompt
a model reads. There is no field on a request that could carry an override.

### Where it fits

```text
your orchestrator (Airflow, cron, an agent)
        │  asks for a Movement or an Analysis
        ▼
      Gantry ── plans, checkpoints, verifies, records provenance, enforces policy
        │  dispatches work to
        ▼
your infrastructure (PostgreSQL, Kafka, Debezium, DuckDB, Temporal)
```

Gantry does not move the bytes itself and does not want to. It decides what
runs, records what happened, and refuses to call something correct until it has
checked.

## The model

Four core resources:

```text
Dataset        addressable logical data unit + manifest
   │
   ├── Movement    make or keep a Dataset reliably available
   │
   └── Analysis    compute over one or more Datasets
                    │
                    ▼
                 Result    bounded, structured, provenanced output
```

Movement and Analysis are both Operations over Datasets, and both traverse one lifecycle
under a single guarantee boundary:

```text
Plan → Generate → Validate → Execute → Verify → Result
```

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) and Docker. About five minutes, most of it
pulling images.

```bash
git clone git@github.com:arvindram03/gantry.git && cd gantry
make install
make dev-up
uv run alembic upgrade head
uv run gantry seed --rows 1000000
```

**Move a table, and prove it arrived.**

```bash
uv run gantry plan   examples/postgres-to-postgres/movement.yaml
uv run gantry start  examples/postgres-to-postgres/movement.yaml
```

Corrupt one row in the target and watch verification find it, then repair only the partition
it was in — the walkthrough is in
[examples/postgres-to-postgres](examples/postgres-to-postgres/README.md).

**Run an Analysis, and ask why you should believe it.**

```bash
make demo
uv run gantry results provenance checkout-regression.analysis
```

```text
checkout-regression.analysis  1 artifacts, 2 dataset versions, 8 checkpoints
  computation  sha256:1fbc4b97c6b3…  on postgres
  read         public.request_logs@2   sha256:ddbfbe2ac08e…
  read         public.deploy_events@2  sha256:6b1897cf648f…
  produced by  orders-snapshot.movement
  checkpoints  8
    partition/public.orders/00000 at partition_id
    dataset/public.customers at partition_id
    …
```

From a finding, in one command, back to the state the data was in when it was read. Any link
it cannot resolve is named rather than omitted.

**See what an agent may reach.**

```bash
uv run gantry policy show
uv run gantry dataset access public.orders
uv run gantry dataset sample public.orders --limit 5 --reason "triage"   # denied
uv run gantry dataset query  public.orders --agg count --agg avg:amount  # allowed
```

**Run the whole thing end to end.**

```bash
make rehearse
```

Seeds, injects faults including a real `kill -9`, corrupts and repairs, runs both Analyses,
traces provenance and exercises the access ladder — about a minute, timed step by step.

## What v1 guarantees

- **Crash replay** — a worker killed mid-copy loses nothing and duplicates nothing
- **Idempotent writes** — proven under shuffled and duplicated delivery
- **Ordering** — stale writes rejected by source position, per key
- **Snapshot ↔ CDC handoff** — LSN-stamped, no gap and no replay of the whole table
- **Verification** — order-independent checksums, with `O(log n)` localisation of a mismatch
- **Repair** — one partition, not the table
- **Provenance** — a finding traces to the Movement checkpoint its input data had reached
- **Agent access** — a ladder enforced in code, not in a prompt

And what it does not — including the gaps found by its own rehearsal — is in
[docs/guarantees.md](docs/guarantees.md). That document is meant to be read before the
feature list, not after.

## Documentation

| | |
|---|---|
| [architecture.md](docs/architecture.md) | the four resources, the shared lifecycle, the components |
| [guarantees.md](docs/guarantees.md) | what v1 guarantees, and what it does not |
| [adapters.md](docs/adapters.md) | the adapter interfaces and what each one owns |
| [benchmarks.md](docs/benchmarks.md) | measured numbers, with the conditions they were measured under |
| [rfcs/0000-gantry.md](docs/rfcs/0000-gantry.md) | the design document and its revisions |
| [execution-plan-v1.md](docs/execution-plan-v1.md) | how v1 was built, day by day, with what each day found |

## Status

`v0.1.0`. Every guarantee above is exercised by tests against real databases, real Kafka and
real Debezium — not simulations — and by a rehearsal that runs the whole sequence through the
CLI. It has not been run in production by anyone, including its authors.

## Development

```bash
make check      # lint, strict typecheck, unit tests
make test-int   # integration tests against the local stack
make test-chaos # fault injection, including a real kill -9
make rehearse   # the full end-to-end rehearsal, timed
```

`make help` lists every target. [CONTRIBUTING.md](CONTRIBUTING.md) covers the rest.

## License

Apache-2.0
