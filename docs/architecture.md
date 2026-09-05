# Architecture

Gantry sits above the transport and processing infrastructure you already run.
It never stores, interprets or transforms your data. It owns the execution
contract around it — what was planned, what ran, what was checkpointed, what
was verified, and what may be believed as a result.

**A distinction worth making early, because looser phrasing invites the wrong
question.** Bytes do sometimes pass through Gantry: the Postgres backend relays
one `COPY` stream into another through a bounded queue, so a partition's bytes
traverse the worker process. What never happens is that they become rows —
Python moves buffers, engines move rows (§17).

That relay is not what the guarantees rest on — what is load-bearing is that
**Gantry chooses the transaction boundary and observes the commit**. But it is
still a design that drifted: a single Python process is the throughput ceiling
for a large partition, and a crash there is a crash *in* the data path rather
than beside it.

**v1.2 corrects it, and removes the relay.** The principle is that Gantry never
moves data: it plans the work, **generates a job**, submits it to an executor,
observes it finish, verifies the outcome, and checkpoints what verified. A job is
whatever actually moves bytes — a transaction script, a Beam pipeline, a Flink
job, an ETL container.

The line is precise: **control flows through Gantry; data does not.** Issuing a
statement and waiting for it is submission. Holding the rows in a queue is being
the mover, and that is what goes.

**The default job is a SQL transaction script**, which is also the strongest
kind: the commit boundary stays Gantry's and checkpoints stay partition-granular.
Beam is for what SQL cannot reach — another engine, or scale beyond one server.
See [execution-plan-external-execution.md](execution-plan-external-execution.md).

## Four resources

```text
Dataset        an addressable logical data unit, plus its manifest
   │
   ├── Movement    make or keep a Dataset reliably available somewhere
   │
   └── Analysis    compute over one or more Datasets
                    │
                    ▼
                 Result    bounded, structured, provenanced output
```

**Dataset** is the noun everything else is about. A manifest describes it —
schema, keys, time semantics, statistics, access policy — and is
content-addressed, so re-registering an unchanged manifest is a no-op rather
than a new version. Everything downstream pins a Dataset *version*, never
"whatever is current".

**Movement** and **Analysis** are both Operations. They differ in what they do
at each stage, not in the stages they pass through, which is why they share one
state machine, one plan format, one checkpoint store and one verification
framework rather than three of each that drift apart.

**Result** is what an Operation is allowed to assert. It carries provenance —
the artifact it came from, the Dataset versions it read, the checkpoints those
Datasets had reached — and a verification record. A Result whose verification
failed is still written, because *why* nothing was concluded is worth keeping.

## One lifecycle

```text
Plan → Generate → Validate → Execute → Verify → Result
```

Every Operation traverses it. The stages mean:

| Stage | Movement | Analysis |
|---|---|---|
| Plan | discover, profile, partition, order by dependency | resolve inputs, pin Dataset versions |
| Generate | partition bounds, copy statements, connector config | compile the spec to engine SQL |
| Validate | check the target's shape, the key, the write mode | plan the query, estimate cost, sample it |
| Execute | copy partitions, apply changes, checkpoint | run the artifact on an engine |
| Verify | counts, checksums, key uniqueness, referential integrity | row expansion, join coverage, alignment, null rate |
| Result | `MovementResult` | `AnalysisResult` with findings |

**Generation is a no-op for operations that compile straight to an engine API**
— the stage still exists so provenance has one shape.

The boundary this diagram draws is the product. An engine reporting `SUCCESS`
means the query ran. Verify decides whether the numbers may be believed, and it
is a different question with a different answer.

## What is deterministic and what is not

```text
   agent / planner                 deterministic runtime
   ───────────────                 ─────────────────────
   proposes a spec        →        compiles it to an artifact
   proposes a repair      →        decides whether it may run
   reads a Result         ←        verifies, checkpoints, enforces policy
```

An agent may plan and re-plan freely. It cannot widen a guarantee, skip a
verification, reach past the access ladder, or mark its own work verified —
not because it is asked not to, but because those decisions live in code it
calls rather than in a prompt it reads.

This is also why the Analysis spec has a **fixed signal vocabulary** rather
than accepting expressions, and why the agent-facing query API takes a
structured aggregate rather than SQL. A spec that accepted arbitrary SQL would
make Gantry a query language, and engines already have one.

## Components

```text
                 spec (YAML)  ──►  loader  ──►  domain model
                                                     │
                                                  planner
                                                     │
                                              PlanVersion (immutable,
                                               content-addressed)
                                                     │
                            ┌────────────────────────┼────────────────┐
                            ▼                        ▼                ▼
                       scheduler               source/target      engine
                  (Temporal, or the             adapters          adapters
                   Postgres leased queue)     (Postgres, CDC)  (Postgres, DuckDB)
                            │                        │                │
                            └────────────────────────┼────────────────┘
                                                     ▼
                                          metadata store (Postgres)
                                   operations · plans · checkpoints ·
                                   verifications · results · artifacts ·
                                   dead letters · access log
```

**The metadata store holds no customer row data.** The data plane keeps that;
the control plane keeps only what it needs to plan, checkpoint, verify, audit
and recover. The one exception is the dead-letter queue, which retains the
change event it could not apply — a dead-letter queue that discards the payload
is a counter.

**Workers are disposable.** Everything needed to resume lives in the metadata
store, which is why a plan is reconstructed from storage rather than recompiled
and why `kill -9` mid-copy is a tested path rather than a hoped-for one.

**The scheduler is replaceable.** Temporal owns dispatch, retries and timeouts
by default; the Postgres leased queue implements the same `WorkflowBackend`
interface. The runtime's guarantees do not move between them: both deliver
at-least-once, so the activity still commits before it checkpoints and still
depends on the write being idempotent.

## Where to read next

- [guarantees.md](guarantees.md) — what v1 guarantees, and what it does not
- [adapters.md](adapters.md) — the adapter interfaces and what each one owns
- [benchmarks.md](benchmarks.md) — measured numbers, with the conditions
- [rfcs/0000-gantry.md](rfcs/0000-gantry.md) — the design document and its revisions
