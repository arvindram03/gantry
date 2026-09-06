# Execution backends

Gantry does not move data. It decides what should move, generates the work,
hands it to something else, and decides afterwards whether the result may be
believed. This page is about that "something else" — what the choices are, what
each costs, and how to pick.

For what each backend *guarantees*, see [guarantees.md](guarantees.md). This
page is about choosing; that page is about what you are buying.

## The two kinds

A **job** is one unit of work: a script, generated from the plan, packaged so
something can run it. Two kinds exist today.

### `sql` — the default

A shell script that runs `psql` inside a container. It connects to both
databases itself and streams `COPY` from one into the other, through a staging
table and a merge. No bytes pass through any Gantry process.

Choose it for **ordinary databases, one source to one target.** It starts in
about a quarter of a second, moves ~180k rows/sec on a laptop, and checkpoints
every partition — the strongest guarantee Gantry offers. It is the default
because it is the strongest, not because it came first.

### `beam` — for reach and volume

An Apache Beam pipeline, run in a container. Gantry generates the pipeline from
the plan's partition bounds and submits it; Beam does the work.

Choose it for **large volumes across heterogeneous sources and sinks** — the
thing Beam is actually for. Its I/O connectors are the reason to accept its
costs, and those costs are real:

| | `sql` | `beam` |
|---|---|---|
| Image | 411 MB | **6.2 GB** (a JRE, the Beam SDK, pre-staged JARs) |
| Startup per job | ~0.25 s | **~11 s** |
| Checkpoint unit | one partition | **a group of partitions** |
| Reports rows moved | yes | **no** — verification counts instead |

**Do not choose `beam` for Postgres-to-Postgres.** Measured on the same machine
and the same million rows, it is slower on every axis, and it costs you the
partition-granular checkpoint. That is not a criticism of Beam; it is a
distributed pipeline being asked to do a job a shell script does better.

## Why `beam` groups partitions

Because eleven seconds a job is not affordable one partition at a time.

Sixty-one partitions cost about fifteen seconds of startup under `sql`, which is
not worth trading a guarantee for. The same sixty-one cost about **eleven
minutes** under `beam`, which is. So partitions travel together, and the group —
not the partition — becomes the unit a crash costs you.

The size is arithmetic, not taste: a job carries enough rows that its startup is
roughly a tenth of it, which at measured throughput is over a million rows. The
constants live in `gantry/movement/grouping.py`, and re-measuring changes the
groups rather than requiring anyone to rewrite the rule.

## What Beam cannot tell you

A `sql` job reports what it committed — rows inserted, updated, unchanged — and
a checkpoint rests on that attestation.

Beam reports no per-row counts a submitter can read. A pipeline reaching `DONE`
says it finished, not that the rows are right. So under `beam` a checkpoint may
not advance on `DONE`: **the group is verified first, and the verification is
the attestation.** A group that does not verify is not checkpointed, the lease
expires, and it runs again.

This is why a `beam` `MovementResult` carries no row counts. Inventing them from
a checksum would be a different number wearing the same name.

## Iceberg, and appending targets

Iceberg is reachable through `beam`, and it is where the reach claim is actually
tested: Postgres to an Iceberg table on a filesystem catalog, verified against
the source.

Two things about it are worth knowing before you use it.

**The write appends.** Running the same move twice appends the rows twice —
measured, 200 became 400. Since Gantry replays whenever a worker dies between
committing and checkpointing, an unguarded append turns the recovery path into
the corruption path. So the move is guarded: Gantry checks what the target
already holds and moves only if it does not already hold the group. That is
sound because one job commits one atomic Iceberg snapshot and a killed job
leaves nothing at all — both measured, both recorded in
[benchmarks.md](benchmarks.md), and both load-bearing.

**Verification runs as a job.** An Iceberg table has no engine to send SQL to,
and streaming its rows into the Gantry process to add them up is the one thing
this design forbids. So a small container reads the Parquet and returns one
checksum and one count. It runs the same checksum code the rest of the system
uses.

## Choosing, briefly

- One database to another, and you want the strongest guarantee: **`sql`**.
- A sink `psql` cannot reach, or volumes that want a distributed runner:
  **`beam`**, and read [guarantees.md](guarantees.md) first so the weaker
  checkpoint is a decision rather than a surprise.
- Not sure: **`sql`**. It is the default, and switching later is a replan rather
  than a rewrite — the job kind is part of the guarantee fingerprint precisely
  so that changing it cannot happen quietly.

## What is not proven

Said plainly, because "holds" without a runner name is not a claim:

- Everything above was proven on the **Direct runner, in a container, on one
  machine**. **Dataflow and Flink are unproven.**
- Dataflow's submission cost and quota ceilings are unmeasured, and they are the
  numbers that would decide group size there.
- The `beam` image is built locally (`docker/beam/Dockerfile`); nothing publishes
  it.
