# Gantry v1.2 — Movement as a Job

**Status:** Draft 3 — the unit is a job; the relay goes
**Source of truth:** `gantry-spec.md` (RFC 0) §5, §8, §17
**Predecessors:** [`execution-plan-v1.md`](execution-plan-v1.md) → `v0.1.0`,
[`execution-plan-migration.md`](execution-plan-migration.md) → `v0.2.0`
**Window:** 8 working days
**Target release:** `v0.3.0` — Gantry stops moving data

---

## 0. The principle

> **Gantry never moves data.** It plans the work, **generates a job**, submits
> it to an executor, observes it finish, verifies the outcome, and checkpoints
> what verified.

A job is whatever actually moves bytes: a transaction script, a Beam pipeline, a
Flink job, an ETL container, a binary someone else wrote. Gantry does not care
which, and that is the point — it cares about the four things it keeps.

**The relay goes.** v1's `_pipe_copy` streams one `COPY` into another through a
bounded queue *inside the Gantry worker*, which makes a single Python process
the throughput ceiling for a large partition and puts a crash **in** the data
path. It is deleted in this release, not demoted.

The line being drawn is precise: **control flows through Gantry; data does
not.** Issuing a statement over a connection and waiting for it is submission.
Holding the rows in a queue is being the mover. The first is fine at any scale;
the second is what is being removed.

### What a job is

```text
Plan  →  Generate  →  Validate  →  Execute  →  Verify  →  Result
          ↑             ↑            ↑
    the job, as a   can it run?   submit, poll,
    content-addressed             never carry bytes
    artifact
```

**Generate stops being a no-op for Movement.** Documented since v1 as "a no-op
for operations that compile straight to an engine API" — under this principle a
Movement compiles to a real thing, and that thing is retained as provenance
exactly like Analysis SQL. A `MovementResult` can then name the job that moved
each partition, which it cannot today.

A job carries:

| | |
|---|---|
| **kind** | `sql`, `beam`, `flink`, `container`, … |
| **body** | the transaction script, the pipeline, the image and arguments |
| **unit** | which partitions it covers — one, or a group |
| **hash** | content-addressed, so the same partition compiles to the same job |

### What Gantry keeps, whatever runs the job

Handing execution outside is not handing over the contract:

- **partitioning** — the plan decides the unit of work, never the executor
- **the commit boundary**, wherever the job kind can offer one
- **verification** — the acceptance test, run from outside the mover
- **checkpoints** — written only against verified work
- **provenance** — including, now, the job itself

An executor that wants to decide any of these is not an executor; it is a
replacement, and the answer is no.

---

## 1. The executor interface

Two methods and a state machine, uniform across every job kind:

```text
submit(job)        -> handle          # idempotent: resubmitting adopts
poll(handle)       -> state           # pending | running | done | failed
```

Then Gantry, identically for all of them:

```text
submit  →  poll to terminal  →  verify the unit  →  checkpoint what verified
```

**Nothing is checkpointed on a terminal state alone.** `DONE` is a mover saying
it finished; a returned `COMMIT` is Postgres saying it committed. Neither is a
claim that the rows are right, and this project exists not to take that class of
claim at face value.

**Verification costs nothing extra.** `verify_dataset` already runs every
declared check at dataset scope *and* every partition scope at the end of a
Movement. Verifying a unit when its job finishes is the same scan volume moved
earlier — and earlier stops a doomed movement at unit 2 of 40 rather than after
all 40 are paid for. Dataset-scope checks still run once at the end; they cannot
be answered a unit at a time.

**Repair does not need checkpoints.** It needs partition bounds, which live in
the plan. A single-partition job repairs, under any executor.

### The weaker the job's signal, the more verification carries

| Job kind | Commit boundary | Checkpoint unit | What a checkpoint asserts |
|---|---|---|---|
| `sql` (transaction script) | Gantry's — the script commits or does not | partition | committed **and** verified |
| `beam` (Dataflow batch) | the runner's — bundle-level, no user checkpoint | partition **group** | committed **and** verified |
| `container` / arbitrary | none — exit code only | whatever it was given | **verified**, and nothing else |

That last row is not a degradation to hide. It is the honest statement that when
a mover offers no durability signal, verification is the *only* authority — and
Gantry has one, which is why arbitrary executors are admissible at all.

---

## 2. Day 0 — measurements, some already taken

### The `sql` job kind: measured

`postgres_fdw` moves data server-to-server: the target's backend opens its own
connection to the source, and Gantry only issues the statement. Verified on the
stack:

- **Predicate pushdown holds.** `EXPLAIN VERBOSE` shows the partition bounds
  shipped to the source — without this, every partition read drags the whole
  table across.
- **Throughput, 1M narrow rows, one machine, Docker loopback:**

| Transport | 1M rows | rows/sec |
|---|---|---|
| `COPY … TO STDOUT \| COPY … FROM STDIN` (the relay's shape) | 1.81 s | ~552k |
| `postgres_fdw`, `fetch_size 50000` | 2.60 s | ~385k |
| `postgres_fdw`, default `fetch_size 100` | — | ~280k |

**Federated is ~30% slower than COPY, and that is the price of the principle.**
Same order of magnitude, not a cliff. The cause is mechanical: fdw fetches
through a cursor over the extended query protocol; `COPY BINARY` is the fastest
bulk path Postgres has.

`fetch_size` is the dominant knob and its default of **100 rows per round trip**
is badly wrong for bulk work — 100 → 50000 was a 40% improvement over loopback,
and will matter far more over a real network.

**Three caveats on those numbers.** One run, one machine, loopback — real
source↔target latency hurts a cursor more than a stream, so the gap likely
widens. Narrow rows only. And that was a plain `INSERT` with no staging or
upsert merge, so it is **not** comparable to the rehearsal's ~125k rows/sec,
which includes both.

Still to measure: the same numbers with staging and merge, and over a network
with real latency.

### The `beam` job kind: the runner has answered half of it

Assume Dataflow.

**There are no Beam checkpoints to rely on for a bulk copy.** Dataflow snapshots
are a *streaming* feature. A batch pipeline has no user-visible checkpoint to
resume from — only internal bundle retry, invisible to the submitter, which is
why idempotent writes stop being a nicety.

**Polling job status is the mechanism, at one granularity:**

| Job state | What the target contains |
|---|---|
| `DONE` | everything the pipeline wrote is committed |
| `FAILED` / `CANCELLED` | **partially written** — `JdbcIO` commits per bundle, nothing to roll back |

So the unit is the job, and what is left to choose is how much work goes in one:

| Partitions per job | Submission cost | Cost of a crash |
|---|---|---|
| 1 | a cluster start per partition | one partition |
| all | one cluster start | the whole dataset |
| *n* | ⌈partitions/*n*⌉ starts | at most *n* partitions |

One job per partition is correct and economically unusable — Dataflow batch
provisions workers per job, against per-project quotas on concurrent jobs and
creation rate.

- **[A]** Submission-to-first-row and teardown; these set the floor on group size.
- **[A]** Current quota ceilings. A low one forces group size up regardless of
  what a crash costs.
- **[A]** Whether the Python JDBC path needs a Java expansion service in practice.

### Two failure modes, and only one costs work

| What died | The work | Response |
|---|---|---|
| the Gantry worker | the job is still running | **adopt it** — nothing lost |
| the job | the unit is partially written | **re-run it** — idempotent, bounded |

The first is what job-based execution introduces and the relay never had: there,
a dead worker killed the copy. Here the work carries on with nobody watching.

**Exit:** measured numbers for both kinds, a default group size, and the
per-kind guarantee table filled in as far as it can be before code exists.

---

## 3. Days 1–2 — the seam, and the first job kind

### Day 1 — `MovementJob` and the executor interface

- **[A]** `MovementJob` as a generated artifact: kind, body, unit, content hash.
  Compiled at Generate, retained, referenced from the `MovementResult`.
  Retrofitting provenance is how provenance ends up incomplete, so it is day one.
- **[A]** `JobExecutor` protocol — `submit` / `poll` — and the worker driving it
  uniformly. The worker must not learn *which* executor ran; anything that leaks
  is the seam being in the wrong place.
- **[A]** `execution:` on a Movement spec, declared rather than inferred, because
  changing the job kind changes what a crash costs.
- **[B]** `ArtifactLanguage` gains real members. An enum with one value has never
  been tested.

**Exit:** a Movement compiles to a retained job artifact, and the worker submits
and polls through the interface.

### Day 2 — The `sql` executor, and the relay's replacement

- **[A]** Generate a transaction script per partition: `postgres_fdw` setup as a
  Prepare-time concern, then one statement per partition inside a transaction
  Gantry opens. The commit boundary stays Gantry's, so every guarantee **should**
  hold — and the chaos suite is what says whether "should" was right.
- **[A]** A missing extension or user mapping is a **Prepare-time refusal naming
  the privilege needed**, not a runtime error at row zero.
- **[A]** **Run the whole chaos suite against the `sql` executor.** New fixtures,
  **no new assertions** — the guarantees are the same guarantees.
- **[A]** **Only once that passes: delete `_pipe_copy` and the relay.** In that
  order. Deleting first would leave the crown jewels unproven for however long
  the replacement takes.
- **[B]** Throughput with staging and merge, into `docs/benchmarks.md`, whichever
  way it comes out.

**Exit:** Postgres→Postgres with **no bytes through the Gantry process**, the
chaos suite green against it, and the relay gone.

---

## 4. Days 3–4 — the `beam` job kind

### Day 3 — Submit, adopt, classify

- **[A]** A Beam pipeline as a job body, built from the partition bounds the plan
  recorded — never recomputed, because recomputing lets a partition move under a
  replay.
- **[A]** **Idempotent submission.** A worker that dies between submitting and
  recording the submission must not start a second job: deterministic job naming
  from the plan node id, and a running job is adopted rather than duplicated.
- **[A]** Failure classification: a runner failure and a bad row are different,
  and retrying a bad row forever is what happens if they are not.
- **[B]** Runner and job id in the trail, so an operator can find it in the
  runner's own console.

**Exit:** kill a worker between submit and checkpoint; restarting adopts.

### Day 4 — Groups, verification, and the guarantees

- **[A]** Partition groups sized from Day 0's numbers. Checkpoints for every
  partition in a group, **only after the group verifies**.
- **[A]** The chaos suite against `beam`. Where a guarantee needs the runner's
  cooperation, **name the runner** — "holds on Direct, unproven on Dataflow" is
  a useful sentence and "holds" is not.
- **[A]** Ordering and stale writes: Beam bundles are unordered by design, so the
  target's stale-write rejection is load-bearing here rather than incidental.
- **[B]** Snapshot ↔ CDC handoff is **out of scope**: it is LSN-stamped and
  Postgres-specific, and making it engine-neutral is its own piece of work.

**Exit:** the per-kind guarantee table filled in, including whatever does not hold.

---

## 5. Day 5 — the reach payoff

One non-Postgres target, chosen for what it proves rather than what is
fashionable. **BigQuery** or **Iceberg on local object storage**, and the second
is preferable: it runs in the existing Docker stack with no cloud account, so
the test suite stays runnable by a stranger with a clone.

- **[A]** Postgres → the new target, end to end, verified.
- **[A]** Verification against a non-SQL target. This is where the checksum
  abstraction gets tested: the current one computes `md5` sums *inside* the
  engine, and a target that cannot do that needs either a Beam-side checksum or
  a documented gap.
- **[B]** Type mapping, and what it refuses. Postgres `numeric(38,9)` into a
  target that cannot hold it should fail in Prepare, not at row 40 million.

**Exit:** a Movement lands data in a non-Postgres target and verification either
passes or names precisely what it could not check.

---

## 6. Days 6–8 — proof and release

### Day 6 — Fallout

Reserved. Days 1–5 will find things; this is where they get fixed rather than
deferred into the rehearsal.

If the plan is running clean by Day 6, spend it on the Flink runner — the Direct
runner is single-process and proves the API, not the distribution — or on making
`federated` the default, which is a docs and defaults change and should not be
done in a hurry.

### Day 7 — Rehearsal

A third suite in `scripts/rehearsal.py` (`--suite beam`), same shape as the
other two:

1. Seed; run the same Movement under both job kinds
2. `sql` — a transaction script per partition, no bytes through Gantry
3. `beam` — Direct runner, per-group verify-then-checkpoint
4. Kill a worker mid-job under `beam`; restart; adopt, do not duplicate
5. Corrupt a chunk; localise; repair one partition — under each kind
6. Move Postgres→the non-Postgres target via `beam`; verify
7. Show a `MovementResult` naming the job that moved each partition
8. Print the per-kind guarantee table with its measured answers

**Exit:** two consecutive clean runs, and the §0 table filled in with measured
answers rather than intentions.

### Day 8 — Docs and release

- `docs/beam.md`: when to choose which backend, and what each costs.
- `docs/guarantees.md`: **the guarantee table, per backend.** This is the whole
  point. A reader must be able to see at a glance that choosing Beam changes
  what a crash costs, if it does.
- `docs/adapters.md`: the execution-backend interface alongside the four adapter
  kinds.
- README: Beam in the model, and in "What Gantry is not" — *Gantry is not a Beam
  wrapper; it decides what runs and what may be believed, and Beam is one of the
  things that can run it.*
- Tag `v0.3.0`.

**Exit:** a stranger can choose a backend from the documentation and be right.

---

## 7. Cut-lines

| # | Cut | Why it is safe |
|---|---|---|
| 0 | **Beam entirely** | The `sql` job kind alone delivers the principle for Postgres→Postgres and removes the relay. It costs the reach claim, which the docs must then not make |
| 1 | Flink runner | Direct proves the contract; distribution is a scale claim, not a correctness one |
| 2 | The non-Postgres target (Day 5) | Costs the reach claim entirely — say so in the docs rather than implying it |
| 3 | Beam-side checksums | Fall back to "verification unsupported on this target", named explicitly |
| 4 | Job adoption after crash (Day 2) | Only if replaced by a refusal: a duplicate job is worse than a stopped migration |
| 5 | Configurable group size → one group per dataset | Simplest possible backend; the crash cost becomes the whole dataset, and the docs must say so |

**Never cut:** the guarantee table, and the ordering in Day 2. Shipping a second
job kind without a per-kind statement of what holds would make every guarantee in
the project ambiguous, including the ones that are fine. And the relay is deleted
*after* the chaos suite passes against its replacement, never before — the
guarantees were proven against it, and removing it first leaves them unproven for
however long the replacement takes.

---

## 8. Risk register

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| **Handing execution outside becomes handing over the contract** | **Critical** | **Medium** | §1 lists what stays Gantry's: partitioning, the commit boundary, verification, checkpoints, provenance. An executor that wants to decide any of them is a replacement, not an executor |
| The `sql` kind is slower than the relay was | Medium | **Confirmed: ~30%** | Measured on Day 0 and published. The principle costs throughput here; pretending otherwise would be the dishonesty this release exists to correct |
| `postgres_fdw` cannot be installed where it matters | High | **Medium** | **No fallback now that the relay is gone.** Either the job kind grows a variant that does not need it — a script the operator runs, a container — or that environment cannot use Gantry for Postgres→Postgres. Decide this on Day 0, not on the day someone hits it |
| Deleting the relay before its replacement is proven | **Critical** | Low | Day 2's ordering is explicit and is in the never-cut list |
| Cross-language transforms drag in a Java expansion service | High | **High** | Prove the JDBC path on Day 0. If it needs a Java sidecar, that is a stack change and belongs in the same decision as the architecture |
| Beam's Python SDK weight and startup cost | Medium | High | Optional extra (`gantry[beam]`), never a core dependency (§17) |
| Checkpoint granularity collapses silently | **Critical** | Medium | The guarantee table is the artifact that prevents this; fill it in as you go rather than at the end |
| Group size chosen without measuring | High | Medium | Day 0 measures submission cost and quota ceilings first. A default picked by taste trades a crash cost nobody computed against a bill nobody predicted |
| Someone reads `DONE` as "the rows are right" | Medium | Medium | `DONE` means committed, not correct. Nothing is checkpointed on `DONE` alone — the group is verified first |
| A target Gantry cannot verify silently gets a weaker guarantee | High | Medium | Verification is the acceptance test, so an unverifiable sink has none. Name it in the guarantee table rather than letting `DONE` stand in for correctness |
| The Direct runner passes and Flink does not | Medium | Medium | Say which runner each guarantee was proven on. "Holds" without a runner name is not a claim |
| Two data paths diverge over time | Medium | High | The worker must not know which backend ran; anything that leaks into it is the seam being wrong |

---

## 9. Definition of done

- [ ] A Movement runs on Beam through the same worker interface as the Postgres path
- [ ] Job submission is idempotent — a crash between submit and checkpoint adopts, never duplicates
- [ ] The chaos suite runs against Beam, and every guarantee is either proven or written down as lost
- [ ] Data lands in one non-Postgres target, verified or explicitly unverifiable
- [ ] **`docs/guarantees.md` states what holds per backend**, and a reader can choose from it
- [ ] The checkpoint-unit sentence is written *before* the adapter, and the adapter matches it
- [ ] Nothing is checkpointed on `DONE` alone; every checkpoint has a verification behind it
- [ ] Two consecutive clean rehearsal runs
- [ ] Nothing in `gantry/movement/worker` or the scheduler knows which executor ran
- [ ] **No byte of customer data passes through a Gantry process, on any path**
- [ ] `_pipe_copy` and the relay are deleted, and the chaos suite passed against
      the `sql` executor first
- [ ] Every Movement retains the job that moved its data, and the
      `MovementResult` names it

---

## 10. What this does not settle

**Whether Beam is the right dependency.** It is a large one, bought for reach
and scale at the cost of a Java expansion service and a runner to operate. If
Day 0 shows the JDBC path needs a sidecar and submission overhead forces coarse
groups, the honest conclusion may be that the `sql` job kind delivers the
principle and a MySQL source adapter delivers the reach, for a fraction of the
cost.

**That conclusion is an acceptable outcome**, and reaching it on Day 0 rather
than Day 8 is what Day 0 is for. Note what it would not undo: the `sql` kind is
what removes Gantry from the data path, and it stands whether or not Beam ships.

**What replaces the relay where `postgres_fdw` cannot be installed.** The relay
was the answer with no prerequisites and it is being removed. A `container` or
operator-run script job kind is the obvious candidate — Gantry generates it,
something else runs it, and verification is the only acceptance test — but it is
not designed here and Day 0 has to say whether it is needed.
