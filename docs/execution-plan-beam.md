# Gantry v1.2 — Apache Beam Execution Plan

**Status:** Draft 1
**Source of truth:** `gantry-spec.md` (RFC 0) §5, §8, §17
**Predecessors:** [`execution-plan-v1.md`](execution-plan-v1.md) → `v0.1.0`,
[`execution-plan-migration.md`](execution-plan-migration.md) → `v0.2.0`
**Window:** 8 working days
**Target release:** `v0.3.0` — Movement over a data path Gantry does not run

---

## 0. What this proves

Every guarantee in this project reduces to one sentence in the Postgres
backend: **`copy_partition` opens the transaction, and returns only after that
transaction commits.** The worker checkpoints on the next line. Crash replay,
idempotency, single-partition repair — all of it rests on Gantry choosing the
commit boundary and observing the commit synchronously.

Beam breaks *that*, and it is worth being precise about what it does not break.

It is tempting to say Gantry "is the data path" today and Beam takes it away.
That is not the distinction. The Postgres backend does relay bytes — a bounded
queue pipes one `COPY` stream into another — but the guarantee does not depend
on it. Rewrite that path to use `postgres_fdw` or a server-side `COPY`, so no
byte ever enters the Gantry process, and every guarantee holds unchanged.
Topology is not what is load-bearing.

What is load-bearing is three things, and Beam changes all of them:

| | Postgres backend | Beam |
|---|---|---|
| Who chooses the commit boundary | Gantry — one transaction per partition | the runner — bundle boundaries |
| Is the commit synchronously observable | yes, the call returns after `COMMIT` | no — submit, then poll |
| Who retries, and can Gantry see it | Gantry, visibly | the runner, invisibly |

So the question is not "can we call Beam", and not "who moves the bytes". It is:

> **Can Gantry keep the commit boundary its guarantees are defined in terms of,
> when the commit happens inside a runtime with its own?**

That is the thesis under test. Gantry claims to own checkpoints, replay,
ordering, idempotency, verification and provenance while engines own the data —
and Postgres-to-Postgres never tested it, because Gantry chose every transaction
boundary itself.

### The measurement

The Migration plan measured lines changed in `gantry/movement/`. This one
measures something harder and more honest — **which guarantees survive**:

| Guarantee | Postgres path | Beam path — to be filled in |
|---|---|---|
| A killed worker loses nothing and duplicates nothing | holds | ? |
| Writes are idempotent under duplicated delivery | holds | ? |
| Stale writes rejected by source position | holds | ? |
| Checkpoint granularity | per partition | **per partition group** — the job is the only durability boundary Dataflow exposes |
| Repair re-copies one partition, not the table | holds | ? |
| Verification is order-independent and localises in `O(log n)` | holds | ? |

One row is already answered, from the runner's semantics rather than from an
experiment: **Dataflow exposes no checkpoint below the job**, so the Beam
backend's unit is a partition group. That is a real difference in what a crash
costs and it belongs in the same sentence as the feature.

**A guarantee that cannot be kept must be written down as lost, not quietly
redefined.** If Beam-backed Movements checkpoint per dataset rather than per
partition, that is a real difference in what a crash costs, and the docs must
say so in the same sentence as the feature.

### Why Beam and not a MySQL adapter

The honest answer to "what unblocks the most" has been a second source adapter
since `v0.1.0`. Beam is a *bigger* version of that answer: its connector
ecosystem reaches BigQuery, Kafka, Iceberg, Spanner, JDBC and object storage,
so one adapter buys many backends rather than one.

It also costs more, and the cost is real: cross-language transforms, a Java
expansion service for the JDBC connectors, and a runner to test against. That
trade is stated in §7 rather than discovered in week two.

---

## 1. Scope

| In scope | Out of scope |
|---|---|
| `BeamMovementBackend`: a Movement executor that submits pipelines | Analysis on Beam SQL |
| Direct runner (tests) and Flink (distribution proof) | Dataflow — named, needs GCP, not required |
| Checkpoint ownership across an external runtime | Beam-native CDC |
| One non-Postgres target, to earn the reach claim | Rewriting the Postgres path |
| Verification unchanged, against whatever Beam wrote | Beam as a scheduler (Temporal keeps that) |

**The Postgres path stays.** It is faster for Postgres-to-Postgres than any
distributed runner will be — no job submission, no serialisation, COPY straight
through — and it is the reference implementation the guarantees were proven
against. Beam is a second backend, chosen per Movement, not a replacement.

**Analysis on Beam is deliberately deferred.** Gantry already has two SQL
engines; a third proves less than the Movement work does, and Beam SQL would
test the artifact abstraction while the Movement path tests the guarantee
abstraction. The second is the harder question.

---

## 2. Day 0 — the shape question

**Partly answered already, and the answer narrows the design before any code is
written.** Assume Dataflow as the runner.

### What Beam gives us, and what it does not

**There are no Beam checkpoints to rely on for a bulk copy.** Dataflow snapshots
are a *streaming* feature — Streaming Engine state, drain and resume. A batch
pipeline has no user-visible checkpoint to resume from. What it has instead is
internal bundle and work-item retry, which is invisible to the submitter and is
precisely why idempotent writes stop being a nicety and become the thing holding
correctness together.

**Polling job status is the mechanism, and it is enough — at one granularity.**
Be exact about what a terminal state says about the sink:

| Job state | What the target contains |
|---|---|
| `DONE` | everything the pipeline wrote is committed — a real durability signal |
| `FAILED` / `CANCELLED` | **partially written.** `JdbcIO` commits per bundle; there is no global transaction to roll back |

So `DONE` is a genuine commit signal **at job granularity, and nothing finer is
available**. A failed job leaves the target partially populated, which is safe
only because Gantry already requires idempotent upserts: re-running is correct,
merely expensive.

### The consequence: checkpoint granularity is a tuning knob

This is the finding, and it replaces the three-way choice above.

The checkpoint unit is the job, because that is the only durability boundary
Dataflow exposes. **What is left to choose is how much work goes in a job** —
and that is a straight trade:

| Partitions per job | Submission cost | Cost of a crash |
|---|---|---|
| 1 | one cluster start per partition | one partition re-run |
| all | one cluster start | the whole dataset re-run |
| *n* | ⌈partitions/*n*⌉ starts | at most *n* partitions re-run |

Naive one-job-per-partition is **correct and economically unusable**: Dataflow
batch provisions workers per job, so submission-to-first-row is minutes, and
there are per-project quotas on both concurrent jobs and job creation rate.
Sixty-one partitions is sixty-one cluster starts.

So the design is **one job per partition group**, with the group size declared
in the spec and defaulting to something that makes a crash cost minutes rather
than hours. The guarantee statement becomes precise rather than weakened:

> On the Beam backend, the checkpoint unit is a partition group, not a
> partition. A crash re-runs at most one group. Group size is declared.

### What would be needed for finer granularity, and why it is not day one

Sub-job durability signal requires the pipeline to write the checkpoint itself,
in the same transaction as the data. `JdbcIO` cannot do that — its batches do
not align with partitions and it exposes no hook — so it needs a **custom sink**
that writes rows and a partition marker in one transaction. That is real work,
it puts Gantry code inside someone else's pipeline, and it is only correct if
the marker lands in the same database as the data.

Two things that look like a shortcut and are not:

- **Beam metrics.** Counters are explicitly best-effort and may be reported from
  retried bundles. A metric is not a durability signal.
- **`@FinishBundle` hooks.** Bundle boundaries are non-deterministic and a bundle
  can be retried after the hook ran. Writing a checkpoint there claims durability
  the runner has not promised.

### Day 0 still has to measure

The shape is decided; the numbers are not.

- **[A]** Dataflow batch submission-to-first-row, and job teardown, for a trivial
  pipeline. This sets the floor on group size.
- **[A]** Current per-project quotas on concurrent jobs and creation rate. If the
  ceiling is low, group size is forced up regardless of what a crash costs.
- **[A]** Whether the Python SDK's JDBC path needs a Java expansion service in
  practice, and what that means for the dev stack.
- **[B]** The same three on the Direct runner, which is what tests will use.

**Exit:** a default group size chosen from measured numbers, and the guarantee
sentence above written into `docs/guarantees.md` before the adapter exists —
so the thing being built is the thing that was promised.

---

## 3. Days 1–2 — the backend

### Day 1 — Submit, await, classify

- **[A]** `BeamMovementBackend` beside `MovementExecutor`, behind the same
  interface the worker already calls. The worker must not learn which backend
  ran — it asks for a node to be executed and is told when the result is
  durable. If it has to learn, the seam is in the wrong place.
- **[A]** Pipeline construction from a `PlanNode`: the partition bounds the plan
  recorded, not recomputed. Recomputing at execution time lets a partition move
  under a replay, which is the bug the Postgres path already documents.
- **[A]** Failure classification. Beam reports job states; Gantry's
  `FailureClass` decides retry versus quarantine. A runner-level failure and a
  data-level one are different, and treating a bad row as a transient fault
  retries forever.
- **[B]** `ArtifactLanguage.BEAM` — the enum has had exactly one value since v1,
  and a second one is the point at which it stops being decoration.

**Exit:** one partition moves Postgres→Postgres through the Direct runner, and
the resulting `CommitResult` is indistinguishable to the worker from the
Postgres path's.

### Day 2 — Checkpoints across a boundary

- **[A]** Partition groups: the plan already names partitions, so a group is a
  contiguous run of them and the job covers exactly that range. A checkpoint is
  written for **every partition in the group, and only once the job reports
  `DONE`** — never on `RUNNING`, and never per partition, because Dataflow has
  told us nothing about individual partitions.
- **[A]** Proven rather than asserted: a checkpoint exists if and only if the
  data it describes is durable. The test that matters is the inverse — a job
  that fails after writing 90% of its group leaves **no** checkpoints, and the
  re-run is safe because the writes are idempotent.
- **[A]** The submission itself must be idempotent. A worker that crashes
  between submitting a job and recording that it submitted must not start a
  second one — deterministic job naming from the plan node id, and a submitted
  job is adopted rather than duplicated.
- **[B]** Job identity in the trail: which runner, which job id, so an operator
  can find it in the runner's own console.

**Exit:** kill a worker between submit and checkpoint; restarting adopts the
running job rather than launching a second. **This is the new failure mode Beam
introduces and the Postgres path does not have** — there, a crash mid-copy kills
the copy, and here the work carries on without anyone watching it.

---

## 4. Days 3–4 — the guarantees, one at a time

### Day 3 — Crash replay and idempotency

Run the existing chaos suite against the Beam backend. It should need new
fixtures and **no new assertions** — the guarantees are the same guarantees.

- **[A]** `kill -9` mid-movement: nothing lost, nothing duplicated.
- **[A]** Duplicate and shuffled delivery: Beam retries bundles by design, so
  the target's idempotent write is doing more work here than on the Postgres
  path, not less.
- **[B]** Where a guarantee needs the runner's cooperation, say which runner.
  "Holds on Flink, unproven on Dataflow" is a useful sentence; "holds" is not.

**Exit:** the chaos suite passes against Beam, or the table in §0 gains a row
saying what does not hold and why.

### Day 4 — Ordering and stale writes

- **[A]** Source position through the pipeline. Beam bundles are unordered by
  design, and the stale-write rejection the Postgres path relies on becomes
  load-bearing rather than incidental.
- **[B]** Snapshot ↔ CDC handoff is **explicitly out of scope** for Beam in
  v1.2, and the reason is worth stating: the handoff is LSN-stamped and
  Postgres-specific, and making it engine-neutral is its own piece of work.

**Exit:** shuffled and replayed bundles land the same rows as in-order delivery.

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

If the plan is running clean by Day 6, spend it on the Flink runner — the
Direct runner is single-process and proves the API, not the distribution.

### Day 7 — Rehearsal

A third suite in `scripts/rehearsal.py` (`--suite beam`), same shape as the
other two:

1. Seed; plan a Movement with `execution: beam`
2. Move Postgres→Postgres via the Direct runner; verify
3. Kill a worker mid-job; restart; adopt the running job, not a second one
4. Corrupt a chunk; localise; repair one partition
5. Move Postgres→the non-Postgres target; verify
6. Show the guarantee table with its real answers

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
| 1 | Flink runner | Direct proves the contract; distribution is a scale claim, not a correctness one |
| 2 | The non-Postgres target (Day 5) | Costs the reach claim entirely — say so in the docs rather than implying it |
| 3 | Beam-side checksums | Fall back to "verification unsupported on this target", named explicitly |
| 4 | Job adoption after crash (Day 2) | Only if replaced by a refusal: a duplicate job is worse than a stopped migration |
| 5 | Configurable group size → one group per dataset | Simplest possible backend; the crash cost becomes the whole dataset, and the docs must say so |

**Never cut:** the guarantee table. Shipping a second execution backend without
a per-backend statement of what holds would make every guarantee in the project
ambiguous, including the ones that are fine.

---

## 8. Risk register

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| **Gantry becomes a Beam wrapper** | **Critical** | **Medium** | The §0 question is the whole plan. If Beam owns partitioning, checkpointing and retries, Gantry owns nothing — measure what survives on Day 3, not Day 7 |
| Cross-language transforms drag in a Java expansion service | High | **High** | Prove the JDBC path on Day 0. If it needs a Java sidecar, that is a stack change and belongs in the same decision as the architecture |
| Beam's Python SDK weight and startup cost | Medium | High | Optional extra (`gantry[beam]`), never a core dependency (§17) |
| Checkpoint granularity collapses silently | **Critical** | Medium | The guarantee table is the artifact that prevents this; fill it in as you go rather than at the end |
| Group size chosen without measuring | High | Medium | Day 0 measures submission cost and quota ceilings first. A default picked by taste trades a crash cost nobody computed against a bill nobody predicted |
| Someone reads `DONE` as "the rows are right" | Medium | Medium | `DONE` means committed, not correct. Verification is a separate stage and stays exactly where it is |
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
- [ ] Two consecutive clean rehearsal runs
- [ ] Nothing in `gantry/movement/worker` or the scheduler knows which backend ran

---

## 10. What this does not settle

**Whether Beam is the right dependency.** It is a large one, and the plan buys
reach and scale at the cost of a Java expansion service and a runner to operate.
If Day 0 shows the JDBC path needs a sidecar and the submission overhead makes
per-partition checkpointing impractical, the honest conclusion may be that a
MySQL source adapter delivers more of the same value for a fraction of the cost.

**That conclusion is an acceptable outcome of this plan**, and reaching it on
Day 0 rather than Day 8 is what Day 0 is for.
