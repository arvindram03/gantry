# Gantry v1.2 — Apache Beam Execution Plan

**Status:** Draft 1
**Source of truth:** `gantry-spec.md` (RFC 0) §5, §8, §17
**Predecessors:** [`execution-plan-v1.md`](execution-plan-v1.md) → `v0.1.0`,
[`execution-plan-migration.md`](execution-plan-migration.md) → `v0.2.0`
**Window:** 8 working days
**Target release:** `v0.3.0` — Movement over a data path Gantry does not run

---

## 0. What this proves

Every version so far has moved bytes with its own code. `copy_partition` opens a
connection to the source, streams through a bounded queue, and commits on the
target — and the whole crash-replay guarantee rests on that call returning only
after a durable commit, at which point the worker checkpoints.

Beam breaks that. A Beam pipeline is submitted to a runner, executes somewhere
else, and finishes asynchronously. Gantry does not hold the connection, does not
see the rows, and cannot observe the commit directly.

So the question is not "can we call Beam" — it is:

> **Can Gantry own the execution contract over a data path it does not run?**

That is the thesis under test. Gantry claims to own checkpoints, replay,
ordering, idempotency, verification and provenance while engines own the data.
Postgres-to-Postgres never really tested it, because Gantry *was* the data path.
Beam is the first case where something else moves the bytes and Gantry still has
to be able to say what is durable.

### The measurement

The Migration plan measured lines changed in `gantry/movement/`. This one
measures something harder and more honest — **which guarantees survive**:

| Guarantee | Postgres path | Beam path — to be filled in |
|---|---|---|
| A killed worker loses nothing and duplicates nothing | holds | ? |
| Writes are idempotent under duplicated delivery | holds | ? |
| Stale writes rejected by source position | holds | ? |
| Checkpoint granularity | per partition | ? |
| Repair re-copies one partition, not the table | holds | ? |
| Verification is order-independent and localises in `O(log n)` | holds | ? |

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

**Do not write an adapter until this is answered.** Three architectures are
possible and they differ in what a crash costs. Spike all three far enough to
measure; pick with numbers.

**(A) One job per partition.** Gantry submits a pipeline per partition, awaits
it, checkpoints. The commit-then-checkpoint ordering is *unchanged* — "commit"
becomes "the job reported success" — so every guarantee holds as written. The
cost is job submission: seconds on the Direct runner, a minute or more on
Dataflow, paid per partition.

**(B) One job for the dataset, Gantry polls.** Cheap to submit, and checkpoint
granularity collapses to the whole dataset. A crash at 95% re-runs everything.
Simple, fast, and a real weakening.

**(C) One job that reports back.** The pipeline's write step commits data and a
checkpoint marker together, or emits per-bundle completion Gantry consumes.
Keeps fine-grained checkpoints, needs a transactional sink, and puts Gantry's
code inside someone else's pipeline.

**Exit:** a table of measured submission overhead per runner, and a decision
recorded with its reasoning. **My expectation, to be tested rather than
assumed:** (A) for correctness now, (C) documented as where this goes, (B)
rejected — a guarantee that evaporates at scale is worse than one that was never
claimed.

---

## 3. Days 1–2 — the backend

### Day 1 — Submit, await, classify

- **[A]** `BeamMovementBackend` beside `MovementExecutor`, behind the same
  interface the worker already calls. The worker must not learn which backend
  moved the bytes — if it has to, the seam is in the wrong place.
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

- **[A]** Whatever Day 0 chose, implemented and *proven* rather than asserted:
  a checkpoint exists if and only if the data it describes is durable.
- **[A]** The submission itself must be idempotent. A worker that crashes
  between submitting a job and recording that it submitted must not start a
  second one — deterministic job naming from the plan node id, and a submitted
  job is adopted rather than duplicated.
- **[B]** Job identity in the trail: which runner, which job id, so an operator
  can find it in the runner's own console.

**Exit:** kill a worker between submit and checkpoint; restarting adopts the
running job rather than launching a second. **This is the new failure mode Beam
introduces and the Postgres path does not have.**

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
| Job submission overhead makes partitioned movement absurd | High | Medium | Day 0 measures it. If per-partition submission costs a minute, architecture (A) is unusable at real partition counts and (C) becomes required rather than aspirational |
| The Direct runner passes and Flink does not | Medium | Medium | Say which runner each guarantee was proven on. "Holds" without a runner name is not a claim |
| Two data paths diverge over time | Medium | High | The worker must not know which backend ran; anything that leaks into it is the seam being wrong |

---

## 9. Definition of done

- [ ] A Movement runs on Beam through the same worker interface as the Postgres path
- [ ] Job submission is idempotent — a crash between submit and checkpoint adopts, never duplicates
- [ ] The chaos suite runs against Beam, and every guarantee is either proven or written down as lost
- [ ] Data lands in one non-Postgres target, verified or explicitly unverifiable
- [ ] **`docs/guarantees.md` states what holds per backend**, and a reader can choose from it
- [ ] Two consecutive clean rehearsal runs
- [ ] Nothing in `gantry/movement/worker` or the scheduler knows which backend moved the bytes

---

## 10. What this does not settle

**Whether Beam is the right dependency.** It is a large one, and the plan buys
reach and scale at the cost of a Java expansion service and a runner to operate.
If Day 0 shows the JDBC path needs a sidecar and the submission overhead makes
per-partition checkpointing impractical, the honest conclusion may be that a
MySQL source adapter delivers more of the same value for a fraction of the cost.

**That conclusion is an acceptable outcome of this plan**, and reaching it on
Day 0 rather than Day 8 is what Day 0 is for.
