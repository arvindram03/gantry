# Gantry v1.2 — External Execution

**Status:** Draft 2 — restructured around a principle, not a dependency
**Source of truth:** `gantry-spec.md` (RFC 0) §5, §8, §17
**Predecessors:** [`execution-plan-v1.md`](execution-plan-v1.md) → `v0.1.0`,
[`execution-plan-migration.md`](execution-plan-migration.md) → `v0.2.0`
**Window:** 8 working days
**Target release:** `v0.3.0` — Gantry stops moving data

---

## 0. The principle

> **Gantry never moves data.** It plans the work, generates the instruction,
> hands it to something that executes, and verifies the outcome.

v1 does not honour this, and the violation is in the reference implementation.
`_pipe_copy` relays one `COPY` stream into another through a bounded queue in
the Gantry worker: a single Python process is the throughput ceiling for a
hundred-million-row partition, and a crash there is a crash **in** the data path
rather than beside it. "Python moves buffers, engines move rows" was a narrower
definition defending a design that had already drifted.

The correction is not Beam-specific. It applies to Postgres first, because
that is where the drift is.

### What it changes

Every backend becomes the same shape, and the shape is one the lifecycle
already has a name for:

```text
Plan  →  Generate  →  Validate  →  Execute  →  Verify  →  Result
          ↑                          ↑
   the movement instruction    submit it, observe it,
   as a content-addressed      never carry the bytes
   artifact
```

**Generate stops being a no-op for Movement.** Since v1 it has been documented
as "a no-op for operations that compile straight to an engine API"; under this
principle a Movement *does* compile to something — a federated `INSERT … SELECT`,
a Beam pipeline — and that artifact is retained as provenance exactly like
Analysis SQL. A Result can then show the statement that moved its data, which it
cannot today.

### Two executors, one interface

| Executor | Moves data | When |
|---|---|---|
| **federated** | the target server pulls from the source over `postgres_fdw` | same engine, no extra infrastructure beyond an extension |
| **beam** | Dataflow (or Flink, or Direct) | cross-engine, or scale beyond one server |
| ~~relay~~ | the Gantry worker | retained as a fallback, not the default |

**The relay stays.** Every guarantee in this project was proven against it, and
deleting the reference implementation to make a point would be trading proof for
tidiness. It becomes the fallback for environments where `postgres_fdw` cannot
be installed — and it is honestly labelled as the one backend where Gantry is in
the data path.

---

## 1. What must survive

The measurement, per executor. **A guarantee that cannot be kept is written down
as lost, not quietly redefined.**

| Guarantee | relay (v1) | federated | beam |
|---|---|---|---|
| A killed worker loses nothing and duplicates nothing | holds | ? | ? |
| Writes idempotent under duplicated delivery | holds | ? | ? |
| Stale writes rejected by source position | holds | ? | ? |
| Checkpoint unit | partition | ? | **partition group** |
| What a checkpoint asserts | committed | ? | **committed and verified** |
| Repair re-copies one partition | holds | ? | ? |
| Gantry is in the data path | **yes** | no | no |

That last row is the point of the release.

### What stays Gantry's, in every executor

Handing execution outside is not handing over the contract. Gantry keeps:

- **partitioning** — the plan decides the unit of work, not the executor
- **the commit boundary**, wherever one can be had
- **verification** — the acceptance test, run from outside the mover
- **checkpoints** — written only against verified work
- **provenance** — including, now, the instruction that did the moving

An executor that wants to decide any of those is not an executor, it is a
replacement, and the answer is no.

---

## 2. Day 0 — measure both executors before writing either

### Federated: what it costs to not be in the path

`postgres_fdw` is available in the stack but not installed. Installing it, and
creating a server and user mapping, needs privileges the relay never asked for.
That is the trade, and it should be measured rather than assumed away.

- **[A]** Throughput of `INSERT INTO target SELECT … FROM foreign_partition`
  against the relay, on the same partition. **If federated is slower, the
  principle costs something and the docs say so.**
- **[A]** Whether the commit boundary is still Gantry's. It should be — Gantry
  issues the statement inside a transaction it opened — which would make
  federated *strictly better* than the relay: same guarantee, no Python in the
  path.
- **[B]** What the prerequisite actually is, written as an operator would need
  it: extension, server, user mapping, and the privileges each requires.

### Beam: the runner has already answered half of it

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

So the checkpoint unit is the job, and what is left to choose is how much work
goes in one:

| Partitions per job | Submission cost | Cost of a crash |
|---|---|---|
| 1 | a cluster start per partition | one partition |
| all | one cluster start | the whole dataset |
| *n* | ⌈partitions/*n*⌉ starts | at most *n* partitions |

One job per partition is correct and economically unusable: Dataflow batch
provisions workers per job, so sixty-one partitions is sixty-one cluster starts,
against per-project quotas on concurrent jobs and creation rate.

- **[A]** Submission-to-first-row and teardown, which set the floor on group size.
- **[A]** Current quota ceilings. A low one forces group size up regardless of
  what a crash costs.
- **[A]** Whether the Python JDBC path needs a Java expansion service in practice.

**Exit:** measured numbers for both, a default group size, and the per-executor
guarantee rows filled in as far as they can be before code exists.

### Verification is the acceptance test, not the checkpoint

True for both executors, and it follows from what the project already believes:
**an engine reporting success is not a correct result.** `DONE` is Dataflow
saying it finished; a returned `INSERT` is Postgres saying it committed. Neither
is a claim about whether the rows are right.

```text
execute a unit of work  →  observe it finish  →  verify it  →  checkpoint it
```

**It costs nothing extra.** `verify_dataset` already runs every declared check at
dataset scope *and* every partition scope at the end of a Movement, so verifying
a unit when it finishes is the same scan volume moved earlier — and earlier is
better, because a doomed movement stops at group 2 of 40 rather than after all
40 are paid for. Dataset-scope checks still run once at the end; they cannot be
answered a group at a time.

**Repair does not need the checkpoints.** It needs partition bounds, which live
in the plan. A single-partition job repairs under either executor.

Two limits, both real. **The target has to be verifiable** — if Gantry cannot
checksum a sink, there is no acceptance test and `DONE` is all there is, which
makes verification load-bearing for progress rather than only correctness. And
**the source has to be stable while a unit verifies**, which a snapshot Movement
gives and live writes do not, with the watermark caveat from the Migration work.

### Two failure modes, and only one costs work

| What died | The work | Response |
|---|---|---|
| the Gantry worker | the external job is still running | **adopt it** — nothing lost |
| the external job | the unit is partially written | **re-run it** — idempotent, bounded |

The first is the failure mode external execution introduces and the relay does
not have: there, a dead worker kills the copy. Here the work carries on with
nobody watching.

---

## 3. Days 1–2 — the seam, and the cheap executor first

### Day 1 — `MovementExecutor` becomes an interface

- **[A]** Extract the interface the worker already calls. It asks for a plan
  node to be executed and is told when the result is durable; it must not learn
  *which* executor ran. Anything that leaks into the worker is the seam being in
  the wrong place.
- **[A]** `execution:` on a Movement spec — `relay`, `federated`, `beam` —
  defaulting to `relay` until something else has earned it. Changing an
  executor changes what a crash costs, so it is declared rather than inferred.
- **[A]** The **movement artifact**: whatever the executor will run, compiled at
  Generate, content-addressed, retained. This is what makes Generate stop being
  a no-op, and it is worth doing on day one because retrofitting provenance is
  how provenance ends up incomplete.
- **[B]** `ArtifactLanguage` gains its second and third values. An enum with one
  member has never been tested.

**Exit:** the relay runs through the new interface, unchanged, with every
existing test passing and a retained artifact describing what it did.

### Day 2 — Federated, and Gantry leaves the data path

Cheapest executor first, because it is the one that removes Python from the path
without adding infrastructure.

- **[A]** `postgres_fdw` setup as an explicit, idempotent step with its own
  failure mode: a missing extension is a **Prepare-time refusal naming the
  privilege needed**, not a runtime error at row zero.
- **[A]** One statement per partition, inside a transaction Gantry opens. The
  commit boundary stays Gantry's, so **every guarantee should hold unchanged** —
  and the chaos suite is what says whether "should" was right.
- **[A]** Run the whole chaos suite against it. New fixtures, **no new
  assertions**. The guarantees are the same guarantees.
- **[B]** Throughput against the relay, recorded in `docs/benchmarks.md`
  whichever way it comes out.

**Exit:** Postgres→Postgres with **no bytes through the Gantry process**, and
the guarantee table's `federated` column filled in from a test run rather than
an argument. If this column is all "holds", the principle costs nothing here and
federated becomes the default.

---

## 4. Days 3–4 — Beam

### Day 3 — Submit, adopt, classify

- **[A]** `BeamMovementBackend` behind the same interface. Pipeline built from
  the partition bounds the plan recorded, never recomputed — recomputing lets a
  partition move under a replay.
- **[A]** **Idempotent submission.** A worker that dies between submitting and
  recording the submission must not start a second job: deterministic job naming
  from the plan node id, and a running job is adopted rather than duplicated.
  This is the new failure mode, and it is the one thing the relay never had.
- **[A]** Failure classification: a runner-level failure and a bad row are
  different, and retrying a bad row forever is what happens if they are not.
- **[B]** Job identity in the trail — runner and job id, so an operator can find
  it in the runner's own console.

**Exit:** kill a worker between submit and checkpoint; restarting adopts the
running job. One partition group moves Postgres→Postgres through the Direct
runner.

### Day 4 — Groups, verification, and the guarantees

- **[A]** Partition groups sized from Day 0's numbers. A checkpoint is written
  for every partition in a group, **only after the group verifies** — never on
  `DONE` alone.
- **[A]** The chaos suite against Beam. Where a guarantee needs the runner's
  cooperation, **name the runner**: "holds on Direct, unproven on Dataflow" is a
  useful sentence and "holds" is not.
- **[A]** Ordering and stale writes. Beam bundles are unordered by design, so
  the target's stale-write rejection is load-bearing here rather than
  incidental.
- **[B]** Snapshot ↔ CDC handoff is **explicitly out of scope**: it is
  LSN-stamped and Postgres-specific, and making it engine-neutral is its own
  piece of work.

**Exit:** the guarantee table's `beam` column filled in, including whatever does
not hold.

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

1. Seed; run the same Movement under all three executors
2. `relay` — the v1 path, unchanged, as the control
3. `federated` — the same result with no bytes through the Gantry process
4. `beam` — Direct runner, per-group verify-then-checkpoint
5. Kill a worker mid-job under `beam`; restart; adopt, do not duplicate
6. Corrupt a chunk; localise; repair one partition — under each executor
7. Move Postgres→the non-Postgres target via `beam`; verify
8. Print the guarantee table with its measured answers

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
| 0 | **Beam entirely** | `federated` alone delivers the principle for Postgres→Postgres. It costs the reach claim, which the docs must then not make |
| 1 | Flink runner | Direct proves the contract; distribution is a scale claim, not a correctness one |
| 2 | The non-Postgres target (Day 5) | Costs the reach claim entirely — say so in the docs rather than implying it |
| 3 | Beam-side checksums | Fall back to "verification unsupported on this target", named explicitly |
| 4 | Job adoption after crash (Day 2) | Only if replaced by a refusal: a duplicate job is worse than a stopped migration |
| 5 | Configurable group size → one group per dataset | Simplest possible backend; the crash cost becomes the whole dataset, and the docs must say so |

**Never cut:** the guarantee table, and the relay. Shipping a second execution
backend without a per-backend statement of what holds would make every guarantee
in the project ambiguous, including the ones that are fine — and deleting the
implementation every guarantee was proven against, to make an architectural
point, trades proof for tidiness.

---

## 8. Risk register

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| **Handing execution outside becomes handing over the contract** | **Critical** | **Medium** | §1 lists what stays Gantry's: partitioning, the commit boundary, verification, checkpoints, provenance. An executor that wants to decide any of them is a replacement, not an executor |
| Federated is slower than the relay | Medium | Medium | Measure on Day 0 and publish it either way. The principle may cost throughput, and pretending otherwise would be the same dishonesty as the phrasing it replaces |
| `postgres_fdw` cannot be installed where it matters | High | Medium | The relay stays, honestly labelled as the backend where Gantry is in the data path. A principle with no fallback is a deployment blocker |
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
- [ ] **A Postgres→Postgres Movement completes with no bytes through the Gantry process**
- [ ] Every Movement retains the artifact that moved its data, as provenance

---

## 10. What this does not settle

**Whether Beam is the right dependency.** It is a large one, bought for reach
and scale at the cost of a Java expansion service and a runner to operate. If
Day 0 shows the JDBC path needs a sidecar and submission overhead forces coarse
groups, the honest conclusion may be that `federated` delivers the principle and
a MySQL source adapter delivers the reach, for a fraction of the cost.

**That conclusion is an acceptable outcome**, and reaching it on Day 0 rather
than Day 8 is what Day 0 is for. Note what it would *not* undo: `federated` is
the part that removes Gantry from the data path, and it stands whether or not
Beam ever ships.

**Whether the relay should eventually go.** Not decided here. It is the only
backend where Gantry is in the data path, and it is also the only one with no
prerequisites. Making `federated` the default is a Day 6 question; removing the
relay is a later release's, if ever.
