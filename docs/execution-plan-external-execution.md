# Gantry v1.2 — Movement as a Job

**Status:** Draft 4 — jobs run in containers; no `postgres_fdw`, no relay
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

**The default job is a SQL script running in a container.** The container
connects to the source and the target and runs the script. Gantry submits it and
watches; it never holds a data connection.

**The relay goes.** v1's `_pipe_copy` streams one `COPY` into another through a
bounded queue *inside the Gantry worker*, making a single Python process the
throughput ceiling and putting a crash **in** the data path. It is deleted.

### It is still a relay — that is fine, and the difference is the whole point

The containerised script pipes `COPY … TO STDOUT` into `COPY … FROM STDIN`.
Bytes pass through *something*. What changed is what that something is:

| | v1 relay | containerised job |
|---|---|---|
| The mover is | the Gantry worker | a disposable container |
| Scaling it means | scaling Gantry | running more containers |
| Its crash takes down | the orchestrator | one unit of work |
| It is scheduled by | nothing — it is just there | the executor, with its own resources |

An orchestrator that is also the bottleneck cannot schedule around itself.
Moving the relay into the job is not a cosmetic change of address.

### Two axes, not one

| | What it is | Examples |
|---|---|---|
| **Job** | *what* to run, generated and content-addressed | a SQL script, a Beam pipeline |
| **Executor** | *where* to run it | Docker, Kubernetes Jobs, Dataflow |

A SQL-script job runs on Docker locally and Kubernetes in production without
being regenerated. A Beam job needs a Beam runner. Keeping the axes separate is
what stops "run it on Kubernetes" from becoming a fork of the job format.

### What Gantry connects to a database for, and what it does not

"No bytes through Gantry" invites an obvious objection — *verification reads
every row*. It does not:

| Purpose | What crosses the wire | Through Gantry |
|---|---|---|
| discover, profile | catalog rows, planner statistics | yes — bounded, tiny |
| **verify** | **one checksum and one count per chunk**, computed in the engine | yes — one row per chunk |
| positions, checkpoints | a single LSN | yes |
| **bulk movement** | **every row** | **never** |

Adapters stay exactly as they are for the first three. They are the control
plane, they are bounded by construction, and a checksum over ten million rows
returns one number. The rule is not "Gantry opens no connections" — it is
**Gantry never carries a payload proportional to the data**.

### What Gantry keeps, whatever runs the job

- **partitioning** — the plan decides the unit of work, never the executor
- **the commit boundary** — defined by the script Gantry generated
- **verification** — the acceptance test, run from outside the mover
- **checkpoints** — written only against verified work
- **provenance** — including the job itself

An executor that wants to decide any of these is a replacement, not an executor.

---

## 1. The default job, concretely

A SQL script per partition, run by `psql` in a container:

```sh
psql "$SOURCE" -v ON_ERROR_STOP=1 \
     -c "COPY (SELECT … WHERE key >= :lo AND key < :hi) TO STDOUT (FORMAT binary)" \
| psql "$TARGET" -v ON_ERROR_STOP=1 --single-transaction -f apply.sql
```

**`--single-transaction` with `ON_ERROR_STOP=1` is what makes the exit code a
commit signal.** Any error aborts and rolls back; a zero exit means the
transaction committed. So:

> **exit 0 ⟹ committed.** Non-zero ⟹ rolled back, *or* killed after commit and
> before exit — which is safe, because the write is idempotent and re-running
> converges.

That gives the `sql` kind the strongest properties of any job kind:

| Job kind | Commit boundary | Checkpoint unit | What a checkpoint asserts |
|---|---|---|---|
| `sql` in a container — **default** | Gantry's, written into the script | **partition** | committed **and** verified |
| `beam` (Dataflow batch) | the runner's; no user checkpoint | partition **group** | committed **and** verified |
| arbitrary container | none — exit code only | whatever it was given | **verified**, and nothing else |

Partition granularity survives because **container startup is seconds, not
minutes**. That is the whole reason one job per partition is affordable here and
absurd on Dataflow.

### The executor interface

```text
submit(job)  -> handle      # idempotent: resubmitting adopts, never duplicates
poll(handle) -> state       # pending | running | done | failed
```

Then, identically for every kind:

```text
submit  →  poll to terminal  →  verify the unit  →  checkpoint what verified
```

**Nothing is checkpointed on a terminal state alone.** Exit 0 is a mover saying
it finished. This project exists not to take that class of claim at face value,
and verification costs nothing extra: `verify_dataset` already runs every check
at partition scope at the end of a Movement, so verifying a unit when its job
finishes is the same scan volume moved earlier — and earlier stops a doomed
movement at unit 2 of 40 rather than after all 40 are paid for.

**Repair does not need checkpoints.** It needs partition bounds, which live in
the plan. A single-partition job repairs, under any executor.

---

## 2. Day 0 — measurements, some already taken

### The transport is already measured, and it is the fastest option

1M narrow rows, one machine, Docker loopback:

| Transport | 1M rows | rows/sec |
|---|---|---|
| **`COPY … TO STDOUT \| COPY … FROM STDIN`** — the containerised script | **1.81 s** | **~552k** |
| `postgres_fdw`, `fetch_size 50000` | 2.60 s | ~385k |
| `postgres_fdw`, default `fetch_size 100` | — | ~280k |

**Dropping `postgres_fdw` costs nothing and gains ~40%.** It also removes the
extension, the foreign server, the user mapping and the privileges each needs —
so the default path has **no database prerequisite at all**.

Caveats, because it is one run on one machine: loopback rather than a real
network, narrow rows, and no staging-plus-merge. The like-for-like number with
the apply step is Day 0 work.

### What Day 0 must actually measure

- **[A]** **Container startup to first row**, on Docker and on Kubernetes. This
  is the number that decides whether one job per partition is affordable, and
  the whole partition-granular guarantee rests on it.
- **[A]** The script end to end *with* staging and merge, against v1's relay
  numbers, so the comparison in `docs/benchmarks.md` is honest.
- **[A]** Concurrency: how many containers the executor will run at once, and
  what that does to the source.
- **[B]** Image size and cold-pull cost. A 900 MB image pulled per partition is
  a startup cost nobody predicted.

### The `beam` job kind: the runner has answered half of it

Assume Dataflow. **There are no Beam checkpoints for a bulk copy** — snapshots
are a *streaming* feature, and a batch pipeline has only internal bundle retry,
invisible to the submitter.

| Job state | What the target contains |
|---|---|
| `DONE` | everything the pipeline wrote is committed |
| `FAILED` / `CANCELLED` | **partially written** — `JdbcIO` commits per bundle |

So the unit is the job, and the choice is how much work goes in one. One job per
partition is correct and economically unusable on Dataflow: batch provisions
workers per job, against per-project quotas on concurrent jobs and creation rate.

- **[A]** Submission-to-first-row and teardown; these set the floor on group size.
- **[A]** Current quota ceilings.
- **[A]** Whether the Python JDBC path needs a Java expansion service in practice.

### Two failure modes, and only one costs work

| What died | The work | Response |
|---|---|---|
| the Gantry worker | the job is still running | **adopt it** — nothing lost |
| the job | the unit is partially written | **re-run it** — idempotent, bounded |

The first is what job-based execution introduces and the relay never had: there,
a dead worker killed the copy. Here the work carries on unwatched.

**Exit:** container startup measured, a decision on one-job-per-partition, and
the per-kind guarantee table filled in as far as it can be before code exists.

---

## 3. Days 1–2 — the seam, and the default job kind

### Day 1 — `MovementJob`, `JobExecutor`, and a Docker executor

- **[A]** `MovementJob` as a generated artifact: kind, body, unit, content hash.
  Compiled at Generate, retained, referenced from the `MovementResult`.
  Retrofitting provenance is how provenance ends up incomplete.
- **[A]** `JobExecutor` protocol — `submit` / `poll` — and a Docker
  implementation. Credentials reach the container as environment or mounted
  secrets and **never appear in the job body**, which is retained and readable.
- **[A]** The worker drives the interface uniformly and must not learn which
  executor ran. Anything that leaks is the seam in the wrong place.
- **[B]** `execution:` on a Movement spec, defaulting to `sql`.

**Exit:** a Movement compiles to a retained job artifact and the worker submits
and polls it through the interface.

### Day 2 — The SQL script job, and the relay's deletion

- **[A]** Generate the script per partition, with `--single-transaction` and
  `ON_ERROR_STOP=1` so **exit 0 means committed**. Bounds come from the plan,
  never recomputed — recomputing lets a partition move under a replay.
- **[A]** The script is **readable**. It is retained as provenance and an
  operator will eventually run one by hand to work out what happened; SQL that
  reads like machine output wastes the artifact.
- **[A]** **Run the whole chaos suite against it.** New fixtures, **no new
  assertions** — the guarantees are the same guarantees.
- **[A]** **Only once that passes: delete `_pipe_copy` and the relay.** In that
  order. Deleting first leaves the crown jewels unproven for however long the
  replacement takes.
- **[B]** Throughput with staging and merge into `docs/benchmarks.md`, whichever
  way it comes out.

**Exit:** Postgres→Postgres with **no bytes through any Gantry process**, the
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
2. `sql` — a script per partition in a container, no bytes through Gantry
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
| 0 | **Beam entirely** | The containerised `sql` job alone delivers the principle and removes the relay. It costs the reach claim, which the docs must then not make |
| 0b | The Kubernetes executor | Docker proves the interface; Kubernetes is a deployment target, not a correctness claim |
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
| **Container startup makes one-job-per-partition too slow** | **High** | Medium | The partition-granular guarantee rests entirely on it. Measure on Day 0; if it is seconds the design holds, and if it is not, `sql` inherits Beam's grouping problem and the guarantee table says so |
| Credentials leak into a retained job artifact | **Critical** | Medium | The body is provenance and is meant to be read. Secrets reach the container as environment or mounts and never appear in the body — asserted by a test, not by care |
| A container image nobody pinned changes underneath a replay | High | Medium | The image digest is part of the job's content hash, so a changed image is a different job rather than the same one behaving differently |
| The default job kind is chosen by convenience rather than by guarantee | Medium | Low | `sql` is the default because it is the strongest kind — partition-granular, commit boundary retained. If that stops being true, the default moves |
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
- [ ] **No byte of customer data passes through a Gantry process, on any path** —
      and what Gantry does read (catalog, statistics, one checksum per chunk) is
      bounded by construction rather than by the size of the data
- [ ] No credential appears in a retained job body
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

**Where the container runs in production.** Docker is the development answer
and Kubernetes Jobs the obvious production one, but ECS, Cloud Run Jobs and
Nomad are all the same shape. The executor interface is two methods precisely so
this can be answered later without reopening the job format — but "later" is a
real dependency on whoever deploys Gantry, and it should be said out loud rather
than discovered.

**What the image contains.** A `psql` client is enough for the default job, and
"enough" is worth defending: every tool added to that image is a thing that runs
next to production credentials.
