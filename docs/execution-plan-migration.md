# Gantry v1.1 — Migration Workflow Execution Plan

**Status:** Draft 1
**Source of truth:** `gantry-spec.md` (RFC 0) §5.2, §7, §9.2–9.3
**Predecessor:** [`execution-plan-v1.md`](execution-plan-v1.md) — shipped as `v0.1.0`
**Window:** 8 working days
**Target release:** `v0.2.0` — Migration as a workflow over Movements

---

## 0. What this proves

v1 shipped four resources and one lifecycle. It also made an architectural claim it did not
test:

> Migration is a workflow composed from Movements, not a fundamental abstraction.
> — RFC 0 §5.2, §1279

That claim is currently unproven. If it is right, Migration is assembly: workflow states above
the Operation state machine, gates over facts the runtime already measures, and an approval
record. If it is wrong, building it will require reaching into Movement and changing it — and
**that** is the signal worth watching. A Migration that cannot be built without modifying
Movement means Movement was factored around migration all along and the abstraction is
decorative.

**So the exit criterion for the whole plan is two things, not one:** a database migration runs
end to end with gated cutover and a working rollback — *and* the diff to `gantry/movement/`
is small enough to read in one sitting. The second is the real test.

### What Migration adds that Movement does not have

| | Movement | Migration |
|---|---|---|
| Scope | one Dataset set, moved once or continuously | a whole cutover, composed of Movements |
| Ends when | the data is there and verified | traffic has moved and the rollback window has closed |
| Decides | whether the copy is correct | whether it is *safe to switch* |
| Needs approval | no | yes — a recorded human decision |
| Reversible | re-run it | rollback window, with the source still authoritative |

Movement answers "is the data right?". Migration answers "may we act on that?" — and those
are different questions, which is why they are different resources rather than a flag.

---

## 1. Scope

| In scope | Out of scope |
|---|---|
| `kind: Migration` spec and domain model | Automated traffic switching |
| Workflow state machine (§9.2) above Operation states | Reverse CDC |
| Cutover gates over already-measured facts | Source decommissioning automation |
| Recorded approval, with agents able to propose only | Multi-region or multi-source topologies |
| Rollback window with the source authoritative | Schema *transformation* during migration |
| Reconciliation as a distinct phase (§7 Phase 7) | A second source adapter (parallel track) |
| Final audit report | Adaptive concurrency (still deferred) |

**The largest deliberate cut: Gantry does not flip your traffic.** It decides whether you
*may*, records that decision with the evidence behind it, and holds the rollback window open.
Redirecting your application is your deploy system's job. Owning it would be the same category
error as owning the bytes on the wire — and it would make Gantry untestable against anything
but a toy.

**The second cut: no schema transformation.** Migration moves data between schemas that are
already compatible, and refuses when they are not. A migration tool that also rewrites your
data model is two products.

---

## 2. Day 0 — the crack in the foundation

**This is not part of Migration. Do it first anyway.**

Day 19 of v1 found that starting a Movement against an Operation already `EXECUTING` half-runs
instead of refusing: an interrupted run leaves leases and quarantined partitions, and a fresh
plan version runs the partitions it names while the old state is still there — then reports a
verification failure that looks like a data bug.

Today it is an annoyance. Under Migration it is a correctness problem, because
`CATCHING_UP → READY_FOR_CUTOVER` is a gate that reads Operation state to decide whether it is
safe to move production traffic. A gate above a state machine that half-runs is a gate that
can say yes when the answer is no.

- **[A]** `MovementService.run` refuses when the Operation is `EXECUTING` under a *different*
  plan version, naming what to clear and which version is in flight.
- **[A]** Resuming the *same* plan version stays legal — that is replay, and it is the
  behaviour the whole design rests on. The test must distinguish the two.
- **[B]** `gantry status` shows in-flight leases and quarantined partitions, so the refusal
  message points at something the operator can see.

**Exit: met.** Reproduced against the real stack — an Operation left `EXECUTING` on version 1,
the source grown so a replan yields version 2:

```text
$ gantry plan examples/postgres-to-postgres/movement.yaml
error: operation 'orders-snapshot' is still executing version 1; refusing to start
version 2 alongside it. Wait for it to finish, or `gantry pause orders-snapshot
--reason ...` to drain it and then plan again. `gantry status orders-snapshot`
shows what is still in flight.

$ gantry pause orders-snapshot --reason "draining to replan"
$ gantry plan  examples/postgres-to-postgres/movement.yaml
planned orders-snapshot v2  12 nodes, 6 partitions
$ gantry start examples/postgres-to-postgres/movement.yaml
ok orders-snapshot.movement  2,000,000 rows in 29.7s   partitions 6/6   16/16 checks passed
```

Guards on all three entry points — `plan` (which is where the conflicting submit happened),
`run`, and `run_on_temporal`.

**Two things this turned up.** The first message recommended `gantry abort`, which reaches
`FAILED` — terminal by design, since repair starts a new attempt rather than reviving one. The
advice would have left an Operation nobody could restart. It recommends `pause` now, and the
test follows the message's own advice end to end rather than matching its text.

The second: the plan version did not travel with the transition into `EXECUTING`, so the
record kept whatever it last saw and the guard could compare against a stale number. It is
carried through now, on both the queue and Temporal paths.

---

## 3. Days 1–2 — the Migration resource

### Day 1 — Spec, model, state machine

- **[A]** `kind: Migration` in `gantry/spec/migration.py`: metadata, the Movements it
  composes, `cutover.gates`, `rollback.window`, `reconciliation`. The `CutoverBlock` and
  `RollbackBlock` **already parse** — v1 deliberately dropped them from the Movement domain
  model with a deprecation warning. This is where they land properly.
- **[A]** Workflow state machine (§9.2) in `gantry/lifecycle/migration.py`, layered *above*
  the Operation states rather than replacing them:
  `DRAFT → DISCOVERING → PLANNED → PREPARING → SNAPSHOTTING → CATCHING_UP → VERIFYING →
  READY_FOR_CUTOVER → CUTTING_OVER → ROLLBACK_WINDOW → COMPLETED`, plus `FAILED`, `PAUSED`,
  `ROLLING_BACK`, `ROLLED_BACK`.
- **[A]** Every transition persisted, with `ActorKind` — which already distinguishes
  `RUNTIME`, `OPERATOR` and `AGENT`, and already encodes "agents propose, they do not execute".
- **[B]** `gantry validate` accepts a Migration spec; `gantry schema show Migration` emits it.

**Watch for:** the urge to give Migration its own checkpoint store, its own plan format, or its
own verification. Each is a signal that the shared machinery did not fit, and each should be
argued for out loud rather than quietly added.

**Exit: met.** A Migration spec parses, the state machine rejects illegal transitions naming
what would have been allowed, and `gantry/movement/` is untouched.

Two decisions worth recording. **Gate defaults are the strict answer**, so an omitted gate is
still enforced — otherwise the spec becomes a place to quietly turn checks off, and relaxing
one has to be written down. And `CUTTING_OVER` / `ROLLING_BACK` are **operator-only by
construction**: both move production traffic, and neither is something a model may decide.

### Day 2 — Composition and the migration store

- **[A]** A Migration *drives* Movements rather than reimplementing them: `SNAPSHOTTING` and
  `CATCHING_UP` delegate to `MovementService`, and the Migration's state is derived from the
  Movements' Operation states.
- **[A]** `migrations` and `migration_transitions` tables, with an Alembic migration. Append
  only, like every other table that records a decision.
- **[B]** `gantry migration {plan,start,status}` — status shows the workflow state, the
  Movements beneath it, and which gate is currently blocking.
- **[B]** Dependency ordering across Movements, reusing the plan DAG rather than a second
  ordering implementation.

**Exit: met.** Against the real stack, driving the actual two-Dataset demo Movement:

```text
$ gantry migration start spec/examples/migration-orders-to-warehouse.yaml
verifying orders-to-warehouse
  ✓ orders-snapshot  completed  plan v1

$ gantry migration status orders-to-warehouse
orders-to-warehouse  state=verifying
  ✓ orders-snapshot  completed  plan v1
  recent transitions
    draft -> discovering   runtime  resolving the movements this migration composes
    discovering -> planned runtime  composed from 1 movement(s)
    planned -> preparing   runtime  checking targets before moving anything
    preparing -> snapshotting  runtime  running 1 movement(s)
    snapshotting -> catching_up  runtime  applying changes made during the snapshot
    catching_up -> verifying     runtime  reconciling source against target
```

### The measurement

**35 lines added to `gantry/movement/`, in one function.** That is the number this plan exists
to take, and it says the composition claim is holding so far.

What those lines are is more interesting than how many. Composing a workflow over Movement
exposed a real v1 bug: **a Movement never reached `COMPLETED`.** It stopped at `VERIFYING` and
stayed there forever, even with every check passed — `gantry status` had been showing it and
nobody chased it, because nothing in v1 ever asked the question. A workflow gate asking "are
the Movements done?" could never have got a yes.

The fix is the state machine's own rule, finally implemented: `COMPLETED` is reachable only
through verification, and an engine reporting success does not by itself get you there. So the
diff is not Migration reaching into Movement to bend it — it is Migration *asking a question
v1 never asked* and finding the answer was missing.

**How the workflow drives a Movement matters as much as that it does.** The runner is injected
rather than constructed: `MigrationService.run` knows that a Movement can be asked to run and
that it reports an Operation state. It does not know about engines, partitions, checkpoints or
spec files, and the CLI supplies all of that. Movement state is **read** from the Operations
on every `status` call rather than mirrored into the Migration — two records of one fact drift,
and the one an operator happens to read decides what they believe.

---

## 4. Days 3–4 — Prepare and Reconcile

### Day 3 — Prepare (§7 Phase 4)

Cutover is where a migration fails loudly. Prepare is where it fails *early*, which is much
cheaper.

- **[A]** Target schema creation and **compatibility checking**: types, nullability, keys,
  and the cases where the target can hold the source's values but not the reverse.
  Incompatibility is a structured refusal naming the column and the reason — the same shape
  Analysis validation uses, and for the same reason: an agent can act on it, a stack trace
  cannot.
- **[A]** Connectivity, permissions and replication-slot validation, before anything moves.
  A migration that discovers at hour six that it cannot create a slot has wasted six hours.
- **[B]** Dry-run verification queries: prove the reconciliation queries *run* against an
  empty target before there is data to reconcile.
- **[B]** Index strategy: which target indexes to create before the snapshot and which to
  defer until after, since indexes slow bulk load and their absence slows verification.

**Exit:** an incompatible target is refused during `PREPARING` with a structured report naming
every offending column — before a single row moves.

### Day 4 — Reconcile (§7 Phase 7)

Reconciliation is verification run as a *phase* rather than as a per-partition check: layered,
cheapest first, and drilling only where it must.

- **[A]** Layered reconciliation reusing the v1 verification framework: counts → aggregates →
  chunk checksums → constraints, stopping at the first layer that settles the question.
- **[A]** Row-level drill-down only on mismatched chunks, reusing the `O(log n)` localisation
  from v1 Day 12.
- **[B]** A `ReconciliationReport` attached to the Migration as evidence, carrying what each
  layer checked, what it cost, and where it stopped.
- **[B]** Reconcile **while catch-up continues** — the count that matters is the one taken at
  a consistent position, not one taken with the stream paused.

**Exit:** reconciliation on a migrated Dataset with live writes reports agreement at a stated
position, and a deliberately corrupted chunk is localised without a full row-level scan.

---

## 5. Days 5–6 — Gates and cutover

### Day 5 — Gates and approval

The heart of the workflow, and the part most likely to be built wrong by making it clever.
**A gate is not a heuristic. It is a fact the runtime already measures, compared to a
threshold the spec declared.**

- **[A]** Gate evaluation over facts that already exist:

  | Gate | Fact it reads | Already measured? |
  |---|---|---|
  | `allPartitionsVerified` | per-partition verification state | yes, v1 Day 11 |
  | `maxCdcLag: 2s` | CDC lag | yes, v1 Day 15 |
  | `criticalVerificationFailures: 0` | verification results by severity | yes, v1 Day 11 |
  | `targetHealthy` | target adapter health probe | **new** |
  | `schemaCompatible` | Day 3's compatibility check | yes, Day 3 |
  | `requireApproval` | a recorded operator decision | **new** |

- **[A]** `GateReport`: every gate, its measured value, its threshold, and pass or fail.
  Persisted as evidence. **A gate that passes is as worth recording as one that fails** — the
  question a post-mortem asks is "what did we believe when we cut over?".
- **[A]** Approval: an `OPERATOR` transition carrying identity, timestamp and reason. An
  `AGENT` may *propose* `READY_FOR_CUTOVER`; the transition is refused without an operator
  approval on record. This is the access-ladder principle applied to a state machine — the
  constraint is in the transition function, not in a prompt.
- **[B]** `gantry migration gates <name>` prints the table, so "why can't I cut over" is one
  command rather than an investigation.

**Exit:** cutover is refused with a gate report naming the failing gate and its measured
value; an agent-proposed cutover is refused for want of approval; an approved cutover with all
gates green proceeds. All three recorded.

### Day 6 — Cutover and the rollback window

- **[A]** `CUTTING_OVER`: re-check gates immediately before the transition — a gate report
  from ten minutes ago is a claim about ten minutes ago — then quiesce, drain CDC to zero lag,
  final reconcile, and record the cutover position.
- **[A]** `ROLLBACK_WINDOW`: the source stays authoritative for the configured period. Gantry
  keeps verifying both sides and will *say* if they diverge; it does not switch back on its
  own, because deciding to roll back is an operational judgement with a blast radius.
- **[A]** `ROLLING_BACK`: return authority to the source, with the reason recorded. Traffic
  rollback rather than reverse bulk migration, per §7 Phase 9.
- **[B]** `FINALIZE`: final reconciliation, disable CDC, drop the replication slot, mark the
  source decommissionable, emit the audit report.

**Watch for:** the temptation to make rollback automatic on divergence. Divergence during the
rollback window may mean the migration was wrong, or it may mean the application is now
writing to the target correctly and the source is stale *by design*. The runtime cannot tell
those apart, and guessing would be worse than reporting.

**Exit:** a migration cuts over, holds the rollback window, and rolls back on command with the
source authoritative throughout. The audit report reconstructs every decision and who made it.

---

## 6. Days 7–8 — Proof and release

### Day 7 — The rehearsal

Extend `scripts/rehearsal.py` rather than writing a second one. The v1 rehearsal already
drives the public surfaces and already found five real bugs by doing so.

1. Seed a source; start continuous writes
2. `gantry migration plan` → `PREPARING` catches an incompatible target column
3. Fix it; snapshot and catch up under live writes
4. Reconcile → agreement at a stated position
5. Corrupt a chunk → reconciliation localises it → repair → reconcile clean
6. Attempt cutover → **refused**, CDC lag above threshold
7. Let it settle → attempt cutover as an agent → **refused**, no approval
8. Approve → cutover → rollback window opens
9. Roll back → source authoritative, reason recorded
10. Re-cut over → finalize → audit report

**Exit:** two consecutive clean runs from a fresh `make dev-up`. **And measure the Movement
diff** — the number that answers the question this plan exists to ask.

### Day 8 — Docs and release

- `docs/migration.md`: the workflow, the gates, what Gantry decides and what it refuses to
  decide for you.
- `docs/guarantees.md`: what Migration guarantees, and — the section that matters — what it
  does not. Traffic switching, reverse CDC and schema transformation are all *absences*, and
  they should be as easy to find as the features.
- RFC 0: fold in whatever this plan proved wrong. If Migration needed Movement to change, say
  so plainly; that is a finding about the abstraction, not an embarrassment.
- README: Migration in "How do you use it?", and in "What Gantry is not" — **it still does not
  flip your traffic**.
- Tag `v0.2.0`.

**Exit — v1.1:** a stranger clones, runs a migration end to end with a gated cutover and a
rollback, and can say afterward exactly which facts the decision rested on.

---

## 7. Cut-lines

Cut in this order. Each buys about a day and preserves the claim being tested.

| # | Cut | Why it is safe |
|---|---|---|
| 1 | `FINALIZE` phase | The rollback window is the interesting part; finalize is bookkeeping |
| 2 | Index strategy (Day 3) | Correctness does not depend on it; speed does |
| 3 | Target health gate | Five gates prove the mechanism as well as six |
| 4 | Reconcile-under-live-writes → reconcile at a quiesced position | Weaker, but honest if documented |
| 5 | Dry-run verification queries | Prepare still catches the schema failures that matter |

**Never cut:** the approval gate, the gate *report*, the rollback window, or the Day 0 guard.
An ungated cutover is not a migration tool — it is a copy script with more states.

---

## 8. Risk register

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| **Migration cannot be built without changing Movement** | **Critical** | **Medium** | This is the plan's actual hypothesis. Measure the diff on Day 2, not Day 7. If it is already large, stop and redesign rather than pushing through — the finding is more valuable than the feature |
| Gates become heuristics | High | Medium | A gate reads a measured fact and compares it to a declared threshold. Anything that needs a judgement call is an approval, not a gate |
| Cutover becomes traffic management | High | **High** | Stated as out of scope in three places, including the README. The moment Gantry holds a connection string it is a proxy |
| Rollback window auto-reverts on divergence | High | Medium | Report divergence, never act on it. The runtime cannot distinguish a bad migration from a correct one whose source is now stale by design |
| Approval is a boolean in a spec file | Medium | **High** | An approval is a persisted transition with an identity, a timestamp and a reason. A spec field saying `approved: true` is not an approval, it is a comment |
| Reconciliation reimplements verification | Medium | Medium | Reconciliation is a *phase* that calls the v1 framework. A second checksum implementation is a second thing that can be subtly wrong |
| The 15 workflow states never all occur | Low | Medium | Fine. States that exist for symmetry are cheap; the rehearsal exercises the path that matters |

---

## 9. Definition of done

- [ ] A Migration spec parses, versions, and drives Movements it does not reimplement
- [ ] Every workflow transition is persisted with an actor; agents propose and cannot execute
- [ ] Cutover is refused unless every declared gate passes, with the report kept either way
- [ ] Approval is a recorded operator decision, not a field in a file
- [ ] The rollback window keeps the source authoritative and rollback works on command
- [ ] Reconciliation reuses v1 verification rather than duplicating it
- [ ] Two consecutive clean rehearsal runs from a fresh stack
- [ ] **The diff to `gantry/movement/` is small enough to read in one sitting**

That last box is the one to look at first. It is the whole reason this plan exists.

---

## 10. What comes after

Not in this plan, and ordered by what unblocks the most:

1. **A second source adapter (MySQL).** v1 and v1.1 both migrate PostgreSQL to PostgreSQL,
   which is not the migration most people need. This is the adoption blocker, and it is a
   clean parallel track — the adapter interfaces are small and documented.
2. **Adaptive runtime controls** (§10) — deferred from v1, still deferred.
3. **The agent planner** — the LLM authoring side. Everything it would need already exists:
   structured refusals, a gated access ladder, and proposals that cannot execute themselves.
