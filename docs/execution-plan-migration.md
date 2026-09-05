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

**Exit: met.** Against the real stack, with three different faults introduced into the demo
target at once:

```text
$ gantry migration prepare spec/examples/migration-orders-to-warehouse.yaml
not ready orders-to-warehouse
  ✗ schema_compatible (public.customers.region -> public.customers): source allows
    nulls, target does not; rows would be rejected                fix: alter_target
  ✗ schema_compatible (public.orders.amount -> public.orders): source is
    numeric(12, 2), target is numeric(6, 2); the target is narrower and would
    truncate                                                      fix: alter_target
  ✗ schema_compatible (public.orders.status -> public.orders): target has no such
    column                                                        fix: alter_target
  public.customers: target also has warehouse_loaded_at
```

`gantry migration start` refuses on the same report, and the assertion that matters is the
row count: **3,000,000 before, 3,000,000 after.** Nothing moved. The workflow returns to
`PLANNED` rather than `FAILED` — an incompatible target is repairable input, not a dead end,
exactly as a failed Analysis validation returns to `DRAFT`.

### Compatibility is directional, and that is the whole design

A target wider than the source is fine; the reverse truncates. A target more permissive about
nulls is fine; the reverse rejects rows the source considers valid. Every rule is written in
the direction data actually flows — a symmetric check would refuse half the migrations that
are safe and permit half that are not.

Three things are deliberately *not* refusals. A target that does not exist yet is reported as
something the run will create, because "about to create four tables" is a thing an operator
may want to stop but not an error. Extra target columns are reported, not refused: a target
may carry its own bookkeeping, and a column nobody remembers adding is worth seeing before a
cutover rather than after. And a snapshot-only migration is not asked for logical decoding,
which would refuse migrations that are perfectly fine.

**Writability is tested, not asked about.** Querying the catalog for granted privileges gets
this wrong in every interesting case — inherited roles, default privileges, a read replica
that answers happily until you write. Creating and dropping a temporary table is the question
actually being asked.

### Two bugs the tests found

`check_reachable` caught `SQLAlchemyError`, but a refused connection surfaces as `OSError` and
never reaches the driver's exception hierarchy — so the one case the check exists for was the
one it missed.

And the refusals rendered as `schema_compatible : source allows nulls…` with the subject
missing. Rich reads `[...]` as markup and had been silently swallowing the column name. A
refusal that loses the name of what it refused is worse than no refusal; the separator is now
parentheses, and a unit test asserts no failure description contains a square bracket.

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

**Exit: met.** Against the real stack, on the 3,000,000-row demo target:

```text
$ gantry migration reconcile spec/examples/migration-orders-to-warehouse.yaml
  public.customers agrees below 1000000 in 4 queries
    count:    agreed — 1,000,000 rows on both sides            (2 queries, 0.15s)
    checksum: agreed — 576066573638…  over 1,000,000 rows      (2 queries, 0.79s)
  public.orders agrees below 3000000 in 4 queries
    count:    agreed — 3,000,000 rows on both sides            (2 queries, 0.39s)
    checksum: agreed — 173008798253… over 3,000,000 rows       (2 queries, 2.59s)
```

With one row corrupted at `order_id = 1777777`:

```text
  public.orders disagrees below 3000000 in 50 queries
    count:    agreed — 3,000,000 rows on both sides
    checksum: disagreed — source 173008798253…, target 173008843032…
    row_diff: disagreed — 1 differing in 23 comparisons
    differing keys: 1777777
```

**23 comparisons over three million rows.** log₂(3,000,000) ≈ 21.5.

### What each layer can and cannot conclude

The ordering is not a style choice. A count is an index scan; a checksum reads every row on
both sides; a row diff reads and compares them. On a migration that matters the difference is
hours.

- Counts agreeing proves nothing about content. Counts *dis*agreeing proves the sides differ,
  so the checksum is **skipped** — confirming it would read everything to learn nothing.
- Checksums agreeing is strong evidence. Disagreeing says nothing about *where*, which is what
  the drill-down is for.
- An empty target is not a disagreement. Saying "disagrees" would be true and useless: the
  phase has not run.

### Reconciling while the stream is still moving

Every comparison is bounded by the **highest key present in the target when reconciliation
began**. Rows written after that point are excluded from both sides, which makes the
comparison stable without pausing the stream — and stops a streaming migration from reporting
replication lag as missing data. The watermark is read from the *target*, the side that lags;
bounding by the source's maximum would include rows the target has not been given yet.

That is exact for append-shaped data and honest about the rest: updates to keys below the
watermark can still race, and the report says `source still moving` rather than pretending
otherwise.

### Two bugs, both of which reported success while being wrong

**The watermark was compared as text.** `id::text <= '1000000'` is a string comparison, and
`'2' > '1000000'` lexically — so a million-row table was bounded to seven rows and
reconciliation cheerfully agreed over them. A comparison that silently narrows to the wrong
subset is worse than one that errors. The bound is now re-typed from the schema, the same way
partition predicates are.

**The drill-down was enumerating, not halving.** It was handed an open lower bound, and it
cannot compute a midpoint without both ends — so it fell back to reading all three million
rows: *one* comparison, twenty-one seconds, correct answer. The verdict looked fine and the
`O(log n)` property from Day 12 was simply gone. Both bounds are passed now, and a test
asserts the comparison count stays within `3·⌈log₂ n⌉` rather than merely that the answer is
right.

### The measurement

**No change to `gantry/movement/` or `gantry/verification/` this day.** Reconciliation is the
v1 framework — the same checksum expression, the same localiser — arranged as a phase. That is
what "verification reused, not duplicated" was supposed to mean, and it held.

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

**Exit: met**, all three against the real stack.

```text
$ gantry migration gates spec/examples/migration-orders-to-warehouse.yaml
gate                          outcome   measured          required
allPartitionsVerified         passed    12/12             every partition verified
maxCdcLag                     disabled  no change stream  not required
criticalVerificationFailures  passed    0                 <= 0
targetHealthy                 passed    healthy           reachable and writable
schemaCompatible              passed    compatible        compatible
requireApproval               failed    nobody            a named operator
orders-to-warehouse: blocked by requireApproval
```

Altering the target *after* planning, then attempting cutover, is refused on `schemaCompatible`
— which is the point of re-evaluating rather than trusting an earlier report. And approved:

```text
$ gantry migration cutover … --approved-by arvind --reason "release window, gates green"
cutting over orders-to-warehouse  approved by arvind

  ready_for_cutover -> cutting_over   operator (arvind)   release window, gates green
```

### Unmeasured is not passed

The property that makes the rest trustworthy. A gate nobody supplied a reading for is
`unknown`, and unknown **blocks** — otherwise forgetting to wire a probe is indistinguishable
from wiring one that always says yes. `GateFacts` is all-optional and `None` means "nobody
measured this" rather than zero, which is why a missing CDC lag reading and a lag of zero
cannot be confused.

The one exception is stated rather than fudged: a snapshot-only migration has no stream, so
`maxCdcLag` is `disabled` with "no change stream" as its measured value. Faking a zero would
have been easier and would read as a measurement nobody took.

**A disabled gate is recorded, not omitted.** An operator reading the report afterwards needs
to see that a check was turned off, not merely fail to see that it ran. Same reasoning for
keeping the gates that passed: the question a post-mortem asks is *what did we believe when we
decided*, and a record of only the failures cannot answer it.

### The retry loop, which was broken

Reconciliation disagreeing sends the workflow back to `CATCHING_UP`, and running again has to
walk `CATCHING_UP → VERIFYING` from where it already is. Every phase transition was
unconditional, so the self-edge raised — **the one path the design most expects to be taken
was the one that failed**, and it only surfaced by driving a real refusal and then retrying.
Phase transitions now no-op when already in the target state.

### The measurement

**No change to `gantry/movement/` or `gantry/verification/`.** Gates read facts; they do not
change how facts are made. Five of the six read something v1 already measured, and the two new
ones — target health and approval — are a connection probe and a recorded human decision.

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

**Exit: met.** The whole cycle against the real stack:

```text
$ gantry migration cutover … --approved-by arvind --reason "release window"
cut over orders-to-warehouse at 37450805752 by arvind at 2026-09-05T20:24:59+00:00
  drain: ok — no change stream to drain
  final_reconcile: ok — 2 dataset(s) agree
  record_position: ok — lsn=37450805752
  rollback window open for 1 day, 0:00:00; source remains authoritative

$ gantry migration window …
holding  orders-to-warehouse: holding, 24.0h left, source authoritative

$ gantry migration finalize …          # while the window is still useful
error: cannot finalize 'orders-to-warehouse': 23:59:43 left before the window closes
```

Corrupt one row in the target and the window notices — and does nothing about it:

```text
diverged  orders-to-warehouse: diverged on public.orders — source is still
          authoritative; rolling back is your call
  divergence is reported, not acted on: it may mean the migration was wrong, or
  that the target is now correct and the source is stale by design

$ gantry migration status orders-to-warehouse
orders-to-warehouse  state=rollback_window          # unmoved
```

```text
$ gantry migration rollback … --decided-by arvind --reason "checkout errors spiked"
rolled back orders-to-warehouse from 37450805752 by arvind
  the source was authoritative throughout; no data moved back
```

### The ordering that looks wrong and is not

The cutover steps run **after** the transition into `CUTTING_OVER`, not before. That reads
backwards until you ask what happens if the drain fails: the moment an operator approves,
traffic is being moved by whatever moves it, and the only safe direction from a half-finished
cutover is back — which is reachable from `CUTTING_OVER` and from nowhere earlier. Doing the
work first and transitioning after would leave the workflow in a state with no way out.

So a cutover that cannot drain lands in `ROLLING_BACK`, not `FAILED`.

### What refuses to be clever

`finalize` refuses while the window is still running *and* while the sides disagree — closing
the window on a divergence would discard the only way back. Reconciling nothing is not
agreement: an empty list of reports must not read as a clean bill of health, because a cutover
on a target nobody checked is a guess.

And the window never rolls back on its own. That is the one place the plan warned about, and
the reasoning holds: divergence may mean the migration was wrong, or it may mean the
application is now writing to the target correctly and the source is stale by design. Nothing
in the runtime can tell those apart from the outside.

### Two gaps the audit trail exposed

The first was mine twice over: I documented that a rollback returns the source to *the point
the cutover recorded*, and then never wired it — `from_position: None` in the trail. Worse, the
first fix hardcoded `PositionKind.LSN` on the way back, which is exactly the assumption
`SourcePosition` exists to prevent. The kind travels with the value now.

The second: `migration audit` printed the gate report as a raw list of dicts — technically
complete, practically unreadable. It renders as `allPartitionsVerified=passed,
maxCdcLag=disabled, …` now. An audit nobody reads is not an audit.

### The measurement

**No change to `gantry/movement/` or `gantry/verification/`.** Cutover, the rollback window and
finalize are entirely workflow — the deepest part of Migration reached without touching the
primitive underneath it.

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

**Exit: met.** Two consecutive clean runs, `make rehearse-migration`:

```text
step                                                            seconds  proved
1. seed and register                                                8.7  200,000 orders, 1,000,000 customers
2. prepare refuses an incompatible target before anything moves     1.3  refused, named the column, moved no rows, stayed replannable
3. movement runs and reconciliation opens the door to cutover      10.8  2 dataset(s) agreed at a stated watermark
4. a corrupted row is localised, repaired, and reconciles clean      4.7  located one row in 15 comparisons
5. cutover refused: a gate fails, and nobody has approved            4.4  a failing gate refused by name; approval blocks
6. approved cutover opens the rollback window                        3.5  cut over at lsn=38109237360, source authoritative
7. divergence is reported, not acted on; rollback is on command      3.4  reported without acting; rolled back from the cutover position
8. re-cut over, wait out the window, finalize, audit                21.0  finalize refused early then completed; audit holds 10 decisions
total                                                               57.8
```

It is a **suite of the existing script** rather than a second one — `--suite {v1,migration,all}`
— because the v1 sequence proves the guarantees and this one proves the workflow composed over
them. Both still pass; `make rehearse` and `make rehearse-migration` run them separately and
`make rehearse-all` back to back.

### Two deviations, both stated rather than papered over

**Step 6 refuses on `schemaCompatible`, not CDC lag.** The demo migration is snapshot-only, so
its lag gate is legitimately `disabled` — forcing a lag reading nobody took would demonstrate
the wrong thing. The refusal is driven by a gate that genuinely applies.

**The rehearsal's rollback window is five seconds, not a day.** `migration-rehearsal.yaml`
differs from the shipped example in that one field. The window's *duration* is policy; what is
on trial is the mechanism — that it holds, that it reports divergence without acting, and that
`finalize` refuses until it elapses. Waiting a real day proves nothing extra, and the step
still waits the window out rather than simulating it.

### Two bugs in the rehearsal itself

A **hardcoded key**. Step 4 corrupted `order_id = 1777777`, which does not exist at rehearsal
scale — so reconciliation agreed, and *a reconciliation that agrees because nothing was broken
looks exactly like one that agrees because everything is right*. The victim key is derived from
the data now, and taken from the middle of the range so the drill-down has to halve.

**Rich wrapping broke a check** for the second time in this project. `"operator (rehearsal)"`
arrived split across two lines and the assertion failed on output that was correct. It asserts
on tokens that survive wrapping now. (The first was Day 3, where `[...]` was swallowed as
markup.) Grepping rendered output is convenient and brittle; where a check matters, it should
not depend on terminal width.

### The measurement

**No change to `gantry/movement/` or `gantry/verification/` this day**, and cumulatively across
the whole plan: **35 lines, in one function.** Those lines fixed a v1 bug — Movements never
reaching `COMPLETED` — that composing a workflow *exposed* rather than caused.

The hypothesis this plan existed to test was RFC 0 §5.2: *migration is a workflow composed from
Movements, not a fundamental abstraction*. Seven days of building one, through prepare,
reconcile, gates, cutover, rollback and finalize, needed 35 lines from the primitive
underneath. **The claim holds.**

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

**Exit: met.** Verified by dropping every table on all three databases and the entire metadata
schema, then following the README:

```text
$ gantry migration prepare …
ready orders-to-warehouse: ready, creating 2 target(s)
  + will create target public.customers
  + will create target public.orders

$ gantry migration start …
ready_for_cutover orders-to-warehouse
  public.orders agrees below 200000 in 4 queries

$ gantry migration gates …
orders-to-warehouse: blocked by requireApproval

$ gantry migration cutover … --approved-by arvind --reason "release window"
cut over at 38810574752 by arvind
  drain: ok · final_reconcile: ok — 2 dataset(s) agree · record_position: ok

$ gantry migration rollback … --decided-by arvind --reason "checkout errors spiked"
rolled back orders-to-warehouse from 38810574752 by arvind

$ gantry migration audit orders-to-warehouse
  ready_for_cutover -> cutting_over   operator (arvind)
      gates: allPartitionsVerified=passed, maxCdcLag=disabled, …
```

The last line is the criterion: *exactly which facts the decision rested on*, recovered from
the trail rather than reconstructed.

**Shipped:** `docs/migration.md`, Migration guarantees and — the section that matters — its
absences in `docs/guarantees.md`, RFC 0 with what v1.1 proved about §5.2 and four deviations
written down, README with Migration in "How do you use it?" and traffic-routing added to "What
Gantry is not", and `v0.2.0`.

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
