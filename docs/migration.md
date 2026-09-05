# Migration

A Migration is a **workflow composed from Movements**. It is not a fifth
resource alongside Dataset, Movement, Analysis and Result — it is the first
thing built *on* them, and that distinction is load-bearing rather than
decorative. Building it took 35 lines from `gantry/movement/`, in one function,
and those fixed a bug composition exposed rather than one it caused.

Movement answers *is the data right?* Migration answers *may we act on that?*
Those are different questions, which is why they are different resources rather
than a flag.

## What Gantry decides, and what it refuses to decide

**Gantry does not move your traffic.** It decides whether you *may* cut over,
records that decision with the evidence behind it, and holds the rollback
window open. Redirecting an application is your deploy system's job; owning it
would make Gantry a proxy — the same category error as owning the bytes on the
wire.

**Gantry does not roll back on its own.** Divergence during the rollback window
may mean the migration was wrong, or it may mean the application is now writing
to the target correctly and the source is stale *by design*. Nothing in the
runtime can tell those apart from the outside, so the window reports and waits
to be told.

**Gantry does not transform your schema.** It moves data between schemas that
are already compatible and refuses when they are not. A migration tool that
also rewrites your data model is two products.

## The workflow

```text
DRAFT → DISCOVERING → PLANNED → PREPARING → SNAPSHOTTING → CATCHING_UP
      → VERIFYING → READY_FOR_CUTOVER → CUTTING_OVER → ROLLBACK_WINDOW
      → COMPLETED

           FAILED · PAUSED · ROLLING_BACK · ROLLED_BACK
```

These are **workflow** states above the Operation lifecycle, not instead of it.
While a Migration is `SNAPSHOTTING`, the Movements beneath it run their own
`DRAFT → … → COMPLETED` and keep their own checkpoints. The workflow state says
which phase of the cutover we are in; the Operation states say what is durable.

Two structural promises live in the transition table:

**Cutover is one-way through the gates.** `READY_FOR_CUTOVER` is reachable only
from `VERIFYING`, and `CUTTING_OVER` only from `READY_FOR_CUTOVER`. No edge
skips gate evaluation, because an edge that exists will eventually be taken.

**Rollback is available only while there is something to roll back to** — from
`CUTTING_OVER` and `ROLLBACK_WINDOW`, nowhere else. Once `COMPLETED`, the source
has been released.

## The spec

```yaml
apiVersion: gantry.dev/v1alpha1
kind: Migration
metadata:
  name: orders-to-warehouse
movements:
  - movement: orders-snapshot
    spec: examples/postgres-to-postgres/movement.yaml
cutover:
  gates:
    allPartitionsVerified: true
    maxCdcLag: 2s
    criticalVerificationFailures: 0
    targetHealthy: true
    schemaCompatible: true
    requireApproval: true
rollback:
  window: 24h
  sourceRemainsAuthoritative: true
```

Movements are **named, not embedded**. A Movement is a resource with its own
lifecycle, plan versions and checkpoints; inlining one would make the Migration
own execution.

**Gate defaults are the strict answer.** An omitted gate is still enforced, so
the spec cannot become a place to quietly turn checks off — relaxing one has to
be written down.

## Running one

```bash
gantry migration plan      spec/examples/migration-orders-to-warehouse.yaml
gantry migration prepare   …    # check the target, move nothing
gantry migration start     …    # run the Movements, up to verification
gantry migration reconcile …    # compare source against target
gantry migration gates     …    # why can you or can't you cut over
gantry migration cutover   … --approved-by NAME --reason WHY
gantry migration window    …    # where the rollback window stands
gantry migration rollback  … --decided-by NAME --reason WHY
gantry migration finalize  …    # close the window, release the source
gantry migration audit     NAME # every decision, and who made it
```

`make rehearse-migration` runs the whole sequence end to end in about a minute.

## Prepare: fail early, or fail expensively later

Cutover is where a migration fails loudly. Prepare is where it fails at a cost
of one minute rather than a snapshot, a catch-up, and whatever was scheduled
around them.

**Compatibility is directional.** A target wider than the source is fine; the
reverse truncates. A target more permissive about nulls is fine; the reverse
rejects rows the source considers valid. A symmetric check would refuse half
the migrations that are safe and permit half that are not.

Three things are deliberately *not* refusals: a target that does not exist yet
(reported as something the run will create), extra target columns (a target may
carry its own bookkeeping), and logical decoding for a snapshot-only migration.

**Writability is tested, not asked about.** Querying the catalog for granted
privileges is wrong with inherited roles, default privileges, or a read replica
that answers happily until you write. Creating and dropping a temporary table
is the question actually being asked.

## Reconciliation: layered, cheapest first

| Layer | Cost | What agreement proves | What disagreement proves |
|---|---|---|---|
| count | index scan | nothing about content | the sides differ — stop |
| checksum | reads both sides | content matches | *that* they differ, not where |
| row diff | `O(log n)` checksums | — | which rows |

A count mismatch **skips** the checksum: it already proves the sides differ, and
confirming it would read every row to learn nothing.

**Reconciling under live writes** bounds every comparison by the highest key
present in the target when reconciliation began. Rows written after that are
excluded from both sides, which makes the comparison stable without pausing the
stream — and stops a streaming migration from reporting replication lag as
missing data. The watermark comes from the *target*, the side that lags.

Exact for append-shaped data, honest about the rest: updates to keys below the
watermark can still race, and the report says `source still moving` rather than
implying a frozen instant.

## Gates

**A gate is not a heuristic.** It reads a fact the runtime already measured and
compares it to a threshold the spec declared. Nothing estimates, infers, or asks
a model. If a decision needs judgement it is an approval, and approvals are
recorded with a name attached.

| Gate | Reads |
|---|---|
| `allPartitionsVerified` | per-partition verification state |
| `maxCdcLag` | CDC lag |
| `criticalVerificationFailures` | verification results by severity |
| `targetHealthy` | a connection and write probe |
| `schemaCompatible` | Prepare's compatibility check, re-run |
| `requireApproval` | a recorded operator decision |

**Unmeasured is not passed.** A gate nobody supplied a reading for is `unknown`
and blocks — otherwise forgetting to wire a probe is indistinguishable from
wiring one that always says yes. The one stated exception: a snapshot-only
migration has no stream, so `maxCdcLag` is `disabled` with "no change stream" as
its measured value. Faking a zero would read as a measurement nobody took.

**A disabled gate is recorded, not omitted**, and so are the gates that passed.
The question a post-mortem asks is *what did we believe when we decided*, and a
record of only the failures cannot answer it.

**Gates are re-evaluated at the moment of cutover**, not trusted from an earlier
`gates` call. A column altered during the six hours of snapshot is exactly what
this catches.

## Approval

An `AGENT` may propose `READY_FOR_CUTOVER`. Neither an agent **nor the runtime**
can reach `CUTTING_OVER` or `ROLLING_BACK`: both move production traffic, and
the refusal lives in the transition function rather than in policy that could be
relaxed — the access-ladder principle applied to a state machine.

An operator transition without an `actor_id` is refused. A cutover approved by
"operator" and nobody in particular is a checkbox, not an approval.

## Cutover and the rollback window

A cutover drains the stream, reconciles one last time, records the exact source
position both sides agreed at, and notes who decided. That position is what
makes a rollback meaningful — without it, "go back to the source" names no
particular instant.

The steps run **after** the transition into `CUTTING_OVER`, which reads
backwards until you ask what happens when the drain fails. The moment an
operator approves, traffic is being moved by whatever moves it, and the only
safe direction from a half-finished cutover is back — reachable from
`CUTTING_OVER` and nowhere earlier. So a cutover that cannot drain lands in
`ROLLING_BACK`, not `FAILED`.

`finalize` refuses while the window is still running **and** while the sides
disagree: closing on a divergence would discard the only way back.

## The audit trail

Every transition is persisted with the actor who caused it, the reason, and the
evidence behind it — the gate report for a cutover, the failing layers for a
refused reconciliation. `migration_transitions` is append-only, like every table
that records a decision.

```bash
gantry migration audit orders-to-warehouse
```

## What this does not do

Written here rather than left to be discovered:

- **Traffic switching.** Gantry decides; your deploy system acts.
- **Reverse CDC.** Rollback is traffic rollback, not a reverse bulk migration.
- **Schema transformation.** Compatible schemas only.
- **Automatic rollback on divergence.** Reported, never acted on.
- **Source decommissioning.** `COMPLETED` marks the source decommissionable;
  decommissioning it is yours.
- **Multi-source or multi-region topologies.**
- **A second source adapter.** v1.1 migrates PostgreSQL to PostgreSQL, which is
  not the migration most people need. This is the adoption blocker, and it is a
  separate piece of work.
