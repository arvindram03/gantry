# RFC 0000 — Gantry

The canonical text lives in [../gantry-spec.md](../gantry-spec.md). This file
records what changed in it, and where the v1 implementation knowingly departs
from it.

## Revisions to the spec

- **rev 1** — Dataset added as the fourth core resource. The Core Resource
  Model said "four" and listed three; Movement, Analysis and Result are all
  defined in terms of Datasets, so leaving it implicit made the other three
  harder to describe. Section 9 split into a generic Operation lifecycle state
  machine with Migration workflow states layered above it — a Movement is not
  a migration and does not imply a cutover.

## Deviations in v1

Each of these is a place the implementation does something the spec does not
say, or does not do something it does. They are listed here rather than
discovered.

### The agent query API takes a structured aggregate, not SQL

The spec sketches `dataset.query(sql, params)`. v1 takes a grouping and a fixed
vocabulary of measures instead.

The policy is `rows: deny, aggregates: allow`, and that is only enforceable if
"is this an aggregate" is decidable. Over arbitrary SQL it is not — deciding it
would mean parsing every dialect Gantry dispatches to, and being wrong once
means an agent read raw rows through a rule that said it could not. Built from
a vocabulary, the answer is decidable by construction.

The same choice makes field checking the whole injection defence rather than an
assist to escaping: an identifier that is not in the manifest never reaches SQL.

### `minGroupSize` is a policy knob the spec does not name

At the shipped default of 1, an aggregate grouped by a unique key returns one
row per record — row access wearing a `GROUP BY`. The policy language can close
this and the default does not, because choosing a threshold is a judgement
about a specific Dataset's sensitivity that the runtime cannot make for an
operator. Documented in [../guarantees.md](../guarantees.md) rather than left
to be found.

### Temporal is the default scheduler, and the queue remains

The spec leaves the scheduler open. v1 defaults to Temporal and keeps the
Postgres leased queue behind the same `WorkflowBackend` interface. Both deliver
at-least-once, so the runtime's guarantees do not move between them — the
activity commits before it checkpoints either way, and idempotent writes are
required rather than preferred.

### No cutover

The spec describes cutover gates, approvals and rollback windows. v1 implements
Movement and stops short of cutover. `cutover:` and `rollback:` blocks parse in
a Movement spec for compatibility and are reported as deprecated placement:
they belong to the Migration workflow, and a Movement does not imply one.

### The compiled query orders its groups

Not in the spec, and load-bearing. Two things read rows by position —
cross-engine comparison, and finding derivation, which takes the first group as
the baseline and the last as the current. Without an `ORDER BY`, PostgreSQL and
DuckDB returned the same groups in different orders, which would have inverted
the direction of every finding depending on where the Analysis ran.

### Findings are withheld, not caveated

The spec says a Result carries verification metadata. v1 goes further: when a
verification check fails, the findings are not published at all. A conclusion
drawn from a computation the runtime has rejected is not a weak finding, and
publishing it with a warning attached invites it to be quoted without one. The
Result is still written, carrying the evidence for the refusal.

### `strength_basis` is required

The spec gives a finding a `strength`. v1 requires it to say where the strength
came from — `deterministic`, `statistical` or `model_judgement` — and a
finding claiming either measured basis must carry the measurements behind it.
A number that sometimes means "measured" and sometimes means "a model felt
fairly sure" is worse than no number, because nothing downstream can tell which
it is.

## What v1.1 proved about §5.2

The spec claims, in §5.2 and again at §1279, that **migration is a workflow
composed from Movements, not a fundamental abstraction**. v1 shipped without
testing that claim; v1.1 built the workflow specifically to find out.

**The claim holds.** Seven days of building Migration — spec and state machine,
composition, prepare, reconcile, gates, cutover, rollback, finalize — required
**35 lines of change to `gantry/movement/`, in one function**, and nothing at
all in `gantry/verification/`.

Those 35 lines are worth describing, because they are the interesting part.
Composing a workflow over Movement exposed a v1 bug: **a Movement never reached
`COMPLETED`.** It stopped at `VERIFYING` and stayed there forever with every
check passed. `gantry status` had been showing it and nobody chased it, because
nothing in v1 ever asked the question — a workflow gate asking "are the
Movements done?" could never have got a yes. The fix is the state machine's own
documented rule finally implemented: `COMPLETED` is reachable only through
verification.

So the diff is not Migration reaching into Movement to bend it. It is Migration
asking a question v1 never asked and finding the answer missing. **That is what
a correctly factored abstraction looks like under a new consumer**, and had the
diff been large, the honest conclusion would have been that Movement was
factored around migration all along.

Two things made this measurable rather than a matter of opinion: the number was
taken on day 2 rather than day 7, and it was written into the plan as the exit
criterion before any code existed.

## Deviations in v1.1

### Gantry does not switch traffic

The spec's §7 Phase 8 describes cutover as an operation the system performs.
v1.1 decides whether you *may* cut over, records that decision with its
evidence, and holds the rollback window open — but redirecting an application
is the deploy system's job. Owning a connection string would make Gantry a
proxy, which is the same category error as owning the bytes on the wire, and it
would make the whole workflow untestable against anything but a toy.

### Rollback is never automatic

§7 Phase 9 keeps the source authoritative and prefers traffic rollback over
reverse bulk migration, which v1.1 follows. What it adds is a refusal: the
window reports divergence and **will not act on it**. Divergence may mean the
migration was wrong, or it may mean the application is now writing to the
target correctly and the source is stale by design. Nothing in the runtime can
distinguish those from the outside.

### No schema transformation, no reverse CDC, no decommissioning

§7 Phase 10 lists source decommissioning among the finalize steps. v1.1 marks
the source decommissionable and stops. Reverse CDC ("optional… where
supported") is not implemented. Schema transformation was never in the spec and
is explicitly out: a migration tool that also rewrites your data model is two
products.

### Two gates the spec does not name

`targetHealthy` and `schemaCompatible` are additions. Both read facts the
runtime measures anyway — a write probe and Prepare's compatibility check
re-run at cutover time — and both exist because the §7 Phase 8 gate list is
introduced with "example gates" rather than as a closed set.

## Open questions

The spec's section 23 open questions are tracked as issues. Two were answered
during v1:

- **#1 Temporal** — adopted. v1 defaults to it, with the leased queue retained
  behind the same interface. The original recommendation was to defer; the
  decision to adopt was taken deliberately against it.
- **#2 CDC offset ownership** — Gantry owns the applied-position checkpoint in
  its own metadata store. Kafka consumer offsets are a transport detail: an
  offset says a message was delivered, not that its effect was committed.
  Correctness must not depend on connector bookkeeping.

## Known gaps

Carried forward rather than closed, and stated in
[../guarantees.md](../guarantees.md):

- Adaptive concurrency and rate limits (§10)
- **A second source adapter.** v1 and v1.1 both migrate PostgreSQL to
  PostgreSQL, which is not the migration most people need. This is the adoption
  blocker rather than a correctness one, and it is the next piece of work.
- Multi-source and multi-region topologies
- Causal claims. Gantry reports correlation with its strength basis stated, and
  does not assert cause.

Closed since v1:

- ~~Cutover gates and approvals~~ — shipped in v1.1
- ~~Starting a Movement against an Operation already executing half-runs
  instead of refusing~~ — fixed before Migration was built, because a cutover
  gate reads Operation state to decide whether it is safe to move production
  traffic, and a gate above a state machine that half-runs can say yes when the
  answer is no.
