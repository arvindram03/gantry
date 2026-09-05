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

Carried forward rather than closed in v1, and stated in
[../guarantees.md](../guarantees.md):

- Adaptive concurrency and rate limits
- Cutover gates and approvals
- Starting a Movement against an Operation already executing half-runs instead
  of refusing — found by the Day 19 rehearsal
- Causal claims. Gantry reports correlation with its strength basis stated, and
  does not assert cause.
