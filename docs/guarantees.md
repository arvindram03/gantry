# Guarantees

What the runtime promises, what it does not, and where the edges are. This file
grows as capabilities land; sections marked *not yet* are honest gaps rather
than omissions.

## Checksum normalisation

Chunk checksums compare a rendering of each row, so two databases holding
identical data must render it identically. They frequently do not. These are
the rules Gantry applies, and the reasoning behind each — this is the part most
likely to need extending when a new column type appears.

| Type | Rendering | Why |
|---|---|---|
| `numeric`, `decimal` | `trim_scale(v)::text` | `1.50` and `1.5` are the same number and different strings |
| `real`, `double precision` | `round(v::numeric, 10)::text` | The last bits of a double are not meaningful data |
| `timestamp with time zone` | `to_char(v AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US')` | Rendering otherwise depends on the session's `TimeZone` |
| `timestamp` | same, without conversion | Explicit format rather than the session's `DateStyle` |
| `date` | `to_char(v, 'YYYY-MM-DD')` | As above |
| `boolean` | `v::int::text` | `t`/`f` versus `true`/`false` varies by client |
| `bytea` | `encode(v, 'hex')` | Independent of the `bytea_output` setting |
| everything else | `v::text` | |

Two further rules apply to every column:

- **NULL becomes `\N`.** A NULL rendered as nothing is indistinguishable from an
  empty string, and a NULL inside a concatenation makes the entire row NULL —
  both silently.
- **Columns are joined with `\x1f`** (ASCII unit separator), so `("ab", "c")`
  and `("a", "bc")` do not render identically.

**Known gaps.** Composite types, ranges, `json` (as opposed to `jsonb`, whose
key order is already normalised by PostgreSQL), and arrays of any of these fall
through to `v::text` and may therefore compare unequal across databases that
render them differently. A checksum mismatch on one of these types is worth
investigating as a normalisation gap before it is treated as data corruption.

### Why sum, not XOR

The checksum sums per-row hashes rather than XOR-ing them. XOR cancels
duplicates: a row written twice would XOR to the same value as no rows at all,
which is invisible to precisely the check meant to catch a duplicated write.
Summing is order-independent in the same way and does not have that property.

Each per-row term is 60 bits so it fits a signed `bigint`, and PostgreSQL's
`sum()` over `bigint` returns `numeric`, so the total cannot overflow.

### Row counts travel with checksums

A checksum alone cannot distinguish an empty range from one whose hashes happen
to cancel. Every comparison carries the row count with it.

## Mismatch localisation

A checksum says a range disagrees; halving it says which half. Locating one
corrupted row among ten million takes **27 comparisons**, not ten million.

Drilling stops when a range is small enough to enumerate (2,000 rows by
default), and only then does the runtime read individual keys — at which point
it can distinguish a missing row from an extra one from a differing one.

**Limitation:** halving requires a numeric single-column key. A composite or
non-numeric key falls back to enumerating the range, which is correct but not
sub-linear.

## Repair

A failed checksum means one partition is wrong, not that the migration is.
`gantry repair <spec> <partition>` re-copies that partition alone and
re-verifies only it. This is safe because the copy is idempotent: re-running a
partition restores exactly the rows that are missing or stale and touches
nothing else.

## Who owns the CDC position

Design document open question #2, answered for v1: **Gantry owns the applied
position.** Kafka's consumer group offsets are a transport detail and the
runtime does not rely on them.

The reason is not distrust of Kafka. A consumer group commit is a *separate
durability domain* from the target write, so committing an offset after
applying a change is a second commit that can fail independently — the
distributed-transaction problem, reintroduced through the back door. Recording
the transport position in the same store as the checkpoint puts progress in one
place that either advances or does not.

Two positions are recorded, because they answer different questions:

| Position | Answers | Used for |
|---|---|---|
| Stream position (topic:partition:offset) | Where do I resume reading? | Restart |
| Source LSN | How old is this row version? | Stale-write rejection |

Consumers run with auto-commit disabled. A resumed consumer uses a brand new
group id in tests specifically to prove that nothing but Gantry's own record
determines where it starts.

## Replication slots

A replication slot is what makes CDC reliable and what can take a source
database down. The slot holds WAL until the consumer confirms it, so a consumer
that stops — crashed, paused, or merely slower than the write rate — makes the
source accumulate WAL indefinitely until the disk fills and it stops accepting
writes. The failure is silent right up until it is total.

Two things that only running the real thing teaches, both now handled:

**A connector reports `RUNNING` before its slot exists.** Kafka Connect's health
says the task started, not that it connected to the source and created the
slot. Treating those as the same thing lets Prepare complete while nothing is
capturing, and the gap stays invisible until the snapshot finishes and CDC has
nothing to catch up from. `wait_for_slot` closes it.

**A slot is not free the instant its connector is deleted.** PostgreSQL refuses
to drop an active slot, so dropping immediately after teardown fails and leaves
the slot pinning WAL forever. `drop_slot` waits for the slot to go inactive.
`drop_orphaned_slots` reclaims what a crashed run abandoned.

Retention is checked against a limit and `check_wal_retention` **raises** rather
than warning. Continuing to stream while the source fills its disk trades a
stalled migration for a stopped database, which is much worse than the outcome
being avoided.

## Ordering and stale writes

Ordering is scoped per entity key, the design document's default. Two changes
to the same row are applied in source order; two changes to different rows are
not ordered against each other, so the runtime never pays for the global
ordering the document warns about buying by accident.

**Routing by key hash** is what makes that enforceable with more than one
worker: every change to a key lands in the same lane, so no two workers can
race on one row. The hash is stable across processes — Python's `hash()` is
randomised per process, and two workers would disagree about the same key.

**Stale writes are rejected, not applied.** Every write carries the source LSN
it came from, and the target accepts a change only if it is newer than what it
holds:

```sql
ON CONFLICT (key) DO UPDATE SET … WHERE target.source_lsn < EXCLUDED.source_lsn
```

An equal LSN is a duplicate delivery and is declined by the same guard.
Rejections are counted and reported, never silent — the count is how an
operator sees that delivery order is not being trusted.

Measured on a real target: the same eight events, shuffled and delivered three
times, produce **an identical final state** to the ordered stream, with 16
rejections instead of zero.

### Deleted rows stay deleted

A delete removes the row, which removes the LSN the guard compares against — so
an insert that arrives after the delete but originated before it would
resurrect the row. A **tombstone** records the position at which a key was
deleted, and a change older than the tombstone is refused. A genuinely newer
insert is still applied, because a key can be deleted and re-created.

Tombstones live in the **target** database, not the metadata store. They have
to be written in the same transaction as the delete they describe: a crash
between deleting a row and recording that it was deleted leaves nothing for a
later out-of-order insert to be checked against. That atomicity is worth a
Gantry-owned side table in the target, which already carries a Gantry-owned
`source_lsn` column.

**Requirement:** the target must carry a `source_lsn` column. Without it there
is nothing to compare against, and the applier refuses to start rather than
silently applying whatever arrives last.

### What is not applied automatically

`TRUNCATE` is dead-lettered rather than applied. One event should not be able
to empty a target without a human deciding that is what should happen.

## Dead letters

An event the runtime cannot apply is kept, not dropped — with its payload, so
it can be replayed once the cause is fixed. A queue that records the fact and
discards the event is a counter.

Depth is a first-class signal: a growing queue means the stream is producing
changes the target will not take, which is a different problem from being slow
and needs a different response.

## Snapshot and change stream together

The ordering is the whole of it, and getting it wrong produces a target that
looks correct and is not.

1. **Create the replication slot**, and wait for it to exist.
2. **Capture the source position P**, after the slot exists.
3. **Snapshot**, stamping every row with P.
4. **Apply changes** from the stream; anything at or before P is refused.

Each step exists because of a specific way the alternatives fail.

**The slot comes first.** A slot created after the snapshot begins does not
capture the changes made during it, and those changes are lost with nothing to
indicate they ever happened.

**The position is captured after the slot exists.** A position captured first
names a point the stream cannot replay from.

**The snapshot is stamped with P**, not with whatever the source's own column
holds. A snapshot represents the source as of one position; saying so lets the
ordinary stale-write guard settle every subsequent conflict.

**Overlap is expected, not avoided.** Changes between the slot's creation and P
appear in both phases. They are applied twice and refused the second time,
which is what idempotency is for. Making the phases disjoint instead would
require locking the source — the thing this design exists to avoid.

### The failure the stamping prevents

Without it, a partition copied slowly enough silently undoes changes the stream
has already applied, and nothing downstream can tell, because the row still
looks consistent. The snapshot merge therefore refuses to overwrite anything
newer:

```sql
ON CONFLICT (key) DO UPDATE SET …
 WHERE (target IS DISTINCT FROM excluded)
   AND target.source_lsn < EXCLUDED.source_lsn
```

### One numeric space

Debezium reports each change's LSN as `pg_wal_lsn_diff(lsn, '0/0')`, so the
runtime captures its snapshot position the same way rather than as the `7/9B77D6D0`
text form. Two representations of the same position that cannot be compared are
worse than one.

## What the Analysis compiler will and will not do

Gantry compiles a **fixed vocabulary** — normalise, join, window, aggregate —
onto engines that already have query languages. It is not one itself, and the
places it refuses are as much of the design as the SQL it emits.

**Signals are named, not written.** A spec asks for `p95_latency`; it cannot
supply an expression. Unknown signals are a compile error listing what is
known, and a signal whose required columns the inputs do not provide fails at
compile time rather than at execution time, when it has already cost something.
Adding a signal is a deliberate act: a definition in `gantry/analysis/signals.py`
and a note here. That friction is intended.

**Temporal joins take exactly one row.** `nearest_preceding` compiles to a
lateral subquery ordered and limited to one, not to a join on a time
comparison — which would match every candidate within the distance and multiply
the left side by however many there are. That is the row-expansion failure the
verification layer exists to catch, and it is better not to generate it.

**An Analysis spanning engines is refused.** v1 compiles onto a single engine;
saying so beats picking one and reading the rest wrongly.

**`nearest` is not supported.** Nearest in which direction is a question the
spec does not answer.

### Determinism

The same Analysis compiles to the same artifact, byte for byte. Compilation
time is deliberately outside the content hash, so recompiling produces the same
identity whenever it happens — which is what makes an artifact hash usable in
provenance.

### Semantics discovery cannot supply

A catalog knows a column is `timestamptz`; it does not know that column is what
orders the data, and a temporal join has to be told. Declarations of that kind
— the time field, sensitive fields, agent access policy — live in a Dataset
spec, and **rediscovery preserves them**. Without that, registering a Dataset
spec and then rediscovering would alternate between two manifests for one
table, which is the version churn content addressing exists to prevent.

## Validation

Generating an artifact is not permission to run it. Validation is the gate, and
its output has a specific job: when it refuses, it must say what to change.

Nothing in validation raises on a failure — failures are **values**, structured
well enough for a planner or an agent to act on. A stack trace tells a human
something went wrong and tells an agent nothing.

| Check | What it catches | Suggested repair |
|---|---|---|
| `inputs_exist` | A dataset the registry does not hold | rediscover |
| `syntax` / `plan` | A column that does not exist, a malformed query | edit spec |
| `cost` | An artifact estimated to read more than policy allows | raise limit |
| `sample` | Errors that only appear at execution time | edit spec |

Checks run cheapest first and stop where later ones become meaningless: a plan
that will not plan is never sampled, and an unregistered input is refused
without asking the engine at all. Failure detail carries the **engine's own
words**, trimmed to the line that identifies the problem.

## Deterministic row order

The compiled query orders by its grouping keys. Not cosmetic: two things read
rows by position — comparing the same artifact across engines, and deriving
findings, which takes the first group as the baseline and the last as the
current. Without an `ORDER BY`, PostgreSQL and DuckDB returned the same groups
in different orders, which would have inverted the direction of every finding
depending on where the Analysis ran.

A deterministic artifact that returns non-deterministic row order is not
deterministic.

## Running on more than one engine

Two engines is the minimum that keeps the abstraction honest. With one, "engine
adapter" means whatever PostgreSQL happens to do. DuckDB reads the same
PostgreSQL tables in place rather than being handed a copy — a copy would make
the engines agree for the wrong reason.

The same compiled SQL runs unmodified on both. Two differences are real, and
recorded here rather than smoothed over in a comparison:

**Interpolating aggregates agree only to the input's precision.**
`percentile_cont` interpolates; DuckDB keeps the input's `DECIMAL` scale through
the interpolation while PostgreSQL promotes to double. Over a `numeric(10,2)`
column the two differ in the hundredths — `531.9505` against `531.95`. Counts
and sums agree exactly; percentiles agree to about 1e-4 relative.

**The engines disagree about Python types**, in both directions. The same
aggregate returns `Decimal` from one and `float` from the other. Anything
comparing results across engines has to normalise first, or it will report
inequality on values that agree.

**Estimates are not universally available.** PostgreSQL reports rows and cost on
the first `EXPLAIN` line; DuckDB renders a tree with no such figure. The DuckDB
adapter reports no estimate rather than parsing a number out of prose.

## What an Analysis Result asserts

An engine reporting SUCCESS says the query ran. It says nothing about whether
the numbers mean what the Analysis claimed they would. Four checks stand
between the two, and each catches a way a successful query can still be wrong:

| Check | The failure it catches |
|---|---|
| `rowExpansion` | A join multiplied its input, so every aggregate over it counts some rows more than once |
| `joinCoverage` | A join stayed small by matching almost nothing, describing a different population than the one asked about |
| `temporalAlignment` | A temporal join stayed inside its bound while joining events barely related in time |
| `nullRate` | A grouping or comparison field is null often enough that the groups are not the ones named |

Row expansion is measured against the **left input's own row count under the
same normalisation**, not the raw table: comparing a filtered join to an
unfiltered base would report expansion for a window predicate. The reference
scenario measures `1.0000x` with its temporal qualifier and `2.0000x` without
it — the same SQL, the same successful execution, on opposite sides of the
boundary.

**A failed check withholds the findings; it does not caveat them.** A
conclusion drawn from a computation the runtime has rejected is not a weak
finding, and publishing it with a warning attached invites it to be quoted
without one. The Result is still written, with `verification_failed` status and
the evidence for the refusal, because *why* nothing was concluded is itself
worth keeping.

**A strength number must say where it came from.** Every finding carries a
`strength_basis` — `deterministic`, `statistical`, or `model_judgement` — and
it is required, not defaulted. A number that sometimes means "measured" and
sometimes means "a model felt fairly sure" is worse than no number, because
nothing downstream can tell which it is. A finding claiming either measured
basis must carry the measurements the strength was derived from; only a
`model_judgement` may stand alone, and it is labelled wherever it is rendered.

## Tracing a finding to its data

One call walks the whole chain:

```
finding -> Result -> generated artifact -> Dataset versions
        -> the Movement that produced them -> its checkpoints
```

Each link answers a different question. The artifact says what computation ran,
by content hash rather than by name. The Dataset versions say what it read —
the exact versions pinned at plan time, not whatever is current now. The
Movement checkpoints say how far the data had got when it was read, which is
what turns "these numbers" into "these numbers, from data complete up to here".

**Gaps are named, not omitted.** An artifact that was not retained, a Dataset
pin whose content hash no longer matches the registered version, a reference
carrying no hash at all — each is reported as an unresolved link. A chain that
silently drops what it could not resolve looks exactly like a chain that
resolved completely, which is the one failure this is meant to prevent. A pin
whose version still exists but whose content has changed is treated as
dangling: answering with the current version would answer the question with
data the Result never saw.

## Asking whether a finding still holds

`gantry results refresh` re-executes **the artifact the Result came from**,
addressed by content hash, and reports how each finding's measurements have
moved. It does not recompile: a recompilation is a different computation unless
proven otherwise, and comparing against it would answer a question nobody
asked. A Result whose artifact was not retained is refused rather than
approximated.

Refresh does not write a new Result. A Result needs findings derived under the
spec's own rules and a verification pass behind them, and neither survives in
the stored artifact — emitting one from a re-execution alone would produce
something that looks verified and is not.

A group that has disappeared is reported as absent, not as zero. Zero is a
measurement; the group being gone is a different fact, and collapsing the two
turns a vanished cohort into a dramatic improvement.

## What an agent may reach

Agents do not start with rows. The RFC's ladder, in order, with what each rung
can expose:

| Rung | Returns | Default |
|---|---|---|
| `describe` | Schema, keys, physical reference | allow |
| `profile` | Counts, null rates, distribution | allow |
| `query` | A grouped aggregate | allow |
| `partition` | How the data splits, including key values at the boundaries | allow |
| `sample` | A bounded number of raw rows | **deny** |
| `records` | Raw rows by key, unbounded | **deny** |

The shipped defaults are the design document's: rows deny, aggregates allow,
metadata allow, PII redacted, samples capped at 50 rows and requiring a stated
reason, evidence persisted.

**Enforcement is in the deterministic path, not the prompt.** Every method that
can return Dataset content calls the gate and masks its output with the
decision it got back. There is no system prompt asking a model to behave and no
tool description it could reinterpret — there is a function it must call to get
anything at all, and the decision depends on the policy and the manifest and
nothing else. A request cannot carry an override, because there is no field for
one.

**A Dataset can tighten the global policy and never loosen it.** The effective
decision is the stricter of the two, which is the only composition rule that
does not need a precedence table nobody remembers.

**Aggregates are structured, not SQL.** `rows: deny, aggregates: allow` is only
enforceable if "is this an aggregate" is decidable, and over arbitrary SQL it is
not — deciding it would mean parsing every dialect Gantry dispatches to, and
being wrong once means an agent read raw rows through a rule that said it could
not. So the API takes a grouping and a fixed vocabulary of measures. Every field
is checked against the manifest before anything is built, which is also why a
field name cannot carry SQL: an identifier that is not in the schema never
reaches one.

**Counting a sensitive column is not printing it.** `count`, `count_distinct`,
`sum`, `avg` and `null_rate` reduce a column to a number that reveals no stored
value, and masking them would withhold a figure that gives nothing away. `min`
and `max` do return a stored value, one bound at a time, so they are masked like
a projection of the column itself.

**Masking applies to output columns, not source fields.** An aggregate's output
is aliased — `min_email`, not `email` — and masking by source name against an
aliased output matches nothing while the decision still reads REDACT. A mask
that is not applied is worse than one that was never promised.

**A small group is refused, not silently dropped.** With `minGroupSize` above 1,
a result containing any group under the minimum is refused whole. Filtering the
small groups out would answer a different question than the one asked, and would
not say that it had.

**The trail records refusals as carefully as grants.** A log holding only what
was permitted reads as clean history while an agent probes every rung on every
Dataset and is turned away each time. Each row says who asked, for what, at
which rung, what policy answered, and on what grounds.

### The group-of-one gap

At the shipped `minGroupSize: 1`, an aggregate grouped by a unique key returns
one row per record: row access wearing a `GROUP BY`. The policy language can
close this — raise `minGroupSize` — and the default does not, because choosing
a threshold is a decision about a specific dataset's sensitivity that the
runtime cannot make for an operator. It is called out here rather than left to
be discovered.

## Not yet

- Adaptive concurrency and rate limits (v1.1)
- Cutover gates and approvals (v1.1)
- **Starting a Movement against an Operation that is already executing.** The
  Day 19 rehearsal hit this: a run interrupted mid-execution leaves leases and
  quarantined partitions, and starting again on a fresh plan version ran the
  partitions the new plan named while the old state was still in place, then
  reported a verification failure. It should refuse and say what to clear
  instead of half-running (v1.1)
- Causal claims: Gantry reports correlation with its strength basis stated, and
  does not assert cause
