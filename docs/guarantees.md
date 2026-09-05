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

## Not yet

- Adaptive concurrency and rate limits (v1.1)
- Cutover gates and approvals (v1.1)
- Analysis verification: row expansion, join coverage, temporal alignment
- Cutover gates and approvals (v1.1)
- Analysis verification: row expansion, join coverage, temporal alignment (Day 18)
