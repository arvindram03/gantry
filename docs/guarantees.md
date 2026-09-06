# Guarantees, per execution backend

Gantry runs work it did not write, in places it does not control. What survives
that depends on which backend ran it, and this page says so per backend rather
than making one claim and hoping.

Every "holds" here **names the runner it was proven on**. "Holds" without a
runner name is not a claim: a guarantee that needs the runner's cooperation is
only as good as the runner you actually used.

## What stays Gantry's, whatever runs the work

These do not vary by backend, because they are decided before anything is
submitted and checked after it returns:

- **Partitioning.** The bounds come from the plan and are never recomputed by a
  job. Recomputing lets a partition move under a replay.
- **What may be believed.** A job reaching `DONE` means *committed*, never
  *correct*. Nothing is checkpointed on `DONE` alone.
- **Provenance.** The job body is retained and readable, and carries no
  credential. Every Movement can name the job that moved each partition.
- **Job identity.** The packaging is part of the job's content hash, so a
  package changing underneath a replay is a *different job* rather than the same
  one behaving differently.

## The table

| Guarantee | `sql` | `beam` |
|---|---|---|
| **Checkpoint unit** | **one partition** | **a group of partitions** |
| What a crash costs | that partition, redone | that whole group, redone |
| Commit boundary | Gantry's: `--single-transaction`, so **exit 0 means committed** | the pipeline's: `JdbcIO` commits per bundle |
| On failure, the target holds | nothing from that partition | **a partial group** — bundles already committed stay |
| Replay is a no-op | yes — merge with `ON CONFLICT DO UPDATE` | yes — explicit upsert `statement=`, *not* Beam's default `INSERT` |
| Refuses to overwrite a newer row | yes, `source_lsn` guard | yes, same guard — **load-bearing here**, see below |
| Reports rows inserted / updated / unchanged | **yes** | **no** — see "What Beam cannot tell you" |
| Startup cost per job | ~0.25 s | ~11 s |
| Checkpoint advances on | the job's own committed row counts | **the group's verification**, never `DONE` |
| Proven on | Docker, local | **Direct runner in a container. Dataflow and Flink: unproven.** |

The chaos suite — worker killed between commit and checkpoint, duplicate
delivery, transient faults, poison tasks — runs against both backends with
**identical assertions**, and passes on both:

| Backend | Chaos suite | Wall clock |
|---|---|---|
| in-memory fake | 13 passed | 0.3 s |
| `sql` | 13 passed | 31 s |
| `beam` | 13 passed | **17 minutes** |

The Beam column's runtime is not an aside — it is the evidence. An earlier run
of that suite reported thirteen green Beam tests in 33 seconds, which was
impossible at eleven seconds a job, and it turned out every one of them had run
a SQL job through a mis-wired fixture. A backend that is silently the wrong
backend has exactly one symptom, and it is being fast.

## Why the checkpoint unit differs

Entirely for cost, and the arithmetic is in `docs/benchmarks.md`.

A SQL job starts in about a quarter of a second, so sixty-one partitions cost
about fifteen seconds of startup in total. Fifteen seconds is not worth trading
partition-granular checkpoints for, so `sql` does not group.

A Beam job costs about eleven seconds before it reads a row. The same sixty-one
partitions cost about eleven minutes, which is worth trading for. So `beam`
groups partitions until startup is roughly a tenth of the job — and at the
measured throughput that is well over a million rows a group.

**This is a real loss, stated rather than hidden.** Under `beam`, a crash
between commit and checkpoint costs the whole group, and a group may be an
entire modest dataset. If that is the wrong trade for your data, `sql` is the
backend with the stronger guarantee, and it is the default for that reason.

## Why the stale-write guard is load-bearing for `beam`

Both backends stamp snapshot rows with the position the snapshot represents, and
both refuse to overwrite a row already newer than it.

Under `sql` that guard is a safety net around a mostly-ordered process. Under
`beam` it is the only thing standing between a slow snapshot and silent data
loss: **a pipeline's bundles are unordered by design and may be retried**, so
"the snapshot wrote it second" carries no information about which value is
current. Without the guard, a partition copied slowly enough undoes changes the
stream already applied — and the row looks consistent afterwards, which is the
part that makes it dangerous.

The guard was missing from the first `beam` implementation and was caught before
it shipped. It is now checked against real databases for both backends.

## What Beam cannot tell you

A SQL job reports what it committed — rows inserted, updated, and unchanged —
because the merge counts them inside the committing transaction. A checkpoint
rests on that attestation.

Beam reports no per-row counts a submitter can read. A pipeline that reaches
`DONE` says only that it finished. So under `beam`:

- a checkpoint may **not** advance on `DONE` alone;
- the group is **verified** first, and the verification is the attestation. The
  executor compares checksums over the group's key range on both sides, inside
  each engine, and only then produces the `CommitResult` that lets a checkpoint
  move. A group that does not verify raises: the task fails, the lease expires,
  the group runs again — safe because the write is an upsert, and correct
  because nothing recorded progress over rows that were never confirmed;
- a `CommitResult` from a `beam` job carries no row counts, and anything reading
  them will see zeros. That is honest, not a bug — but it means volume
  reconciliation on a `beam` Movement comes from verification, never from the
  job.

## Verifying a target with no engine

A PostgreSQL target computes its own checksum: Gantry sends SQL and the engine
answers with one number. An Iceberg table is a set of Parquet files and a
metadata tree, and there is nothing to send SQL to.

Streaming every row into the Gantry process to add them up would verify the data
and break the thing this project is for. So the checksum is computed **by a
job**, in a container, and what comes back is one checksum and one row count —
bounded by construction rather than by the size of the data, exactly as it is
for PostgreSQL.

| | PostgreSQL target | Iceberg target |
|---|---|---|
| Who computes the checksum | the engine | a verification job (`docker/verify`) |
| What crosses into Gantry | one number, one count | one number, one count |
| Implementation | `verification/checksum.py` (SQL) | `verification/portable.py` (Python) |

**Those two implementations must agree exactly, and that is a standing hazard.**
They are checked against each other over deliberately awkward values — numerics
with trailing zeroes, floats, microsecond timestamps with non-UTC offsets,
nulls, byte strings, multi-byte text — against a real PostgreSQL rather than
against anyone's reading of the manual. They disagreed on the first run, on
every `double precision` value, because `round(x::numeric, 10)::text` keeps its
ten decimal places and the Python side had trimmed them. A checksum wrong on one
column type is the failure this pairing exists to catch.

## What an Iceberg target refuses

Iceberg's type system is smaller than PostgreSQL's, and the gaps are real. A
column that cannot be held is refused **in Prepare, with the column named** —
not at row forty million inside a Java writer:

| Refused | Why |
|---|---|
| `json`, `jsonb`, `xml` | no equivalent type; store as text and say so |
| `interval` | no interval type; a duration must become a number of units |
| `money` | locale-dependent even within PostgreSQL |
| `numeric` with no precision | unbounded in PostgreSQL; choosing a bound silently stores something else |
| `numeric(p,s)` with p > 38 | Iceberg decimals stop at 38 digits |
| anything unmapped | refused rather than guessed |

Every problem in a schema is reported at once, because fixing a schema one round
trip per column is a bad way to spend an afternoon.

## Idempotence on an appending target

The PostgreSQL path is idempotent because the write is: an upsert applied twice
is the upsert applied once. **Iceberg's write is an append**, so the same trick
is unavailable — running the move twice adds the rows twice, measured.

Idempotence there is arranged rather than inherited: **verify first, move only
if the target does not already hold the group.** That is sound only because one
job commits one atomic snapshot and a killed job leaves nothing at all, so the
target is never in a partial state for the verification to misread. Both facts
are measured and recorded in `docs/benchmarks.md`, and if either stops being
true this guarantee goes with it.

| | PostgreSQL target | Iceberg target |
|---|---|---|
| Replay is a no-op | yes, by the write itself | yes, by verifying before moving |
| Rests on | nothing external | one atomic snapshot per job; no partial state on kill |

## Known gaps

Stated here rather than discovered later:

- **Deletes do not propagate.** A row removed at the source stays in the target.
  Verification *detects* the disagreement — a group will fail to reconcile —
  but nothing repairs it, so the group will retry indefinitely. Movement is
  insert-and-update only today.
- **An Iceberg destination is configured on the executor, not in the spec.** A
  Movement group lands in Iceberg through `MovementExecutor` and is verified,
  but the warehouse location is passed to the executor rather than read from the
  Movement's `destination` endpoint. Wiring `adapter: iceberg` through the spec
  is a small piece of work that has not been done.
- **Finished job containers are kept** as a record of what ran, and are removed
  only when `DockerRunner.reap()` is called. Nothing calls it on a schedule yet.

## Choosing

| | `sql` | `beam` |
|---|---|---|
| For | ordinary databases, one source to one target | large volumes across heterogeneous sources and sinks |
| Reach | wherever `psql` can connect | wherever Beam has an I/O connector — **Iceberg proven**, on a local filesystem catalog |
| Image | 411 MB (`postgres:16-alpine`) | 4.65 GB (a JRE and pre-staged JARs; see `docker/beam/Dockerfile`) |
| Guarantee | stronger — partition-granular | weaker — group-granular |

`sql` is the default because it is the stronger backend, not because it was
first. If that stops being true, the default moves.
