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
- the group is **verified** first, and the verification is the attestation;
- a `CommitResult` from a `beam` job carries no row counts, and anything reading
  them will see zeros. That is honest, not a bug — but it means volume
  reconciliation on a `beam` Movement comes from verification, never from the
  job.

## Choosing

| | `sql` | `beam` |
|---|---|---|
| For | ordinary databases, one source to one target | large volumes across heterogeneous sources and sinks |
| Reach | wherever `psql` can connect | wherever Beam has an I/O connector |
| Image | 411 MB (`postgres:16-alpine`) | 4.65 GB (a JRE and pre-staged JARs; see `docker/beam/Dockerfile`) |
| Guarantee | stronger — partition-granular | weaker — group-granular |

`sql` is the default because it is the stronger backend, not because it was
first. If that stops being true, the default moves.
