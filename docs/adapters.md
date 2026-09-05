# Adapters

Four adapter kinds, each a `Protocol` with a small surface. The surfaces are
small on purpose: an adapter that can do everything ends up owning decisions
the runtime has to be able to make identically across every backend.

| Kind | Owns | Ships in v1 |
|---|---|---|
| Source | discovery, profiling, reading a partition, reporting its position | PostgreSQL |
| Target | preparing a destination, writing a batch idempotently | PostgreSQL |
| CDC | a change stream with positions and lag | Debezium over Kafka |
| Engine | planning and running a compiled artifact | PostgreSQL, DuckDB |

## Source

```python
class SourceAdapter(Protocol):
    async def discover(self, *, schemas) -> Sequence[DatasetManifest]: ...
    async def profile(self, manifest) -> DatasetManifest: ...
    def read_partition(self, manifest, partition, *, batch_size) -> AsyncIterator[...]: ...
    async def current_position(self) -> SourcePosition: ...
```

**Discovery and profiling are separate.** Discovery reads the catalog: names,
columns, types, keys. Profiling reads the planner's statistics: row counts,
null rates, histogram boundaries. Profiling never scans the table — a profile
that has to read a hundred million rows to describe them is not a profile, it
is a migration.

**A partition read is ordered by its key.** That is what makes a checksum over
a partition reproducible, and it is not an optimisation the adapter may skip.

**Positions are opaque to the runtime.** Gantry stores and compares them; only
the adapter that produced one interprets it. The PostgreSQL adapter reports
LSNs numerically rather than as `7/9B77D6D0`, because that is the form Debezium
puts in each change event and two representations of one position that cannot
be compared are worse than either alone.

**What discovery cannot know**, the Dataset spec declares: which column carries
time, which fields are sensitive, what an agent may reach. Rediscovery
preserves those declarations — without that, registering a spec and then
rediscovering would alternate between two manifests for one table forever.

## Target

```python
class TargetAdapter(Protocol):
    async def prepare(self, manifest, *, target) -> None: ...
    async def write_batch(self, ...) -> WriteOutcome: ...
```

**Writes must be idempotent, and this is the interface's central demand.**
Every scheduler Gantry supports delivers at-least-once, so a batch will
sometimes arrive twice. The PostgreSQL target upserts on the key and rejects
stale versions by comparing the source position already stored — a replayed
older change does not overwrite a newer one.

The commit ordering is fixed and is not the adapter's choice: **write, commit,
then checkpoint**. Checkpointing first would lose data on a crash between the
two; there is no ordering that avoids duplicates instead, which is why
idempotency is required rather than preferred.

Tombstones are written in the same transaction as the delete they describe.
They lived in the metadata store once, which made a delete and its record two
transactions that could disagree.

## CDC

```python
class CDCAdapter(Protocol):
    async def start(self, *, resume_from: StreamPosition | None) -> None: ...
    def events(self) -> AsyncIterator[ChangeEvent]: ...
    async def source_position(self) -> SourcePosition: ...
    async def lag(self) -> timedelta: ...
    async def stop(self) -> None: ...
```

**Gantry owns the applied position, not Kafka.** Consumer offsets are a
transport detail: an offset says a message was delivered, not that its effect
was committed. The checkpoint in Gantry's metadata store says what was applied,
and resume reads from there.

The Debezium adapter provisions and tears down its own connector through the
Kafka Connect REST API, and manages the replication slot behind it. A slot left
behind is not an untidy resource — it pins WAL and eventually fills the source's
disk.

Snapshot and stream meet through an **LSN-stamped handoff**: the snapshot
records the position it was taken at, and changes at or before that position
are already contained in it. Without the stamp the only safe options are
replaying everything or losing the window between.

## Engine

```python
class EngineAdapter(Protocol):
    @property
    def engine(self) -> str: ...
    async def explain(self, artifact) -> ExplainResult: ...
    async def sample(self, artifact, *, limit) -> QueryResult: ...
    async def execute(self, artifact) -> QueryResult: ...
```

Engines own scans, joins, sorting, aggregation and shuffle. Gantry owns
submission, validation, resource limits, failure classification and provenance.

**Two implementations is the minimum that keeps the boundary honest.** With
one, "engine adapter" means whatever PostgreSQL happens to do. DuckDB reads the
same PostgreSQL tables in place through the `postgres` extension rather than
being handed a copy — a copy would make the two agree for the wrong reason.

The real differences between them are recorded in
[guarantees.md](guarantees.md) rather than smoothed over: interpolating
aggregates agree only to the input column's scale, the two disagree about
Python types in both directions, and DuckDB reports no row estimate rather than
parsing a number out of its plan text.

## Writing one

Implement the `Protocol` — there is no base class to inherit and no
registration step. What a new adapter must not do:

- **Interpret a position it did not produce.** Comparison is the runtime's job.
- **Checkpoint on its own.** The runtime decides when a checkpoint is earned.
- **Report success it has not confirmed durable.** Everything above this
  interface treats a returned outcome as a fact.
- **Skip the ordering guarantee for speed.** An unordered partition read makes
  every checksum over it meaningless.

`gantry/adapters/fake.py` holds in-memory implementations used by the unit
tests, and they are the shortest readable statement of what each interface
expects.
