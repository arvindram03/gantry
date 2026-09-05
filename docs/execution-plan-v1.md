# Gantry v1 — 4-Week Execution Plan

**Status:** Draft 2 — restructured around the four core abstractions
**Source of truth:** `gantry-spec.md` (RFC 0)
**Window:** Mon 2026-09-07 → Fri 2026-10-02 (20 working days)
**Target release:** `v0.1.0` — Dataset · Movement · Analysis · Result

---

## 0. The abstraction model

Four first-class resources. Everything else in v1 is machinery beneath them.

```text
Dataset        addressable logical data unit + manifest
   │            (physical ref, schema, statistics, semantics, access policy)
   │
   ├── Movement    make or keep a Dataset reliably available
   │
   └── Analysis    compute over one or more Datasets
                    │
                    ▼
                 Result    bounded, structured, provenanced output
```

Three consequences that drive the whole plan:

**1. Dataset is a resource, not a field.** In the spec's §11 Movement YAML, datasets appear as a nested block. In v1 they are registered, addressable resources with manifests (`gantry dataset describe orders`), and a Movement *references* them. Inline dataset definitions in a Movement spec are sugar that auto-registers Dataset resources, so §11 specs keep working unchanged.

**2. Movement and Analysis are siblings sharing one lifecycle.** Both are Operations over Datasets. Both run the RFC's guarantee boundary end to end:

```text
Plan → Generate → Validate → Execute → Verify → Result
```

This is the single most important structural decision in the plan. Building that lifecycle *generically* in week 1 is what makes Analysis affordable in week 4 — it inherits planning, validation gates, durable state, retries, verification, provenance, and policy instead of reimplementing them.

**3. Result is the universal output.** A Movement produces a `MovementResult`; an Analysis produces an `AnalysisResult`. Both carry provenance, measurements, and lineage. Verification results are not log lines — they are evidence attached to a Result.

### What this displaces

The previous draft scoped v1 to Movement depth (M0–M5) and deferred Dataset/Analysis/Result. That made Movement the thing the runtime was built around, which is backwards: the runtime is built around the lifecycle, and Movement is one operation type on it.

The cost is real and I am not going to hide it. Four weeks does not buy four abstractions *and* the full migration product. **M4 (adaptive runtime) and M5 (cutover/rollback) leave v1.** That is consistent with the model rather than a concession to it — §5.2 says Migration is a *workflow composed from Movements*, not a fundamental abstraction. Migration becomes v1.1's first workflow, built on the resources v1 delivers.

| In scope for v1 | Out of scope for v1 |
|---|---|
| Dataset registry, manifests, access policy | Migration workflow lifecycle (discover→cutover→rollback) |
| Movement: partitioned snapshot + CDC (M0–M3) | M4 adaptive runtime controller |
| Analysis: compile → validate → execute → verify | M5 cutover gates, approvals, rollback window |
| Result: findings, provenance, lineage, evidence | M6 agent planner (LLM authoring side) |
| Shared verification across both operation types | Second source/target database engine |
| Deterministic policy + agent access policy | ClickHouse / Spark / Flink engine adapters |
| Postgres + DuckDB analysis engines | Continuous/streaming Analysis mode |

**v1 thesis:** a Dataset can be made available by a Movement, analyzed by an Analysis, and the resulting Result traces back through provenance to the exact Movement checkpoint the data came from — with deterministic verification gating both operations.

### Two spec deviations taken deliberately

1. **§9's state machine is Migration-shaped** (`DISCOVERING`, `SNAPSHOTTING`, `CUTTING_OVER`). Underneath it v1 builds a generic Operation state machine matching the lifecycle: `DRAFT → PLANNED → GENERATED → VALIDATED → EXECUTING → VERIFYING → COMPLETED` (+ `FAILED`, `PAUSED`). Movement contributes sub-states (`SNAPSHOTTING`, `CATCHING_UP`); Migration's states layer on as workflow states in v1.1. **Worth folding back into RFC 0.**
2. **Temporal deferred** (Open Question #1). v1 ships a Postgres-backed leased task queue behind a `WorkflowBackend` interface. Decision point at the Day 5 review — switching costs less then than in week 3.

---

## 1. Operating assumptions

- **Team:** 2 engineers. Lanes marked **[A]** (lifecycle/state/scheduler/movement) and **[B]** (adapters/verification/analysis/telemetry).
- **Solo fallback:** 4 weeks solo buys through **Day 15** — four abstractions modeled, Movement complete through CDC, Analysis unbuilt. Cut-lines in §6 apply in order.
- **Working days only.** Weekends are buffer, not capacity.
- Every day ends with `main` green: lint, `mypy --strict`, unit + integration tests.
- Every merged change ships with tests. There is no "tests week."

### Daily rhythm

| Time | Ritual |
|---|---|
| Start | 10 min: yesterday's exit criteria met? If not, cut or extend *today*, not later. |
| End | Push. CI green. Update the risk register if reality moved. |
| Friday | 30 min milestone review against exit criteria. Explicit go/cut decision. |

---

## 2. Week 1 — The four resources and the shared lifecycle

**Goal:** all four abstractions exist as typed, persisted, addressable resources, and both operation types flow through one lifecycle engine — proven in a simulator before any database is attached.

### Day 1 — Mon 09-07 · Repo, toolchain, local stack

- Python 3.12, `uv`, `ruff`, `mypy --strict`, `pytest` + `pytest-asyncio`, `pre-commit`, GitHub Actions.
- Package layout — organized by abstraction, not by migration phase:

  ```text
  gantry/
    core/          Dataset, Result, Provenance, Lineage, Checkpoint, Position
    spec/          Dataset | Movement | Analysis specs, JSON Schema, validation
    registry/      Dataset registry, manifests, statistics
    lifecycle/     Plan → Generate → Validate → Execute → Verify → Result (generic)
    movement/      partitioning, snapshot, CDC apply  (Operation impl)
    analysis/      normalize, join, window, aggregate compiler  (Operation impl)
    results/       Result store, findings, provenance, lineage
    verification/  shared Verifier interface + movement/analysis verifiers
    state/         durable state machine, transitions, audit
    scheduler/     WorkflowBackend, leased queue, workers
    policy/        deterministic policy, agent access policy
    telemetry/     OTel + Prometheus
    adapters/      source/  target/  cdc/  engine/
    api/  cli/
  spec/schema/  spec/examples/  examples/  deploy/docker/  docs/
  ```

- `deploy/docker/docker-compose.yml`: `pg-source` (`wal_level=logical`), `pg-target`, `pg-meta`, Kafka, Kafka Connect + Debezium, Prometheus. DuckDB is in-process.
- `Makefile`: `dev-up`, `dev-down`, `check`, `test`, `seed`.

**Exit:** `make dev-up` brings the full stack healthy on a clean machine. `make check` green. CI runs on PR.

### Day 2 — Mon+1 · `core` vocabulary + Dataset resource

- **Shared vocabulary first** — every later module depends on these types: `Dataset`, `DatasetRef`, `Schema`, `Result`, `Provenance`, `Lineage`, `Checkpoint`, `SourcePosition`, `TimeWindow`.
- Dataset spec per the RFC manifest: `physical` (adapter, reference, estimated rows/bytes), `schema` (time field, keys), `statistics`, `semantics`, `access.agentPolicy`, `sensitiveFields`.
- Dataset registry behind a `DatasetRegistry` Protocol: register, version, resolve by name, list. Manifests are versioned and content-addressed, so registration is idempotent — a Result's provenance pins the version it read, and a replay that re-registers its inputs must not churn versions. **Sequencing correction:** the metadata store does not exist until Day 4, so Day 2 ships the Protocol with two implementations (in-memory for tests, JSON-file for the CLI) and Day 4 adds the Postgres one behind the same interface.
- `gantry dataset ls | describe | register`.

**Exit:** a Dataset registers, versions, and resolves by name. Manifest round-trips. `describe` returns metadata with no data access.

### Day 3 — Mon+2 · Movement and Analysis specs as siblings

- Pydantic v2 models for both, sharing a common `OperationSpec` base (metadata, inputs as `DatasetRef`s, execution constraints, policies, `verify` block).
- `Movement`: all §11 blocks — strategy, key, ordering, partitioning, write mode, CDC, runtime, rate limits. Inline `datasets:` auto-register as Dataset resources.
- `Analysis`: objective, inputs, window, normalize, joins (including `temporal.strategy: nearest_preceding`), signals, execution engine, output kind.
- Shared `verify:` block — `rowExpansion`, `joinCoverage`, `nullRate`, `temporalAlignment` for Analysis; `row_count`, `chunk_checksum`, `primary_key_unique`, `foreign_key_integrity` for Movement. **One schema, one evaluator, different verifiers.**
- JSON Schema generated from models into `spec/schema/` — generated, never hand-edited.
- Defaults normalized and written into the plan: per §8.3, ordering scope defaults to `key` and is never left implicit.

**Exit:** §11's Movement example and the RFC's Analysis example both parse, normalize, and round-trip. 10+ malformed cases produce errors with YAML field paths.

### Day 4 — Mon+3 · Durable state + the generic lifecycle engine

- Metadata schema + Alembic: `datasets`, `dataset_versions`, `operations`, `plan_versions`, `partitions`, `checkpoints`, `state_transitions`, `results`, `findings`, `provenance`, `verification_results`, `artifacts`, `policy_decisions`, `audit_log`.
- Generic Operation state machine (§0 deviation 1). Transitions are the only mutation path; illegal transitions raise; every transition persists actor, reason, plan version, timestamp.
- **Lifecycle engine** with six pluggable stages. Movement and Analysis register stage implementations; the engine owns sequencing, durability, retries, and the guarantee boundary.
- Immutable, content-addressed `PlanVersion` with deterministic node IDs (stable hash) — this is what makes replay safe. §10 immutable fields cannot change without an explicit replan.
- Optimistic concurrency so two workers cannot both advance one unit of work.
- Postgres `DatasetRegistry` implementation behind the Day 2 Protocol, replacing the JSON-file store as the CLI default.

**Exit:** golden-file test — identical input yields a byte-identical plan across runs and machines. Illegal transitions rejected. Audit row exists for every state change.

### Day 5 — Mon+4 · Simulator over both operation types · **M0 review**

- `WorkflowBackend` with an in-memory implementation defining the leasing rules and a Postgres one reproducing them: `SELECT ... FOR UPDATE SKIP LOCKED`, lease expiry with reclaim, at-least-once dispatch, bounded retries, quarantine on exhaustion. Dependency readiness is evaluated in the same statement as the claim — checking first and claiming second leaves a window where a dependency regresses in between.
- Fake source/target/engine adapters with **injectable faults**: worker crash, duplicate delivery, slow target, commit-then-crash-before-checkpoint (§8.7's hardest case), validation failure, verification failure.
- Simulator runs **a fake Movement and a fake Analysis through the same lifecycle**, each producing a Result with populated provenance.

**Exit — M0:** both operation types complete through one engine. Killing a worker mid-operation resumes from checkpoint with no loss and no duplicate effect. A verification failure produces `VERIFICATION_FAILED` with structured evidence, not a crash.

> **If Analysis needs its own bespoke path through the engine, stop and refactor here.** That divergence costs a day now and a week on Day 16.

**M0 outcome — passed.** Both operation types complete through one engine with no bespoke path for Analysis. A worker killed after commit and before checkpoint replays and leaves one effect, not two. A verification failure yields `VERIFICATION_FAILED` with evidence rather than a crash.

**Decision — the leased queue stays; Temporal is not adopted.** It satisfies leasing, expiry, reclaim, bounded retries and quarantine in ~150 lines of Postgres, and concurrent workers provably never receive the same task. Temporal would cost 2–3 days now to buy durable timers and long-running workflow state that nothing needs until the Migration workflow's rollback window in v1.1. The `WorkflowBackend` Protocol keeps the swap bounded. **Re-examine at Day 10 and no later** — after that, CDC and the adaptive controller lean on the scheduler.

---

## 3. Week 2 — Movement depth: reliable bulk copy (M1)

**Goal:** real Postgres, real partitions, real crashes. The simulator's guarantees must survive contact with a database.

### Day 6 — Mon 09-14 · Postgres source adapter → Dataset manifests

- **[B]** `Discover` (§7 Phase 1): schemas, PKs, indexes, FKs, size/row estimates, version compatibility. **Discovery output registers Dataset resources** — this is where the abstraction pays off, rather than producing a Movement-private snapshot.
- **[B]** `Profile` (§7 Phase 2), scoped to what partitioning needs: key cardinality, min/max, null rates, skew, large objects. Sampled — never a full scan. Results land in the Dataset manifest's `statistics`.
- **[A]** Seed generator: `customers` / `orders` schema, scaling to 100M rows via server-side `generate_series`.

**Exit:** discovering a source registers complete, typed Dataset manifests. Profiling a 100M-row table finishes under a minute.

### Day 7 — Mon+1 · Partition planner + snapshot reads

- **[A]** Partition strategies: `range` (PK) and `time_range`. Skew-aware bounds from manifest statistics — equal row counts, not equal key spans.
- **[A]** Partitions are persisted plan artifacts with explicit `[lo, hi)` bounds. Bounds never recompute on replay.
- **[B]** `ReadPartition` with server-side cursors and a stable snapshot; `CurrentPosition` returns the LSN for CDC handoff.

**Exit:** a 100M-row table partitions into balanced, deterministic ranges. Replanning yields identical bounds. Reading a partition twice yields identical rows.

### Day 8 — Mon+2 · Postgres target adapter + idempotent writes

- **[B]** `Prepare`: target DDL from the Dataset manifest, type mapping, compatibility check, deferred secondary indexes (document the tradeoff).
- **[B]** `WriteBatch`: `INSERT ... ON CONFLICT DO UPDATE` on the stable key, plus `COPY`-to-staging + `MERGE` for bulk. **Never Python row loops** (§17 boundary).
- **[B]** Durable `CommitResult` is the only thing permitted to unlock a checkpoint advance.

**Exit:** replaying an identical batch changes zero rows. ≥ 50k rows/sec on the dev stack, recorded as the baseline.

### Day 9 — Mon+3 · Worker loop, checkpoints, replay

- **[A]** Worker: lease → read → write → commit → **verify durable** → advance checkpoint. §8.2's ordering enforced structurally, not by convention.
- **[A]** Per-partition and per-operation checkpoints, written transactionally with respect to the commit they attest to.
- **[A]** Failure classification: retryable / fatal / needs-replan. Poison tasks quarantined, not spun.
- **[A]** `kill -9` chaos test in CI at the commit-then-crash boundary.

**Exit:** kill any worker at any point during a 10M-row copy; on restart it completes with correct counts and zero duplicates. **The single most important test in v1.**

### Day 10 — Mon+4 · CLI, pause/resume, first real Result · **M1 review**

- **[A]** `gantry plan | start | status | pause | resume | abort`. `status` shows per-dataset and per-partition progress, throughput, ETA.
- **[A]** Pause drains in-flight work to a clean checkpoint boundary — it does not kill tasks.
- **[A]** **`MovementResult`**: rows moved, partitions verified, checkpoint state, duration, source manifest versions, lineage to the output Dataset. The first real instance of the Result abstraction.
- **[B]** Scale run: 100M orders end to end. Record wall time, throughput, target load in `docs/benchmarks.md`.

**Exit — M1: passed.** 101,000,000 rows copied in 7m 43s at 218,271 rows/sec, from an empty target, with source and target counts matching exactly and 100,000,000 distinct keys. Durable state, retries, checkpoints, idempotent writes and pause/resume all in place, emitting a persisted `MovementResult` queryable from the metadata store.

**Scheduler decision revised.** Day 5 deferred Temporal and Day 10 adopted it, earlier than that review recommended. Temporal now owns dispatch, retries and timeouts; the leased queue remains behind `--backend queue` until Temporal has proved itself on more runs. Two gaps it forced closed were worth closing regardless: plans are now persisted rather than reconstructed, and they pin the Dataset versions they were compiled against.

**Known limitation.** The two backends do not share progress state — the queue tracks dispatch in `tasks`, Temporal in workflow history — so switching mid-operation re-runs every node. Safe, because effects are idempotent, but wasteful. Progress reporting is backend-neutral (checkpoints against the stored plan); dispatch state is not.

---

## 4. Week 3 — Verification and CDC (M2/M3)

**Goal:** the capabilities that separate Gantry from a copy script. Verification makes correctness provable; CDC makes it live. Verification is built **shared** — Analysis reuses it in week 4.

### Day 11 — Mon 09-21 · Shared verification framework

- **[B]** `Verifier` interface and the §13 hierarchy (Operation → Dataset → Partition → Chunk → Row), written against Operations generally — not Movement specifically.
- **[B]** Movement verifiers: `row_count`, `primary_key_unique`, `null_rate`, `foreign_key_integrity`.
- **[B]** `VerificationResult` persists every §13 field including `evidence` and `plan_version`, and **attaches to the Result** as evidence.
- **[A]** Verification is a lifecycle stage, not post-processing (§22). Partitions reach `VERIFIED` before `COMPLETE`.

**Exit:** a deliberately corrupted row count is caught, attributed to a specific partition, and surfaced as evidence on the `MovementResult`.

### Day 12 — Mon+1 · Chunk checksums + mismatch localization

- **[B]** Order-independent, key-ranged checksums pushed into SQL on both sides. Computed in the database, never in Python (§17).
- **[B]** Type normalization rules for safe comparison (numeric scale, timestamps/timezones, text encoding, NULL sentinel) — documented, since this is where checksum comparisons usually go wrong.
- **[B]** Drill-down per §13: count → aggregate → chunk checksum → key diff → row diff, halving the key range each step.
- **[A]** Repair: mark only the failed partition replayable and re-run it alone.

**Exit:** corrupt one row in 10M; drill-down isolates it in O(log n) checks with no full-table diff. That partition alone is repaired and re-verified.

### Day 13 — Mon+2 · Debezium/Kafka CDC adapter

- **[B]** Connector provisioning in Prepare (publication + replication slot), teardown on finalize.
- **[B]** `CDCAdapter`: `Start(from position)`, `Events`, `Checkpoint`, `Lag`.
- **[B]** **Open Question #2 resolved for v1:** Gantry owns the applied-position checkpoint in its own metadata store; Kafka consumer offsets are a transport detail. Correctness must not depend on connector offset bookkeeping. Documented in `docs/guarantees.md`.
- **[A]** Replication slot lag monitoring + a guard against unbounded WAL growth when apply stalls.

**Exit:** events flow source → Debezium → Gantry with a resumable position surviving consumer restart.

### Day 14 — Mon+3 · CDC apply: idempotency, ordering, staleness

- **[A]** Per-key ordering (§8.3 default) via key-hash routing — one key, one worker.
- **[A]** **Stale-write rejection** by `source_lsn`: conditional update `WHERE target.source_lsn < event.source_lsn`. Out-of-order events are dropped, counted, never silently applied.
- **[A]** Dedup by event ID; deletes and updates share the conditional guard.
- **[B]** Dead-letter queue with replay tooling; DLQ depth is a first-class metric.

**Exit:** a shuffled + duplicated event stream produces the same final target state as the ordered stream. Stale rejects counted and visible.

### Day 15 — Mon+4 · Snapshot↔CDC coordination · **M2/M3 review**

- **[A]** Consistent handoff: capture source LSN *before* the snapshot, snapshot at that point, apply CDC from exactly there. No gap; overlap absorbed by idempotency.
- **[A]** `CATCHING_UP` sub-state with lag tracking and a `wait for lag threshold` plan node.
- **[B]** Live-write test: continuous source writes throughout a full snapshot, then catch up.
- **[B]** Prometheus metrics for the §14 progress/CDC/reliability/correctness families (folded in here; the abstraction work displaced the standalone telemetry day).

**Exit — M2/M3: passed.** A 50,000-row snapshot taken while writes continued throughout, then caught up: 279 changes applied, final lag 1.48 s against a 2 s threshold, source and target checksums identical. Verified by checksum rather than row count, because a count cannot see a row that is present on both sides and stale — which is exactly what a mishandled handoff produces.

**What makes it work.** The snapshot is stamped with the position it represents, and its merge refuses to overwrite anything newer. Without that, a partition copied slowly enough silently undoes changes the stream has already applied, and the row still looks consistent afterwards. Overlap between the two phases is absorbed by idempotency rather than avoided — avoiding it would mean locking the source.

---

## 5. Week 4 — Analysis and Result

**Goal:** prove the second operation type runs on the same lifecycle, and that Results are bounded, verified, and traceable back through Movement provenance.

**v1 Analysis scenario** — the RFC's checkout-latency-regression shape, narrowed to Postgres-resident data so it needs no log store: join `orders` (replicated by the week-2/3 Movement) against `deploy_events` and `postgres_metrics`, with a `nearest_preceding` temporal join, producing findings with measurements. Same spec features, no ClickHouse.

### Day 16 — Mon 09-28 · Analysis compiler: Plan → Generate

- **[B]** Compile the Analysis spec to engine SQL: field normalization (aliases → canonical), joins including `temporal.strategy: nearest_preceding` with `maxDistance`, windowing, signal aggregation.
- **[B]** **Generated artifacts are versioned and retained as provenance** (RFC "Generate"). The generated SQL is an inspectable, stored object — not a transient string.
- **[A]** Engine selection: `engine: auto` resolves from the Datasets' physical adapters. Postgres for source-resident data, DuckDB for local artifacts.
- **Scope fence:** v1 supports exactly the operations in the RFC's Analysis spec. Anything else is a v1.1 issue. This compiler will try to become a query language; do not let it.

**Exit:** the scenario spec compiles to inspectable, versioned SQL for both engines. Compilation is deterministic — identical spec yields identical artifact hash.

### Day 17 — Mon+1 · Validate + Execute

- **[B]** **Validate stage** (RFC): syntax/compile check, schema and type resolution, Dataset existence, `EXPLAIN` plan inspection, cost/bytes-scanned estimate against policy, bounded sample execution. Returns an accepted artifact **or a structured failure suitable for agentic repair** — never a stack trace.
- **[A]** Execute stage: dispatch to the engine adapter, own submission, execution state, retries, cancellation, resource limits, progress, intermediate artifacts, failure classification.
- **[A]** Engine adapters: Postgres + DuckDB. Two engines is the minimum that proves dispatch isn't Postgres-shaped.
- **[A]** Provenance capture during execution: engine, artifact version, Dataset manifest versions, time ranges, **and the Movement checkpoint state the input Datasets were at**.

**Exit:** the scenario executes on both engines and produces identical aggregates. A spec referencing a missing column fails in Validate with a structured, repairable error — and never reaches Execute.

### Day 18 — Mon+2 · Verify + AnalysisResult + Result store

- **[B]** Analysis verifiers on the Day 11 framework: `rowExpansion`, `joinCoverage`, `nullRate`, `temporalAlignment`.
- **[B]** **The engine-success/Gantry-failure case** (RFC): a technically successful join expanding 84M rows to 1.7B is rejected against a declared `rowExpansion.max: 1.1`. Engine says `SUCCESS`; Gantry says `VERIFICATION_FAILED`. This single behavior is the clearest proof of the guarantee boundary — build the demo around it.
- **[B]** `AnalysisResult`: findings with `claim`, `measurements`, `references`, and `strength`. **`strength` is derived from deterministic/statistical evidence and explicitly labeled when it is not** — it must never silently mean model confidence.
- **[A]** Result store + `gantry results get | explain | provenance | refresh`. Provenance resolves the full chain: finding → computation → artifacts → manifest versions → source partitions → Movement checkpoint.

**Exit:** the scenario produces an `AnalysisResult` whose finding traces, in one command, back to the Movement checkpoint its input data came from. A verification-violating Analysis is rejected despite engine success.

**Outcome (met).** Both halves demonstrated on the reference scenario, on the same SQL:

| | engine | Gantry | findings |
|---|---|---|---|
| with the temporal qualifier | `SUCCESS`, 2 rows | `PASSED` — `rowExpansion 1.0000x` | 4 published |
| without it | `SUCCESS`, 2 rows | `FAILED` — `rowExpansion 2.0000x`, "the join expanded 40,000 rows to 80,000" | withheld |

Findings are **withheld** on a failed check rather than published with a caveat; the Result is still written, carrying `verification_failed` and the evidence for the refusal. Every finding carries a required `strength_basis`, and a `deterministic` or `statistical` one must carry the measurements its strength came from — only `model_judgement` may stand alone, and it is labelled wherever rendered.

`gantry results provenance` resolves the whole chain in one call — finding → Result → artifact (by hash) → pinned Dataset versions → the Movement that produced them → its three LSN checkpoints — and **names the links it could not resolve** rather than omitting them; a pin whose content hash no longer matches is treated as dangling, not silently upgraded to the current version. `gantry results refresh` re-executes the stored artifact and reports measurement drift, refusing outright when the artifact was not retained.

Two things fixed here rather than papered over: the Result store rehydrated every non-Movement kind as the base `Result`, which rejects an `AnalysisResult`'s fields outright — so exactly the Results with the most to say were unreadable; and the CLI's `_pending` placeholder was retired, since `results get` was the last command standing in for an unbuilt one.

### Day 19 — Mon+3 · Agent access policy + full demo rehearsal

- **[A]** Progressive access ladder (RFC): `describe → profile → aggregate/query → partition → sample → exact records`, each gated. Default policy: rows deny, aggregates allow, metadata allow; PII redaction; `samples.maxRows`; `queries.maxBytesScanned` and timeout. **An LLM cannot override these** — enforcement is in the deterministic path, not the prompt.
- **[B]** Typed Python API: `gantry.datasets.{describe,profile,query,sample}`, `gantry.analysis.{plan,execute,status}`, `gantry.results.{get,explain,provenance,refresh}`.
- Full rehearsal, timed, no manual intervention:
  1. Seed 100M orders; start continuous writes
  2. Register Datasets from discovery
  3. Movement: snapshot + CDC → kill a worker (replay) → duplicate events (idempotency) → corrupt a chunk (checksum failure) → repair **only** that partition
  4. Catch CDC up to < 2s → `MovementResult` verified
  5. Analysis over the replicated Dataset → validation catches a bad spec → repair
  6. Analysis with a bad join → `VERIFICATION_FAILED` on row expansion despite engine success
  7. Repair → `AnalysisResult` with findings
  8. `gantry results provenance` traces a finding back to the Movement checkpoint
  9. Attempt raw-row access as an agent → policy denies; aggregate succeeds
- **Reserve the whole day for fallout. The rehearsal will find real bugs.**

**Exit:** two consecutive clean end-to-end runs from a fresh `make dev-up`. Recorded as an asciinema cast.

**Outcome (met, with two deviations stated below).** `make rehearse` drives the whole sequence through the *public* surfaces — the CLI and the typed API — rather than through the runtime's internals, because a rehearsal built out of internals proves what the test suite already proves. Two consecutive clean runs:

| step | seconds | proved |
|---|---|---|
| seed and register | 8.8 | 200,000 orders and 1,000,000 customers; 7 datasets registered from discovery |
| faults | 32.9 | 16 chaos tests, including a real `kill -9` |
| movement | 9.9 | plan, run, verify |
| corruption and repair | 9.2 | one row corrupted, located in `public.orders/00000`, repaired without a full re-copy |
| analysis | 0.6 | well-formed join published findings; expanding join rejected after the engine reported SUCCESS |
| provenance | 0.5 | artifact, Dataset versions and 11 checkpoints from `orders-snapshot.movement`, no gaps |
| agent access | 1.4 | sample denied by policy default; aggregate over the same table returned |
| **total** | **63.3** | |

**Deviations.** No asciinema cast — the recording adds nothing a text transcript does not, and the script's own timing table is the artifact worth keeping. And the rehearsal ran at 200,000 orders rather than 100,000,000: the 100M figures stand from the M1 measurement on Day 10 (101M rows in 462.7s) and are not re-derived here. Every guarantee in the list is scale-independent; the throughput numbers are the ones that are not, and those are Day 10's.

**The access ladder.** `describe → profile → query → partition → sample → records`, with the RFC's defaults shipped: rows deny, aggregates allow, metadata allow, PII redacted, samples capped and requiring a reason, evidence persisted. Enforcement is a function every content-returning path must call, not a prompt — and a request has no field that could carry an override. A Dataset can tighten the global policy and never loosen it.

The API takes a **structured aggregate rather than SQL**, because `rows: deny, aggregates: allow` is only enforceable if "is this an aggregate" is decidable, and over arbitrary SQL it is not. That also makes field checking the whole injection defence: an identifier not in the manifest never reaches SQL.

**What the rehearsal found, which is what it was for:**

1. **Replanning after the data changed failed outright.** Partition bounds come from the data, so recompiling produced different content under identical guarantees — and `next_version` reused the version while the plan store correctly refused to overwrite it. Bounds are content, not a guarantee: a content change now allocates the next version, and an immutable-guarantee change still demands an explicit replan.
2. **Masking matched source field names against aliased output columns**, so a `REDACT` decision on `min(email)` masked nothing while still reporting itself as redacted. A mask that is not applied is worse than one never promised.
3. **The demo registered Datasets in an in-memory registry**, so a stored Result pinned versions that died with the process. Provenance reported the gaps rather than hiding them — the behaviour built on Day 18 catching a bug written on Day 19.
4. **The chaos suite depended on ambient source scale**, failing in a way that looked like a crash-replay bug when it was a short table. `gantry seed` now takes `--customers` separately from `--rows`.
5. **The compiled Analysis SQL had no `ORDER BY`.** PostgreSQL and DuckDB returned the same groups in different orders — and `derive_findings` takes the first group as the baseline and the last as the current, so the *direction* of every finding depended on which engine ran it. The compiler now orders by the grouping keys.
6. **Starting a Movement against an Operation already executing half-runs instead of refusing.** Left as a known gap for v1.1 and written down in `docs/guarantees.md` rather than quietly worked around.

### Day 20 — Mon+4 · Docs and release

- `docs/architecture.md` (the four resources and the shared lifecycle), `docs/guarantees.md` (what v1 guarantees and — equally important — what it does not), `docs/adapters.md`, `docs/benchmarks.md`.
- README: positioning, 5-minute quickstart, demo cast.
- `examples/postgres-to-postgres/` and `examples/analysis/` runnable from a clean clone.
- Apache-2.0 headers, `CONTRIBUTING.md`, RFC 0 as `docs/rfcs/0000-gantry.md` with the §0 deviations folded in, open questions filed as issues.
- Tag `v0.1.0`.

**Exit — v1: met.** Verified by wiping the source tables, the target tables and the entire metadata schema, then following the README quickstart verbatim:

```text
$ gantry seed --rows 1000000
seeded 1,000,000 orders
seeded 40,000 request logs and 3 deploys

$ gantry start examples/postgres-to-postgres/movement.yaml
ok orders-snapshot.movement  1,010,000 rows in 8.1s (124,618 rows/sec)
  partitions 2/2   checkpoints 8   inputs pinned 6
  verification   8/8 checks passed

$ make demo
well-formed  ENGINE: SUCCESS   GANTRY: PASSED  row_expansion 1.0000x  → 4 findings
expanding    ENGINE: SUCCESS   GANTRY: FAILED  row_expansion 2.0000x  → withheld

$ gantry results provenance checkout-regression.analysis
checkout-regression.analysis  1 artifacts, 2 dataset versions, 8 checkpoints
  produced by  orders-snapshot.movement
```

**What that surfaced, and it is the reason the criterion was worth stating that way:** the Analysis half of the demo was *not* reproducible. `public.request_logs` and `public.deploy_events` existed only on the machine where they had been created by hand on Day 16. The specs referenced tables a clean clone did not have, and the four integration test files over them **skipped rather than failed** — the quietest way for a feature to stop being covered. `gantry seed --scenario checkout` now generates the scenario deterministically, and those tests seed their own fixture instead of hoping for one.

Three more bugs came out of writing that seeder and re-running from clean:

1. **A bind parameter inside an SQL comment.** SQLAlchemy scans comments for `:name`, so a comment explaining a casting rule became a required parameter.
2. **`(g - 1) * :spacing` truncated to zero.** PostgreSQL inferred the bind's type from the integer it was multiplied by, so the fractional row spacing became `0` and every request landed at the same instant — the regression the demo exists to show simply was not there. Casting the bind fixed it.
3. **The cross-engine percentile tolerance was relative.** The guarantee is agreement to the input column's scale, which is absolute; a relative bound calibrated on one dataset's magnitude silently changed meaning when the data did. The same 0.01 divergence read as `1e-6` at a latency of 532 and `1.4e-4` at 70.

And one improvement to provenance itself: every plan node checkpointed as `partition`, so a dataset-level verification and a single partition copy were indistinguishable in the trail, rendered as `partition/<node hash>`. Checkpoint scope is now derived from the node kind. The scope **id** stays the node id — making it readable would let two dataset-level nodes over one Dataset share a checkpoint, and the later would overwrite the earlier, losing the per-node progress resume depends on. The readable name lives in the position, which is what the CLI now renders.

**Shipped:** `docs/architecture.md`, `docs/adapters.md`, `CONTRIBUTING.md`, RFC 0 with seven deviations written down rather than left to be discovered, runnable walkthroughs under `examples/`, SPDX headers across the package, `NOTICE`, and `v0.1.0`.

---

## 6. Cut-lines

Cut **in this order**. Each buys roughly a day and preserves the four-abstraction story.

| # | Cut | Why it is safe |
|---|---|---|
| 1 | `time_range` partitioning | PK-range demonstrates the abstraction |
| 2 | DuckDB engine adapter | Costs the "dispatch isn't Postgres-shaped" claim — say so in the docs rather than implying it |
| 3 | Profile depth (Day 6) → min/max/count | Partitioning needs little else in v1 |
| 4 | `results.refresh` | `get`/`explain`/`provenance` carry the Result story |
| 5 | Row-level diff (Day 12) → stop at key diff | Localization to a key set is already the hard part |
| 6 | Reduce demo scale 100M → 10M | Preserves the story; only the headline number shrinks |
| 7 | Progressive access ladder → metadata/aggregate/deny only | Sampling and redaction move to v1.1 |

**Never cut:** crash-replay correctness (Day 9), stale-write rejection (Day 14), snapshot↔CDC handoff (Day 15), the engine-success/verification-failure case (Day 18), provenance chaining a finding back to a Movement checkpoint (Day 18). Those five *are* the product.

---

## 7. Risk register

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| **Analysis needs its own path through the lifecycle** | **Critical** | **Medium** | Day 5 exit criterion runs a fake Analysis through the engine. If it diverges, refactor on Day 5 — it costs a day then and a week on Day 16 |
| Analysis compiler grows into a query language | High | **High** | Hard scope fence on Day 16: exactly the RFC spec's operations, nothing more |
| Debezium/Connect setup burns days | High | **High** | Stack up on Day 1, not Day 13. If unhealthy by Day 3, fall back to `pgoutput` logical replication read directly |
| Snapshot↔CDC handoff has a correctness gap | Critical | Medium | Design it on Day 5 in the simulator; Day 15 validates rather than discovers |
| Checksum type-normalization rabbit hole | Medium | **High** | Timebox to Day 12; ship a documented subset, list unsupported types explicitly |
| Provenance chain incomplete across operation types | High | Medium | Provenance is a Day 2 `core` type, populated by every stage from Day 4 — not bolted on in week 4 |
| Leased queue proves insufficient | High | Low | `WorkflowBackend` exists from Day 5; decide at the Day 5 review, never later than Day 10 |
| Python becomes the data path | High | Medium | Review rule: any per-row Python loop over unbounded data is a blocking comment (§17) |
| Week 4 compresses under week 2–3 slip | High | **High** | Week 3 already absorbed telemetry. Next slip cuts M1 scale targets, **not** Analysis — the abstraction matters more than the row count |

---

## 8. Definition of done for v1

- [ ] All four resources are addressable, versioned, and persisted
- [ ] Movement and Analysis run the same lifecycle engine — verified by a test asserting a shared stage sequence
- [ ] Every Result carries provenance resolving to Dataset manifest versions and, for Movement-derived data, checkpoint state
- [ ] A verification failure blocks a Result regardless of engine success
- [ ] `mypy --strict` clean; no `Any` in public interfaces (§17)
- [ ] Every adapter interface has a fake used in tests
- [ ] Chaos tests (worker kill, duplicate delivery, target stall) run in CI
- [ ] Every state transition and policy decision is in the audit log
- [ ] No raw row data reaches an agent surface under default policy (§16, RFC agent access)
- [ ] Secrets are references, never literals in specs (§16)
- [ ] `docs/guarantees.md` states degraded guarantees honestly
- [ ] Demo runs clean twice consecutively from a fresh environment

---

## 9. What comes after v1

**v1.1 — Migration workflow.** M4 adaptive runtime and M5 cutover/gates/approvals/rollback, built as the first *workflow* over the v1 resources. Cutover gates are cheap once verification and policy exist: gate evaluation is a function over persisted `VerificationResult`s, approval is a CLI command plus an audit row. Roughly a week.

**v1.2 — M6 agent planner.** Spec generation from Dataset manifests, partition suggestions, lag diagnosis, and the repair loop — landing on the proposal API and policy engine. Agents propose; the runtime disposes.

**v2 — Signal breadth.** ClickHouse/Spark engine adapters, continuous Analysis mode, and the RFC's full logs/traces/deploys/metrics scenario.

The order matters: the reliability layer must be trustworthy before anything is allowed to propose changes to it.
