# Project Spec: Gantry

**Working name:** `gantry`  
**Status:** Draft / RFC 0  
**License:** Apache-2.0  
**Primary language:** Python for v1; language-neutral runtime interfaces  
**Audience:** Platform engineers, data engineers, infrastructure teams, enterprise modernization teams

## 1. Summary

Gantry is an open-source reliability and execution layer for reliable data movement across enterprise systems.

It does **not** replace CDC engines, Kafka, Spark, Beam, Airbyte, dbt, or database-native replication tools. Instead, it provides a common abstraction above them for planning, executing, observing, verifying, recovering, and cutting over migrations.

The core design principle is:

> Agents may plan and re-plan. Deterministic infrastructure enforces guarantees.

A migration is represented as a declarative spec. The runtime turns that spec into an execution graph, invokes adapters for existing enterprise tools, persists durable progress, verifies correctness continuously, and exposes a policy-controlled surface for humans or AI agents to modify execution safely.

The project aims to make reliable data movement a reusable systems primitive rather than a collection of one-off scripts and runbooks. Database migration is the first end-to-end workflow built on this primitive.

### Positioning

**Gantry is an open-source reliability layer for data movement.**

Gantry sits above existing transport and processing infrastructure. It does not need to own the bytes on the wire. Instead, it owns the execution contract around movement: checkpoints, replay, ordering boundaries, idempotency requirements, verification, policies, observability, and recovery.

The distinction is intentional:

- **Movement** is the primitive.
- **Migration** is a workflow composed from movements.
- **Connectors/processors** are adapters used to execute movements.
- **Agents** are optional planners operating outside the trusted correctness boundary.

---

## 2. Problem

Enterprise data movement workloads—migrations, replication, backfills, synchronization, and reprocessing—repeatedly rebuild the same reliability machinery:

- snapshot + CDC coordination
- chunking and partitioning
- dependency ordering
- idempotent writes
- durable checkpoints
- retries and replay
- throttling and backpressure
- schema and data validation
- reconciliation
- cutover gates
- rollback procedures
- observability
- human approval workflows

Existing tools solve individual parts of this problem well, but the end-to-end guarantees usually live in custom scripts, consulting playbooks, or operator knowledge.

This creates several problems:

1. Migration correctness depends on bespoke implementation.
2. Operators have no common execution model across technologies.
3. Recovery and rollback are frequently manual.
4. Verification happens too late.
5. AI agents can generate migration code, but there is no safe deterministic runtime constraining their actions.
6. Knowledge from one migration is rarely encoded into a reusable platform.

Gantry provides that missing control and guarantee layer.

---

## 3. Goals

The project should:

- Provide a declarative data movement specification.
- Model data movement as a durable execution graph.
- Make every unit of work restartable and replay-safe.
- Support batch, snapshot, CDC/streaming, and snapshot-then-stream movement strategies.
- Provide reusable abstractions for checkpointing, ordering, idempotency, retries, verification, and rollback.
- Integrate with existing migration/data infrastructure through adapters.
- Continuously verify source and target correctness.
- Support runtime throttling and concurrency changes without restarting a migration.
- Expose structured telemetry for operators and agents.
- Allow AI planners to propose execution changes while a deterministic policy engine validates them.
- Support human approval gates for high-risk actions such as cutover or destructive schema operations.
- Be deployable inside a customer's VPC or private infrastructure.
- Keep customer data out of the control plane whenever possible.

---

## 4. Non-Goals

Version 1 will not:

- Build a new CDC engine.
- Build a new distributed stream processor.
- Replace Kafka.
- Replace Airbyte, Debezium, Spark, Beam, Flink, dbt, or database-native migration tooling.
- Automatically resolve arbitrary semantic schema differences.
- Promise universal exactly-once semantics across every source and target.
- Allow an LLM to bypass runtime safety policies.
- Implement active-active multi-master replication as a primary use case.
- Become a general-purpose workflow engine.
- Compete primarily on connector breadth or transformation authoring.

---

## 5. Core Concepts

### 5.1 Movement

The fundamental Gantry resource. A Movement describes reliable transfer of logical state from a source to a destination under explicit execution and correctness guarantees.

A Movement contains:

- source
- destination
- datasets
- movement strategy
- dependency graph
- execution policies
- correctness guarantees
- checkpoint policy
- verification rules
- recovery policy

A Movement does not imply a cutover or decommissioning event. It may run once, periodically, or continuously.

Examples include:

- database backfill
- continuous replication
- CDC synchronization
- reprocessing/replay
- warehouse materialization
- database migration data plane

### 5.2 Migration Workflow

Migration is the first higher-level workflow built from one or more Movements. It adds migration-specific lifecycle semantics such as discovery, target preparation, catch-up, reconciliation, cutover gates, rollback windows, and source decommissioning.

```text
Migration
  Discover
     ↓
  Prepare Target
     ↓
  Movement (snapshot + stream)
     ↓
  Reconcile
     ↓
  Cutover
     ↓
  Rollback Window
```

### 5.3 Dataset

A migratable logical unit.

Examples:

- database table
- collection
- topic
- object-store prefix
- logical entity composed from multiple source tables

### 5.4 Partition

The smallest independently executable and checkpointed unit of bulk work.

Examples:

- primary-key range
- time range
- hash bucket
- source-native partition
- file/object group

### 5.5 Change Stream

An ordered stream of post-snapshot changes.

Examples:

- PostgreSQL WAL
- MySQL binlog
- Oracle redo log
- Kafka topic
- cloud-native CDC feed

### 5.6 Checkpoint

Durable evidence of successfully committed progress.

Examples:

- source LSN
- Kafka offset
- source timestamp
- primary-key high-water mark
- completed partition ID

A checkpoint MUST only advance after the associated side effects are durably committed.

### 5.7 Verification

A deterministic check comparing expected and observed state.

Examples:

- row count
- chunk checksum
- key-set comparison
- null-rate comparison
- uniqueness
- referential integrity
- aggregate business query
- schema equivalence
- CDC lag
- application smoke test

### 5.8 Execution Plan

A deterministic DAG generated from a Movement spec or a higher-level workflow such as Migration.

The plan contains nodes such as:

- discover
- profile
- create schema
- snapshot partition
- start CDC
- apply CDC
- verify chunk
- reconcile dataset
- wait for lag threshold
- approval gate
- cutover
- post-cutover verification
- finalize

### 5.9 Policy

A machine-enforced constraint on execution.

Examples:

- target CPU must remain below 70%
- max 32 concurrent writers
- checksum verification required before cutover
- no destructive DDL without approval
- CDC lag must be < 2 seconds before cutover
- rollback window must remain open for 24 hours

---

## 6. Architecture

```text
                       ┌──────────────────────┐
                       │   Human / AI Planner │
                       └──────────┬───────────┘
                                  │
                           Movement Spec
                                  │
                                  v
                       ┌──────────────────────┐
                       │     Control Plane    │
                       │                      │
                       │ Planner              │
                       │ Policy Engine        │
                       │ State Machine        │
                       │ Scheduler            │
                       │ Verification Manager │
                       └──────────┬───────────┘
                                  │
                         Execution Commands
                                  │
          ┌───────────────────────┼────────────────────────┐
          v                       v                        v
 ┌────────────────┐     ┌──────────────────┐     ┌────────────────┐
 │ Source Adapter │     │ Transform Adapter│     │ Target Adapter │
 │                │     │                  │     │                │
 │ Debezium       │     │ Beam / Spark     │     │ Postgres       │
 │ DMS            │     │ dbt              │     │ Snowflake      │
 │ Airbyte        │     │ Flink            │     │ BigQuery       │
 │ Native DB APIs │     │ Custom jobs      │     │ Object store   │
 └────────────────┘     └──────────────────┘     └────────────────┘
          │                       │                        │
          └───────────────────────┼────────────────────────┘
                                  v
                         Customer Data Plane

                                  │
                                  v
                      ┌─────────────────────┐
                      │ Telemetry / Events  │
                      │ metrics logs traces │
                      └─────────┬───────────┘
                                │
                                v
                     Re-plan / throttle / alert
```

The preferred enterprise deployment model is:

- Control plane stores metadata and execution state.
- Data plane executes inside customer infrastructure.
- Raw customer data does not need to leave the customer environment.
- Agents receive metadata and approved telemetry, not unrestricted database contents.

---

## 7. First Workflow: Database Migration Lifecycle

### Phase 1: Discover

Collect:

- schemas
- primary keys
- indexes
- foreign keys
- table sizes
- row counts
- estimated change rate
- partition candidates
- database features
- source/target version compatibility

Output: `DiscoverySnapshot`.

### Phase 2: Profile

Measure:

- key cardinality
- null rates
- duplicate rates
- value distributions
- skew
- large objects
- invalid encodings
- referential integrity failures
- write/change rates
- candidate hot keys

Output: `ProfileReport`.

### Phase 3: Plan

Build:

- dependency DAG
- snapshot strategy
- partition plan
- CDC strategy
- ordering boundaries
- target write strategy
- verification plan
- resource limits
- cutover gates
- rollback policy

Output: immutable `PlanVersion`.

### Phase 4: Prepare

Perform:

- target schema creation
- compatibility checks
- CDC setup
- target indexes required for migration
- secrets/connectivity validation
- dry-run verification queries

### Phase 5: Snapshot

Bulk copy historical data by partitions.

Each partition follows:

```text
PENDING
  -> RUNNING
  -> WRITING
  -> COMMITTED
  -> VERIFIED
  -> COMPLETE
```

Failures return to a replayable state.

### Phase 6: CDC Catch-up

Apply source changes occurring during snapshot.

Track:

- source position
- last applied position
- lag
- stale/out-of-order updates
- retries
- dead-letter events

### Phase 7: Reconciliation

Run layered verification.

Fast checks first:

1. counts
2. aggregate checks
3. chunk checksums
4. constraint checks

Only drill to row-level diff on mismatched chunks.

### Phase 8: Cutover

Cutover is permitted only if all mandatory gates pass.

Example gates:

- all snapshot partitions verified
- no critical verification failures
- CDC lag < 2 seconds
- target health within policy
- source and target schema compatible
- operator approval received

### Phase 9: Rollback Window

Keep source available as the rollback authority for a configured period.

Prefer traffic rollback over reverse bulk migration.

Optional reverse CDC may be configured where supported.

### Phase 10: Finalize

After rollback window:

- run final reconciliation
- archive migration metadata
- disable CDC
- mark source decommissionable
- generate audit report

---

## 8. Reliability Guarantees

### 8.1 Idempotency

Every executable operation SHOULD be replay-safe.

Preferred patterns:

- upsert by stable key
- conditional update by version/LSN
- deterministic object names
- deduplication by event ID
- compare-and-set
- transactional staging + merge

The runtime assumes retries will happen.

### 8.2 Checkpointing

Checkpoint ownership belongs to the component applying the side effect.

Rule:

```text
read
-> process
-> write
-> commit
-> verify durable success
-> advance checkpoint
```

Never checkpoint before durable commit.

### 8.3 Ordering

Ordering MUST be explicitly scoped.

Supported boundaries:

- global
- partition
- entity key
- transaction
- none

Default should be per-entity ordering.

Targets SHOULD reject stale writes using:

- source version
- LSN
- sequence number
- commit timestamp

### 8.4 Transactions

Where source transactions span multiple rows, adapters SHOULD preserve transaction boundaries when possible.

If atomic replication is impossible, the plan must state the degraded guarantee.

Cross-system global transactions are out of scope. Prefer:

- idempotent steps
- durable events
- compensating actions
- reconciliation

### 8.5 Replay

A migration MUST support replay from:

- migration checkpoint
- dataset checkpoint
- partition checkpoint
- change-stream offset

Replays MUST retain the same `PlanVersion` unless explicitly replanned.

### 8.6 Verification

A migration is not successful because all jobs completed.

It is successful only when required verification gates pass.

### 8.7 Recoverability

The runtime MUST survive:

- worker loss
- orchestrator restart
- transient source failure
- transient target failure
- network partition
- throttling
- partial batch commit
- duplicate delivery
- process crash after target commit but before checkpoint

---

## 9. Runtime State Machine

### 9.1 Operation lifecycle states

Movement and Analysis are both Operations, and both traverse one generic lifecycle:

```text
DRAFT
PLANNED
GENERATED
VALIDATED
EXECUTING
VERIFYING
COMPLETED

FAILED
PAUSED
```

This is the state machine the runtime enforces. Operation types contribute sub-states within `EXECUTING` — a Movement may report `SNAPSHOTTING` or `CATCHING_UP` — but they do not define their own top-level lifecycle.

Guarantees attach to the lifecycle rather than to an operation type. `VALIDATED` is reached only through deterministic pre-execution validation, and `COMPLETED` only through verification. An engine reporting success does not by itself advance an Operation past `VERIFYING`.

### 9.2 Migration workflow states

Migration is a workflow composed from Movements. It adds workflow-level states above the Operation lifecycle:

```text
DRAFT
DISCOVERING
PLANNED
PREPARING
SNAPSHOTTING
CATCHING_UP
VERIFYING
READY_FOR_CUTOVER
CUTTING_OVER
ROLLBACK_WINDOW
COMPLETED

FAILED
PAUSED
ROLLING_BACK
ROLLED_BACK
```

These are workflow states, not runtime primitives. Other workflows built on Movement and Analysis will define their own.

### 9.3 Transition rules

Every state transition is persisted.

Transitions may require:

- deterministic conditions
- policy evaluation
- operator approval

Agents cannot directly mutate runtime state. They submit proposals.

---

## 10. Adaptive Runtime Controls

The runtime should support live changes to performance parameters while preserving correctness guarantees.

Example tunable parameters:

- worker count
- partition concurrency
- writer concurrency
- batch size
- flush interval
- source read rate
- target write rate
- retry backoff
- max outstanding requests

Immutable during a plan version unless replanned:

- source identity
- target identity
- ordering guarantee
- primary migration key
- verification requirements
- transaction semantics

Example policy:

```yaml
runtime:
  concurrency:
    initial: 16
    min: 2
    max: 64

  target:
    max_cpu_percent: 70
    max_write_p95_ms: 250

  source:
    max_cpu_percent: 60

  cdc:
    desired_lag_seconds: 10
    critical_lag_seconds: 120
```

A controller may decrease concurrency when target health exceeds limits and increase it when healthy.

---

## 11. Movement Specification

Example:

```yaml
apiVersion: gantry.io/v1alpha1
kind: Movement

metadata:
  name: orders-replication

source:
  adapter: oracle
  connectionRef: oracle-prod

destination:
  adapter: postgres
  connectionRef: postgres-prod

strategy:
  mode: snapshot_then_stream

datasets:
  - name: customers
    source: CRM.CUSTOMERS
    target: public.customers

    key:
      columns: [customer_id]

    ordering:
      scope: key
      versionField: source_lsn

    partitioning:
      strategy: range
      column: customer_id
      rowsPerPartition: 5000000

    write:
      mode: upsert

    verification:
      required:
        - row_count
        - chunk_checksum
        - primary_key_unique

  - name: orders
    source: ERP.ORDERS
    target: public.orders

    dependsOn:
      - customers

    key:
      columns: [order_id]

    partitioning:
      strategy: time_range
      column: created_at
      interval: 1d

    write:
      mode: upsert

    verification:
      required:
        - row_count
        - chunk_checksum
        - foreign_key_integrity

cdc:
  adapter: debezium
  checkpoint:
    type: source_lsn

runtime:
  maxConcurrency: 32
  maxRetries: 10

  rateLimits:
    sourceRowsPerSecond: 100000
    targetRowsPerSecond: 80000

  policies:
    targetCpuMaxPercent: 70
    sourceCpuMaxPercent: 60

cutover:
  gates:
    allPartitionsVerified: true
    maxCdcLag: 2s
    criticalVerificationFailures: 0
    requireApproval: true

rollback:
  window: 24h
  sourceRemainsAuthoritative: true
```

---

## 12. Adapter Interfaces

Adapters make the harness vendor-neutral.

### Source Adapter

```go
type SourceAdapter interface {
    Discover(ctx context.Context) (*DiscoverySnapshot, error)
    Profile(ctx context.Context, dataset Dataset) (*ProfileReport, error)
    ReadPartition(ctx context.Context, partition Partition, checkpoint Checkpoint) (RecordStream, error)
    CurrentPosition(ctx context.Context) (SourcePosition, error)
}
```

### CDC Adapter

```go
type CDCAdapter interface {
    Start(ctx context.Context, from SourcePosition) error
    Events(ctx context.Context) (<-chan ChangeEvent, error)
    Checkpoint(ctx context.Context) (SourcePosition, error)
    Lag(ctx context.Context) (time.Duration, error)
}
```

### Target Adapter

```go
type TargetAdapter interface {
    Prepare(ctx context.Context, schema TargetSchema) error
    WriteBatch(ctx context.Context, batch Batch) (CommitResult, error)
    ApplyChange(ctx context.Context, event ChangeEvent) (CommitResult, error)
    ReadForVerification(ctx context.Context, query VerificationQuery) (VerificationResult, error)
}
```

### Verifier

```go
type Verifier interface {
    Verify(ctx context.Context, scope VerificationScope) (VerificationResult, error)
}
```

---

## 13. Verification Model

Verification should be hierarchical.

```text
Migration
  Dataset
    Partition
      Chunk
        Row
```

Recommended flow:

```text
row count
   ↓ mismatch?
aggregate checks
   ↓ mismatch?
chunk checksums
   ↓ mismatch?
key diff
   ↓ mismatch?
row-level diff
```

This avoids expensive row-by-row verification across multi-terabyte datasets.

Each verification result must contain:

```text
status
scope
source_result
target_result
difference
timestamp
plan_version
severity
evidence
```

---

## 14. Observability

Every migration exposes a standard telemetry model.

Progress:

- rows read
- rows written
- bytes transferred
- partitions complete
- completion percentage
- throughput
- ETA

CDC:

- source position
- target-applied position
- lag
- events/sec
- oldest unapplied event

Performance:

- source latency
- target write latency
- batch size
- concurrent operations
- worker CPU
- memory
- network throughput

Reliability:

- retry rate
- error rate
- replay count
- checkpoint age
- dead-letter count

Correctness:

- verified partitions
- checksum mismatches
- row-count differences
- constraint violations
- stale write rejects

Runtime events should support OpenTelemetry.

Prometheus metrics should be exposed by default.

---

## 15. Agent Interface

AI is an optional planner, not a trusted executor.

Agents may:

- explain discovery results
- identify migration risks
- recommend partition strategies
- generate initial specs
- recommend concurrency
- diagnose lag
- suggest verification rules
- propose replans
- summarize incidents

Agents may NOT:

- skip required verification
- modify immutable guarantees
- perform cutover without required approval
- execute arbitrary SQL outside adapter policies
- suppress critical errors

Agent requests should use a proposal API:

```json
{
  "migration": "oracle-orders-to-postgres",
  "base_plan_version": 12,
  "proposal": {
    "type": "runtime_adjustment",
    "changes": {
      "maxConcurrency": 16
    }
  },
  "reason": "Target p95 write latency exceeded policy threshold."
}
```

The policy engine validates the proposal before creating `PlanVersion 13`.

---

## 16. Security Model

Enterprise deployments require:

- customer-managed credentials
- least-privilege IAM
- secret references rather than secrets in specs
- encryption in transit
- encryption at rest
- audit log for every state transition
- audit log for every human or agent proposal
- optional isolated data plane
- no raw row data sent to external agents by default
- configurable PII redaction
- RBAC for migration operations
- approval policies for cutover and destructive operations

---

## 17. Suggested Technology Choices

For an initial implementation:

**Runtime:** Python 3.12+ with typed async control-plane code  
**SDK:** Python-first  
**API:** REST/gRPC with language-neutral contracts  
**Metadata store:** PostgreSQL  
**Durable workflow engine:** Temporal  
**Telemetry:** OpenTelemetry + Prometheus  
**CLI:** Typer  
**Config/spec:** YAML + JSON Schema  
**Policy engine:** Open Policy Agent or a minimal native policy layer initially  
**Local development:** Docker Compose  
**Kubernetes:** optional deployment target  

Initial adapters:

- PostgreSQL source
- PostgreSQL target
- Kafka CDC
- Debezium integration
- generic SQL checksum verifier

Do not start with ten databases. Prove the runtime abstraction first.

### Why Python for v1

Python is a deliberate adoption and ecosystem choice, not a decision to implement the high-throughput data plane in Python. Gantry is expected to be extended by data engineers, AI engineers, and platform teams, and Python provides the lowest-friction surface for adapters, verification logic, policies, notebooks, and agent SDK integrations.

The runtime boundary must remain explicit:

> **Python owns intelligence, orchestration, and reliability state. Existing engines own data-plane throughput.**

Gantry SHOULD coordinate coarse-grained operations rather than process billions of records through Python loops. Bulk movement, CDC transport, large transforms, and compute-heavy operations should be delegated to systems such as databases, Kafka, Debezium, Beam, Spark, Flink, or native libraries. CPU-heavy local work such as large checksum computation should use native implementations, separate worker processes, or external engines.

The core SHOULD use strict typing and explicit contracts. Migration and Movement specs, checkpoints, state transitions, policy decisions, adapter results, and verification evidence should use typed models rather than unstructured dictionaries. Durable state must live outside worker processes so workers remain disposable.

Integrations SHOULD remain isolated optional packages/extras to avoid dependency conflicts across database and agent SDK ecosystems. Public runtime interfaces and APIs should remain language-neutral so a performance-sensitive daemon or worker can later be implemented in Go or Rust without changing the Gantry user model.

---

## 18. MVP

The first useful release should prove the **Movement** abstraction through one concrete scenario:

> PostgreSQL A → PostgreSQL B, snapshot + CDC, while writes continue.

This is deliberately migration-shaped, but the runtime should model it as a Movement. Cutover is supplied by the first higher-level Migration workflow rather than baked into the Movement primitive.

MVP capabilities:

1. Discover tables and primary keys.
2. Partition a large table by primary-key range.
3. Copy partitions concurrently.
4. Persist per-partition checkpoints.
5. Support replay after worker crash.
6. Use idempotent target upserts.
7. Consume CDC from Debezium/Kafka.
8. Reject stale CDC writes using source sequence metadata.
9. Track CDC lag.
10. Compare row counts.
11. Compute checksums by key range.
12. Block cutover if verification fails.
13. Provide manual cutover approval.
14. Retain source as rollback authority.
15. Expose Prometheus metrics.
16. Show migration state through CLI/API.

This is enough to demonstrate the core thesis without building an enormous platform.

---

## 19. MVP Demo Scenario

Seed source with 100M synthetic orders.

Start continuous writes.

Run:

```bash
gantry plan movement.yaml
gantry start orders-replication
gantry status orders-replication
```

During migration:

- kill a worker
- restart it
- demonstrate checkpoint replay
- deliberately send duplicate events
- show idempotency
- overload target
- show concurrency throttle
- corrupt a target chunk
- show checksum failure
- repair/replay only the failed partition

Then catch CDC up to < 2 seconds.

Attempt cutover with failed verification:

```text
CUTOVER BLOCKED
Reason: partition orders/2026-08-17 checksum mismatch
```

Repair.

Approve:

```bash
gantry migration approve-cutover orders-migration
```

Switch.

This demo tells the whole story.

---

## 20. Repository Layout

```text
/
├── cmd/
│   └── gantry/
├── api/
├── internal/
│   ├── controller/
│   ├── planner/
│   ├── scheduler/
│   ├── state/
│   ├── checkpoint/
│   ├── policy/
│   ├── verification/
│   └── telemetry/
├── adapters/
│   ├── source/
│   │   └── postgres/
│   ├── target/
│   │   └── postgres/
│   └── cdc/
│       └── kafka/
├── spec/
│   ├── schema/
│   └── examples/
├── docs/
│   ├── architecture.md
│   ├── guarantees.md
│   ├── adapters.md
│   └── rfcs/
├── examples/
│   └── postgres-to-postgres/
├── deploy/
│   ├── docker/
│   └── kubernetes/
└── README.md
```

---

## 21. First Milestones

### M0 — Spec and simulator

Implement movement spec parsing and an in-memory execution simulator.

Goal: validate abstractions before integrating databases.

### M1 — Reliable bulk copy

Postgres → Postgres partitioned snapshot with:

- durable state
- retries
- checkpoints
- idempotent writes
- pause/resume

### M2 — Verification

Add:

- row counts
- chunk checksums
- mismatch localization
- verification gates

### M3 — CDC

Integrate Debezium + Kafka.

Support:

- snapshot position
- CDC catch-up
- lag
- replay
- stale write protection

### M4 — Adaptive runtime

Add:

- configurable rate limits
- dynamic concurrency
- target-health policies
- pause/throttle/resume

### M5 — Cutover and recovery

Add:

- cutover gates
- approvals
- rollback state
- audit report

### M6 — Agent planner

Add an optional agent API capable of:

- generating movement specs from discovery
- suggesting partitions
- proposing runtime changes
- explaining failures

No agent-generated proposal bypasses deterministic policy validation.

---

## 22. Design Principles

**Gantry coordinates data movement; it does not become the data path.**

Python is the orchestration and extension layer. High-throughput transport and processing belong to existing data engines. Gantry should issue, supervise, checkpoint, verify, throttle, and recover coarse-grained work rather than own the per-record hot path.

**Correctness over throughput.**

A migration that finishes quickly but cannot prove correctness is a failed migration.

**Replay over fragile exactly-once assumptions.**

Assume delivery can duplicate. Make effects idempotent.

**Checkpoint after commit.**

Never allow progress metadata to move ahead of durable state.

**Make ordering boundaries explicit.**

Do not accidentally pay for global ordering.

**Verification is part of execution.**

It is not a final QA step.

**Agents propose; runtime disposes.**

AI operates inside deterministic rails.

**Adapters over replacements.**

Use enterprise infrastructure already trusted by customers.

**Everything is inspectable.**

Plans, checkpoints, policy decisions, verification results, and state transitions are persisted and auditable.

---

## 23. Open Questions

1. Should Temporal be required, optional, or hidden behind a workflow abstraction?
2. Should CDC offsets be owned by the harness or delegated entirely to the connector?
3. How should transaction-level ordering work when one source transaction spans Kafka partitions?
4. What is the minimum common abstraction for sources with very different CDC semantics?
5. Should verification queries run directly against source and target or through dedicated verification workers?
6. How much partition planning should be deterministic before adding AI?
7. What migration metadata may safely be exposed to hosted AI services?
8. How should schema evolution during a running migration affect PlanVersion?
9. How should reverse CDC during rollback windows be modeled?
10. What guarantees should be represented formally in the spec versus inferred from adapters?

---

## 24. Project Thesis

The open-source ecosystem already has excellent tools for transporting and transforming data.

What it lacks is a common reliability runtime for **data movement**.

Gantry should become the layer that says:

> Describe what needs to move and the guarantees it requires. Gantry coordinates the tools you already trust and makes execution restartable, observable, verifiable, policy-controlled, and recoverable.

Migration is the first productized workflow, not the fundamental abstraction.

**Agents decide what should move and can propose how. Gantry makes sure it moves safely.**

---

# RFC Extension: Agent Evidence Layer

## Expanded Thesis

Gantry is not only a reliability layer for moving enterprise data. It is a data execution layer that allows agents to reason over data volumes that cannot and should not fit in an LLM context.

The system separates three concerns:

```text
Dataset
    Common logical representation of enterprise data and signals.

Movement
    Make or keep a Dataset reliably available.

Analysis
    Compute over one or more Datasets using the appropriate execution engine.

Result
    Return bounded outputs with provenance, measurements, artifacts, and optional evidence semantics.
```

An SDLC agent remains outside Gantry. It consumes Analysis Results and uses their findings, measurements, artifacts, and provenance to inspect repositories, propose code changes, run tests, and participate in the software delivery lifecycle.

The core principles are:

> Gantry coordinates large-data computation; it does not pull large data through Python or through an LLM.

> Move data through data engines. Move bounded analysis results into agents.

## Target Use Case: Signals to Code Change

```text
Logs ──────────┐
Metrics ───────┤
Traces ────────┤
Database ──────┤
Deployments ───┤
Incidents ─────┤
Tickets ───────┘
       │
       v
   ┌──────────┐
   │  Gantry  │
   └────┬─────┘
        │
        ├── Movement
        │    ingest / synchronize / checkpoint
        │
        ├── Analysis
        │    normalize / filter / join / aggregate
        │    correlate / window / sample
        │
        └── Result
             findings / provenance / measurements
             supporting queries / source references
        │
        v
    SDLC Agent
        │
        ├── inspect repository
        ├── connect evidence to code paths
        ├── propose patch
        ├── run tests
        └── submit change for review
```

Example objective:

> Explain why checkout latency increased after yesterday's deployment by correlating logs, traces, deployment metadata, and database metrics.

The LLM must not consume billions of raw log records. Gantry executes the large-data analysis and gives the agent a compact, inspectable AnalysisResult.

## Core Resource Model

Gantry has four core resources.

### Dataset

The addressable logical unit of enterprise data and signals.

A Dataset is a first-class resource, not a field nested inside another spec. It is registered, versioned, and referenced by name. Both Movement and Analysis take Datasets as inputs, and either may produce one as an output.

A Dataset owns its physical reference, schema, keys, time semantics, statistics, access policy, and sensitive-field declarations. Its manifest exposes enough metadata for planners and agents to reason about the data without reading it.

Dataset manifests are versioned. A Result's provenance pins the manifest versions it read.

See "Dataset and Signal Model" for the manifest format.

### Movement

A reliable transfer or synchronization of data.

Examples include snapshots, CDC replication, backfills, replay, synchronization, and materialization.

Movement owns source/destination, partitioning, checkpoints, ordering, idempotency, replay, verification, and rate limits.

### Analysis

A reproducible computation over one or more datasets or signals.

Examples:

- filter tens of terabytes of logs to a relevant time window
- normalize service identifiers across logs and traces
- join traces to deployment events
- aggregate latency by service/version
- correlate database lock waits with application errors
- identify anomalous partitions
- generate representative samples

Analysis owns inputs, transformations, join and temporal semantics, execution engine, resource policies, output dataset, and reproducibility metadata.

### Result

A bounded, structured output from an Analysis intended for machine or human consumption.

A Result is not merely an LLM summary. It can contain findings, aggregates, diffs, profiles, samples, generated datasets, artifacts, supporting measurements, source datasets, time ranges, queries/computations, lineage, deployment/code references, generation time, Analysis version, and access/redaction metadata.

Evidence is a semantic property or specialization of a Result, not a top-level Gantry primitive. A downstream agent must still be able to ask: **Why do we believe this?** and trace the answer through provenance.


## Public Abstraction and Internal Execution Model

The primary user-facing abstraction is **Analysis**, not Evidence and not generic Compute.

The public model is:

```text
Dataset
   │
   ├── Movement
   │      reliably make/keep data available
   │
   └── Analysis
          compute over one or more datasets
                 │
                 ▼
               Result
```

`AnalysisResult` may take several semantic forms:

```text
AnalysisResult
├── AggregateResult
├── ProfileResult
├── DiffResult
├── DatasetResult
├── FindingResult
└── EvidenceResult
```

`EvidenceResult` is useful when the caller needs explicit support for a claim, but it is only one specialization.

Internally, Gantry may represent executable work as a `Computation` or `ExecutionPlan`, but this should remain an implementation concept in v1 rather than the primary product abstraction. Gantry is not trying to become a generic distributed compute engine; it compiles Analysis operations onto engines that already exist.


## Dataset and Signal Model

Gantry exposes heterogeneous enterprise signals through a common logical Dataset interface without requiring one physical store.

Examples:

```text
application_logs
distributed_traces
service_metrics
deploy_events
feature_flags
database_metrics
database_tables
kafka_topics
incident_events
support_tickets
build_results
test_results
```

A Dataset Manifest gives planners and agents useful metadata without exposing the full dataset.

```yaml
apiVersion: gantry.dev/v1alpha1
kind: Dataset

metadata:
  name: checkout-logs

physical:
  adapter: clickhouse
  reference: observability.logs
  estimatedRows: 8400000000
  estimatedBytes: 14.2TB

schema:
  timestamp: event_time
  keys:
    - request_id
    - trace_id
    - service

statistics:
  changeRatePerSecond: 18000

semantics:
  timeField: event_time

access:
  agentPolicy: aggregate_or_masked

sensitiveFields:
  - user_email
  - payment_token
```

## Progressive Data Access for Agents

Agents should not begin with raw rows.

```text
Dataset
   ↓
describe
   ↓
profile
   ↓
aggregate/query
   ↓
partition
   ↓
sample
   ↓
exact records
```

Access becomes more expensive and potentially more sensitive as the agent moves downward. Policy can prohibit row access entirely.

Representative Python API:

```python
dataset = gantry.dataset("payments")

await dataset.describe()
await dataset.profile(columns=["payment_status", "provider"])

result = await dataset.query(
    query,
    params={"start": start_time},
)

sample = await dataset.sample(
    where={"payment_status": "FAILED"},
    limit=25,
)
```

## Analysis Specification

```yaml
apiVersion: gantry.dev/v1alpha1
kind: Analysis

metadata:
  name: checkout-latency-regression

objective:
  explain: checkout latency regression

inputs:
  - dataset: application_logs
  - dataset: traces
  - dataset: deploy_events
  - dataset: postgres_metrics

window:
  start: "2026-09-03T00:00:00Z"
  end: "2026-09-04T00:00:00Z"

normalize:
  fields:
    request_id:
      aliases: [request_id, trace_id]
    service:
      aliases: [service_name, app]

joins:
  - left: application_logs
    right: traces
    on: [request_id]

  - left: traces
    right: deploy_events
    on: [service]
    temporal:
      strategy: nearest_preceding
      maxDistance: 24h

signals:
  - p95_latency
  - error_rate
  - database_wait_time
  - timeout_count
  - database_calls_per_request

execution:
  engine: auto

output:
  kind: AnalysisResult
```

The planner may generate this specification. The deterministic execution layer validates and compiles it.

## Analysis Execution

Python is the orchestration layer, not the bulk computation layer.

An Analysis compiler invokes an execution adapter appropriate for the physical data:

```text
ClickHouse       log filtering / aggregation
BigQuery         warehouse-scale SQL
Snowflake        warehouse-scale SQL
Spark            large batch joins/transforms
Flink            continuous signal correlation
Beam/Dataflow    batch/stream processing
OpenSearch       indexed log investigation
PostgreSQL       bounded relational analysis
DuckDB           local/small artifact analysis
```

Execution engines own scans, joins, sorting, aggregation, shuffle, and distributed computation.

Gantry owns planning, dispatch, state, policy, lineage, retries, resource constraints, and result construction.

The hard boundary is:

> Python owns control flow. Data engines own data flow.

## Analysis Result Model

Example:

```json
{
  "analysis": "checkout-latency-regression",
  "version": 7,
  "findings": [
    {
      "id": "finding-1",
      "claim": "Database calls per checkout increased after deploy 8f3142.",
      "strength": 0.94,
      "measurements": {
        "before_calls_per_request": 3.1,
        "after_calls_per_request": 17.8,
        "postgres_lock_wait_change": "+620%",
        "timeout_error_change": "+340%"
      },
      "references": {
        "deployment": "8f3142",
        "services": ["checkout-api"],
        "code_hints": [
          "CheckoutService",
          "PricingRepository",
          "loadActivePromotions"
        ]
      }
    }
  ]
}
```

`strength` must not silently mean "LLM confidence." Where possible it is derived from deterministic or statistical evidence. Subjective model confidence must be explicitly labeled.

## Provenance

Every AnalysisResult finding must be traceable.

```text
Finding
   │
   ├── Analysis version
   ├── computation/query
   ├── intermediate artifacts
   ├── Dataset Manifest versions
   ├── source partitions/time ranges
   └── Movement/checkpoint state where relevant
```

This enables reproducibility, auditing, debugging, evidence refresh, comparisons across time, downstream citations, and human inspection.

## Analysis API for SDLC Systems

Gantry exposes an ordinary typed Analysis API that agent frameworks can wrap.

```python
result = await gantry.analyze(
    objective="Explain checkout latency regression",
    signals=[
        "application_logs",
        "traces",
        "deploy_events",
        "postgres_metrics",
    ],
)

for finding in result.findings:
    print(finding.claim)
    print(finding.support)
```

Lower-level surfaces:

```text
gantry.datasets.describe
gantry.datasets.profile
gantry.datasets.query
gantry.datasets.sample

gantry.analysis.plan
gantry.analysis.execute
gantry.analysis.status

gantry.results.get
gantry.results.explain
gantry.results.provenance
gantry.results.refresh
```

Agno, LangGraph, OpenAI Agents SDK, and similar integrations should be optional wrappers rather than dependencies of `gantry-core`.

## SDLC Boundary

Gantry should not become an SDLC framework.

Its responsibility ends at producing trustworthy Analysis Results and exposing references that help an SDLC system connect those results to software artifacts.

```text
Gantry AnalysisResult
      │
      v
SDLC Harness
      │
      ├── git history
      ├── deployment metadata
      ├── repository graph
      ├── code search
      ├── tests
      └── CI
      │
      v
Candidate cause
      │
      v
Proposed code change
      │
      v
Tests / review / deployment
```

Gantry may carry commit SHAs, deployment IDs, service names, repository references, and trace-to-service mappings. Repository editing and code generation belong to the SDLC harness.

## Continuous Evidence

Analysis can also be continuous:

```yaml
kind: Analysis

metadata:
  name: checkout-regression-watch

mode: continuous

inputs:
  - traces
  - deploy_events
  - postgres_metrics

window:
  type: sliding
  duration: 30m

baseline:
  duration: 7d

trigger:
  when:
    p95_latency_change: "> 50%"
```

This supports:

```text
Production signals
       ↓
continuous Analysis
       ↓
new Evidence
       ↓
SDLC event
       ↓
agent investigation
       ↓
possible code change
```

Analysis result generation alone never authorizes a production code modification.

## Policy for Agent Data Access

Agent access is separate from ordinary database/human access.

```yaml
agentAccess:
  default:
    rows: deny
    aggregates: allow
    metadata: allow

  pii:
    mode: redact

  samples:
    maxRows: 50
    requireReason: true

  queries:
    maxBytesScanned: 100GB
    timeout: 5m

  evidence:
    persist: true
```

Policies can govern datasets, fields, sample sizes, masking, query cost, execution time, join relationships, raw-row access, and external-model exposure.

An LLM cannot override these constraints.

## Updated Architecture

```text
                         Agents
             ┌─────────────┴──────────────┐
             │                            │
      Analysis / Ops Agent          SDLC Agent
             │                            │
             │                   consumes AnalysisResult
             └─────────────┬──────────────┘
                           │
                     Gantry API / SDK
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        v                  v                  v
    Movement            Analysis            Result
     Runtime             Runtime             Store
        │                  │                  │
 checkpoints          compiler/planner    provenance
 verification         engine dispatch     findings
 replay               joins/windows       artifacts
 policies             aggregation         lineage
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                    Adapter Layer
                           │
       ┌───────────┬───────┼────────┬───────────┐
       v           v       v        v           v
    Kafka       Debezium  Spark   ClickHouse  BigQuery
                           │
                           v
                  Enterprise Signals
```

## Updated Python Boundary

Python remains the preferred v1 implementation language because Gantry primarily performs orchestration, planning, typed SDK work, adapter coordination, policy, state management, and agent integration.

Python MAY process bounded metadata, manifests, analysis results, samples, and control messages.

Python SHOULD NOT become the execution path for unbounded or high-throughput record processing.

Heavy operations are compiled or delegated to execution engines.

## Updated MVP Sequence

Do not attempt Movement + Analysis + Result semantics simultaneously.

### MVP 1 — Reliable Movement

PostgreSQL → PostgreSQL:

- partitioned snapshot
- checkpoints
- replay
- idempotency
- verification
- CDC catch-up

### MVP 2 — Dataset Interface

Expose:

- discovery
- manifests
- describe
- profile
- aggregate query
- bounded sample
- access policies

### MVP 3 — Analysis Results

Concrete observability scenario:

```text
application logs
+
traces
+
deployment events
+
Postgres metrics
        ↓
"Why did checkout latency regress?"
```

Implement an Analysis spec, one execution-engine adapter, reproducible artifacts, AnalysisResult objects, provenance, and the agent-facing Python API.

### MVP 4 — SDLC Integration Demo

1. Introduce an N+1 query in a sample service.
2. Deploy it.
3. Generate logs, traces, and database metrics.
4. Gantry identifies the correlated regression.
5. The AnalysisResult references the deployment and service.
6. An external SDLC agent traces the evidence to affected code.
7. The agent proposes a patch.
8. Tests demonstrate improvement.

Gantry does not perform the code edit itself.

## Updated Project Positioning

Short:

> **Gantry is the open-source data execution layer for agents.**

More precise:

> **Gantry moves, analyzes, and connects large enterprise data so agents can operate on bounded, reproducible results instead of raw data.**

The long-term abstraction is:

```text
Enterprise Data
      ↓
    Dataset
      ↓
Movement / Analysis
      ↓
    Result
      ↓
    Agents
      ↓
   Actions
```

Gantry owns everything through Result.

The consuming agent system owns the Action.

## Agent-Generated Execution Lifecycle

Gantry supports agent-generated data operations, but generation is not execution and successful execution is not proof of correctness.

The lifecycle is:

```text
Intent → Plan → Generate → Validate → Execute → Verify → Result
```

**Guarantees wrap the entire lifecycle** rather than appearing only after execution.

```text
              GANTRY GUARANTEE BOUNDARY
┌──────────────────────────────────────────────────┐
│ Plan → Generate → Validate → Execute → Verify    │
│  ▲                                         │     │
│  └──────────── agentic repair/replan ──────┘     │
└──────────────────────────────────────────────────┘
                         │
                         ▼
                   trusted Result
```

The core boundary is:

> **The agent owns adaptation. Gantry owns acceptance.**

An agent may plan, generate, diagnose, repair, and re-plan. Gantry deterministically decides whether generated work is valid, permitted, executable, and whether its result satisfies declared verification requirements.

### Plan

Planning converts intent into a typed `Movement` or `Analysis` specification. Plans declare inputs, outputs, operations, execution constraints, policies, and verification requirements before crossing into execution.

### Generate

A plan may compile directly to an engine API or require generated executable artifacts such as SQL, Spark jobs, Beam pipelines, Flink jobs, dbt models, connector/CDC configurations, transformations, or engine-specific job specifications.

Generated artifacts are versioned and retained as provenance. Code generation is an implementation mechanism beneath `Movement` and `Analysis`, not a top-level Gantry resource.

### Validate

Generated artifacts do not execute merely because generation succeeded.

Pre-execution validation can include:

- syntax/compile validation
- schema and type compatibility
- dataset existence
- authorization and policy checks
- sensitive-field restrictions
- dependency validation
- static analysis
- query-plan inspection
- cost/resource estimation
- bounded sample execution
- sandbox/dry-run execution
- declared invariant validation

For SQL this may include `EXPLAIN`, dry runs, schema resolution, and bounded queries. For Spark, Beam, or Flink it may include graph construction, schema checks, fixture execution, and execution-plan inspection.

Validation returns either an accepted executable artifact or a structured failure suitable for agentic repair.

### Execute

Execution dispatches the accepted artifact to the appropriate data engine. Data engines own high-throughput computation; Gantry owns the execution contract around it.

Gantry owns submission, execution state, checkpoints where applicable, retries, cancellation, resource controls, rate limits, progress monitoring, intermediate artifacts, failure classification, and provenance capture.

Gantry does not implement distributed scans, joins, shuffles, or bulk record processing in Python when an external engine is the appropriate substrate.

### Verify

A completed job is not necessarily a correct job.

Post-execution verification determines whether output satisfies the semantics and guarantees declared by the plan.

Verification can include:

- completeness and reconciliation
- row counts and checksums
- schema invariants
- uniqueness constraints
- null-rate bounds
- join coverage
- row-expansion bounds
- temporal alignment
- statistical distribution checks
- business invariants
- expected relationships
- lineage completeness

Example:

```yaml
verify:
  - rowExpansion:
      max: 1.1
  - joinCoverage:
      min: 0.95
  - nullRate:
      field: trace_id
      max: 0.05
  - temporalAlignment:
      maxDifference: 5m
```

An engine may report `SUCCESS` while Gantry reports `VERIFICATION_FAILED`. For example, a technically successful join that expands 84 million rows to 1.7 billion can be rejected when the declared maximum expansion is `1.1x`.

### Agentic Repair Loop

Validation and verification failures become structured inputs to the planner or agent:

```text
Plan → Generate → Validate ──failure──┐
                    ↓                 │
                 Execute              │
                    ↓                 │
                  Verify ───failure───┤
                    ↓                 │
                  Result              │
                                      │
                     diagnose / repair
                                      │
                                      └──→ Plan
```

The agent may propose a new join key, transformation, partition strategy, query, or engine configuration. Every repaired artifact re-enters the same validation and verification boundary.

An agent cannot waive a failed guarantee unless an authorized policy or human approval explicitly changes the plan.

## Guarantee Model

Guarantees are cross-cutting constraints over the complete lifecycle.

**Execution guarantees:** checkpointing, replayability, idempotency, ordering and atomicity boundaries, retry semantics, and recoverability.

**Generation guarantees:** artifact provenance, immutable/versioned generated code, schema compatibility, deterministic validation gates, policy compliance, and bounded execution before promotion where required.

**Result guarantees:** verification invariants, reconciliation, completeness, lineage, reproducibility, freshness, and declared quality thresholds.

**Governance guarantees:** authorization, data-access policy, masking/redaction, resource and cost bounds, approval gates, and audit trail.

The guarantee layer remains deterministic even when planning and code generation are agentic.

## Updated Agentic Thesis

Gantry can accept an explicitly authored `Movement` / `Analysis` or use an agent to generate one:

```text
Agent / Application
        │
        ▼
      Gantry
        ├── Plan
        ├── Generate executable artifact
        ├── Validate
        ├── Execute on existing data infrastructure
        ├── Verify
        └── Produce AnalysisResult / MovementResult
        │
        ▼
Agent / SDLC / Application
```

For an SDLC workflow:

```text
Huge logs + traces + metrics + deploy signals
                    │
                    ▼
                  Gantry
                    │
        Plan / Generate / Validate
                    │
                    ▼
          Spark / Flink / SQL / ...
                    │
                    ▼
                 Verify
                    │
                    ▼
             AnalysisResult
                    │
                    ▼
                SDLC Agent
                    │
           inspect relevant code
                    │
              propose change
```

Gantry verifies the **data operation and its result**. The consuming SDLC harness remains responsible for verifying a subsequent code change through its own build, test, review, and deployment guarantees.

> **Gantry is the runtime for agent-generated data operations: it plans and generates executable data work, validates it, runs it on existing data infrastructure, and verifies the result under deterministic guarantees.**
