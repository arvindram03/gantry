# MongoDB / NoSQL support — design spec

- Issue: https://github.com/arvindram03/gantry/issues/12
- Status: approved, ready for implementation planning
- Date: 2026-09-10

## Summary

Add a `gantry.nosql` surface that gives MongoDB the same governed
propose → constrain → execute → verify → accept lifecycle that
`gantry.sql` gives relational engines, using MongoDB as the first (and
for v0, only) NoSQL provider.

## Goals

- A `gantry.nosql.connect("mongodb", **config)` entry point that returns a
  `NoSQLConnection` with the same configure-once-then-call-or-`.tool()`
  shape as `SQLConnection`.
- Governed reads via `db.query()`, accepting either a plain filter dict or a
  read-only aggregation pipeline.
- Governed, create-only-equivalent writes via `db.materialize()`, driven by
  `$out`/`$merge` pipelines.
- Verification checks (`DocumentCheck` Protocol) parallel to
  `MaterializationCheck`, adapted to Mongo's schemaless documents via
  bounded sampling.
- Unit tests against an in-memory stub adapter, plus a live integration
  test against real MongoDB that self-skips when unreachable.
- A runnable example script and `docs/nosql.md`, plus a README "Supported
  systems" entry.

## Non-goals (v0)

- Any NoSQL engine other than MongoDB (no DynamoDB, Cassandra, etc.).
- Full MQL support — only `$match, $project, $group, $sort, $limit,
  $unwind, $lookup` for reads, plus `$out`/`$merge` for writes. Stages
  outside this set fail classification and are rejected at `validate()`.
- Schema discovery / `db.describe()` for Mongo — collections are
  schemaless, and full schema inference is out of scope for v0.
- Exhaustive field presence guarantees in `RequiredFields` — the check
  samples a bounded number of documents from the destination and reports
  presence within that sample, not a full-collection guarantee. This is a
  documented limitation, not a bug.
- `docker-compose` integration — the live test and example use a
  standalone `docker run mongo`, not `examples/stack/docker-compose.yml`.
- `motor` or any driver besides PyMongo's native async client
  (`pymongo.AsyncMongoClient`, PyMongo >= 4.9).

## Architecture

`gantry/nosql/` mirrors `gantry/sql/` layer-for-layer:

| File | Role | SQL analog |
|---|---|---|
| `target.py` | `NoSQLTarget` (provider/driver/config/metadata) | `sql/target.py` |
| `policy.py` | `NoSQLPolicy` (read_only, allowed/denied collections, max_documents, timeout_seconds, max_bytes_scanned, max_cost_usd) | `sql/policy.py` |
| `capabilities.py` | `NoSQLCapabilities` | `sql/capabilities.py` |
| `adapter.py` | `NoSQLAdapter` Protocol | `sql/adapter.py` |
| `pipeline.py` | stage classifier: normalizes filter dicts and pipelines, classifies read/write, extracts referenced collections | `sql/dialect.py` + `sql/classification.py` |
| `enforcement.py` | `policy_errors()` | `sql/enforcement.py` |
| `bridge.py` | `NoSQLExecutionAdapter`, adapts `NoSQLAdapter` into the generic `ExecutionAdapter` | `sql/bridge.py` |
| `query.py` | `NoSQLQuery`, configured read operation with `.tool()` | `sql/query.py` |
| `materialization.py` | `NoSQLMaterializer`, governs `$out`/`$merge` | `sql/materialization.py` |
| `result.py` / `output.py` | `NoSQLResult`, `InlineDocuments` | `sql/result.py` + `sql/output.py` |
| `registry.py` | provider registry, seeded with `"mongodb"` only | `sql/registry.py` |
| `api.py` | `NoSQLConnection` + `connect()` | `sql/api.py` |
| `adapters/mongodb.py` | concrete adapter using `pymongo.AsyncMongoClient` | `sql/adapters/postgres.py` etc. |

Verification lives in `gantry/nosql/verify.py`: a `DocumentCheck` Protocol
(`evaluate(self, collection: CollectionSnapshot | None) -> CheckResult`),
parallel to `gantry/verify.py`'s `MaterializationCheck`. `CollectionSnapshot`
is the Mongo analog of `sql.schema.Table`: `name`, `metadata`
(`document_count` when available), and `fields: tuple[str, ...]` derived
from a bounded document sample.

## Components & data flow

### Configuration

```python
db = gantry.nosql.connect("mongodb", uri=..., database="analytics")
reads = db.query(collections=["orders"], max_documents=1_000, timeout=30)
writes = db.materialize(sources=["orders"], destinations=["reporting.daily_rollup"])
```

### Read path (`db.query()` → `NoSQLQuery.__call__`)

1. Caller passes either a plain filter dict (`{"status": "open"}`) or a
   full aggregation pipeline (`[{"$match": ...}, {"$group": ...}]`).
2. `pipeline.py` normalizes both into a stage list and classifies it:
   every stage must be in the v0 read-only allowlist (`$match, $project,
   $group, $sort, $limit, $unwind, $lookup`). `$out`/`$merge` in this path
   is a hard rejection — writes only happen through `materialize()`.
3. `enforcement.py` checks classified collections (including
   `$lookup.from`) against `allowed_collections`/`denied_collections`,
   and stage count/shape against `max_documents`/`timeout_seconds`/
   `max_bytes_scanned`, the same way `sql/enforcement.py` does for SQL.
4. `NoSQLExecutionAdapter.validate()` → `admit()` →
   `MongoAdapter.submit()` runs the pipeline via `AsyncMongoClient`,
   `ControlPlane` persists the handle, `wait()` polls until terminal.
5. Result documents come back as `InlineDocuments` (BSON→JSON-safe types
   only) inside `NoSQLResult`, capped and truncation-flagged like
   `InlineRows`.

### Write path (`db.materialize()` → `NoSQLMaterializer`)

1. Accepts a single pipeline that must end in exactly one `$out` or
   `$merge` stage naming a destination in `destinations`.
2. Source collections referenced anywhere in the pipeline (`$lookup`,
   initial collection) must be in `sources`. This is the create-only
   equivalent for Mongo: `$merge` with `whenMatched: "replace"` or
   `"merge"` is allowed; other `$merge` write modes (e.g.
   `"keepExisting"`, `"pipeline"`, arbitrary update pipelines) are
   rejected, to keep the "governed write" guarantee meaningful.
3. After execution, `MongoAdapter.inspect_collection()` (parallel to
   `inspect_table`) samples the destination — document count via
   `estimated_document_count`/`count_documents`, and a bounded sample
   (default 100 docs) merged into a field-presence summary — and hands
   that to `verify.py` checks as a `CollectionSnapshot`.

### Verification checks (`gantry/nosql/verify.py`)

- `destination_exists()` — parallel to SQL's `DestinationExists`.
- `document_count(min=, max=)` — parallel to `RowCount`, reads
  `CollectionSnapshot.metadata["document_count"]`.
- `required_fields(fields)` — parallel to `RequiredColumns`, checks
  against `CollectionSnapshot.fields`, with a docstring caveat that
  fields absent from every sampled document will read as missing even if
  present in unsampled documents.

## Error handling

No new `FailureKind` values — the existing SQL-derived taxonomy already
covers the Mongo cases:

- Unclassifiable or disallowed pipeline stage → `VALIDATION_ERROR` /
  `OPERATION_NOT_ALLOWED` (fails at `validate()`, never reaches the
  adapter).
- Collection not in `allowed_collections` / in `denied_collections` →
  `SOURCE_NOT_ALLOWED` / `DESTINATION_NOT_ALLOWED`.
- Destination collection already exists on a create-only `$out` →
  `DESTINATION_EXISTS`.
- Driver-level errors (`pymongo.errors.OperationFailure`, `PyMongoError`,
  auth failures, network errors) are caught in `MongoAdapter` and
  normalized: auth failures → `AUTH_ERROR`, timeouts → `TIMEOUT`,
  everything else driver-side → `CONNECTOR_ERROR`. Nothing raw ever
  propagates past the adapter boundary — same fail-closed rule as SQL.
- `max_documents`/`timeout_seconds` exceeded → adapter-enforced where
  `NoSQLCapabilities.document_limit`/`operation_timeout` is `True`;
  otherwise admission rejects up front with
  `UNSUPPORTED_POLICY_REQUIREMENT`, same as SQL's row/timeout handling.
- Post-execution check failures (`RequiredFields`, `DocumentCount`)
  produce `VERIFICATION_FAILED` on the `Result` — execution succeeded,
  acceptance didn't.

## Testing

- **`tests/test_nosql.py`** — unit tests against `StubMongoAdapter` (an
  in-memory fake implementing `NoSQLAdapter`, mirroring
  `StubSQLAdapter`): pipeline classification, policy enforcement
  (allowed/denied collections, document/timeout limits), admission
  rejection paths, `query()`/`materialize()` configuration, verification
  checks against synthetic `CollectionSnapshot`s.
- **`tests/test_mongodb_live.py`** — real MongoDB via `docker run mongo`,
  following `test_postgres_live.py`'s pattern: `pytest.importorskip
  ("pymongo")`, `pytest.skip(...)` when unreachable, seeded fixture
  collections, exercises `connect("mongodb", ...)` end-to-end for both
  the query and materialize paths.
- Both live-test files self-skip when nothing is reachable, so `make
  check` stays green on a clean clone with no services running.

## Docs & examples

- **`docs/nosql.md`** — new doc, same depth as `docs/sql.md`: connection
  setup, `query()`/`materialize()` usage, policy fields, capabilities,
  verification checks, adapter contract summary.
- **README** — add MongoDB to the "Supported systems" table.
- **`examples/mongodb_rollup.py`** — a runnable example against a seeded
  local MongoDB, following `examples/warehouse_rollup.py`'s shape (seed
  data, governed query, governed materialize, verification checks), added
  to `examples/README.md`'s index. Local run instructions use `docker run
  mongo`, not the shared `examples/stack/docker-compose.yml`.

## Dependencies

- New optional extra: `mongodb = ["pymongo>=4.9"]` in `pyproject.toml`,
  alongside `postgres`/`duckdb`/`bigquery`/`snowflake`. Not added to
  `all-sql` (that extra is SQL-specific by name and scope).
