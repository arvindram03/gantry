# Gantry NoSQL

Gantry NoSQL is a governed execution boundary for agent-generated MongoDB pipelines. It does
not expose a database connection or invent a portable query language — every filter or pipeline
runs exactly as written, against MongoDB, through `pymongo.AsyncMongoClient`.

## Configure once

Install the MongoDB driver, then connect once:

```python
import gantry

db = gantry.nosql.connect("mongodb", uri="mongodb://localhost:27017", database="analytics")
query = db.query(
    read_only=True,
    collections=("orders",),
    max_documents=500,
    timeout=30,
)
```

Call the governed operation directly, with a plain filter dict:

```python
result = await query("orders", {"status": "open"})
```

or a read-only aggregation pipeline:

```python
result = await query(
    "orders",
    [{"$match": {"status": "open"}}, {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}}],
)
```

`NoSQLResult.inline` holds the returned documents, BSON types converted to JSON-safe Python
values, capped at `max_documents` and flagged with `truncated` when the cap was hit.

Or expose its narrow, framework-neutral form to an agent:

```python
tool = query.tool()

tool.name  # "query_nosql"
tool.input_schema  # {"collection": "...", "pipeline": ...}
result = await tool.invoke(collection="orders", pipeline={"status": "open"})
```

Query policy — allowed/denied collections, document/timeout/byte/cost limits — never appears in
the agent tool schema.

## Governed writes: `db.materialize()`

Writes only ever happen through a pipeline that ends in exactly one `$out` or `$merge` stage,
naming a destination the operator has approved:

```python
materialize = db.materialize(
    sources=["orders"],
    destinations=["reporting.daily_rollup"],
    verify=[gantry.nosql.destination_exists(), gantry.nosql.document_count(min=1)],
)

result = await materialize(
    "orders",
    [
        {"$match": {"status": "open"}},
        {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
        {"$out": "reporting.daily_rollup"},
    ],
)
```

`$out` is create-only: if the destination collection already exists, the run is rejected with
`DESTINATION_EXISTS` before anything is written. `$merge` is allowed to write into an existing
destination — that is its purpose — but only with `whenMatched` set to `"merge"` or `"replace"`;
other write modes (`"keepExisting"`, `"pipeline"`, arbitrary update pipelines) are rejected, to
keep the governed-write guarantee meaningful.

Every collection referenced anywhere in the pipeline — the collection being aggregated, and any
`$lookup.from` target — must be listed in `sources`.

## Policy fields

`NoSQLPolicy` (built from `db.query(...)`'s keyword arguments):

| Field | Default | Meaning |
|---|---|---|
| `read_only` | `True` | Reject any pipeline that classifies as a write. |
| `allowed_collections` | `frozenset()` | If non-empty, only these collections may be referenced. |
| `denied_collections` | `frozenset()` | These collections are always rejected. |
| `max_documents` | `1_000` | Upper bound on returned documents; excess is truncated and flagged. |
| `timeout_seconds` | `30` | Maximum wall-clock time for the aggregation. |
| `max_bytes_scanned` | `None` | Requires adapter support to enforce. |
| `max_cost_usd` | `None` | Requires adapter support to enforce. |

`MaterializationPolicy` (built from `db.materialize(...)`'s keyword arguments) additionally takes
`sources` and `destinations` glob patterns, and has no `read_only` field — materialization is
always a governed write.

## Supported pipeline stages

Reads: `$match, $project, $group, $sort, $limit, $unwind, $lookup`. Writes (materialize only):
`$out, $merge`. Any other stage fails classification at `validate()` and never reaches MongoDB.

## Capabilities

`NoSQLCapabilities` declares what the adapter actually enforces — `read_only_session`,
`document_limit`, `operation_timeout`, `bytes_scanned`, `cost_limit`, `out_merge_writes`,
`destination_introspection`, and more. A policy requirement with no matching capability fails
admission before the pipeline reaches MongoDB — see `gantry/nosql/enforcement.py`.

## Verification checks

`gantry.nosql.verify` mirrors `gantry.verify`'s SQL checks, adapted to Mongo's schemaless
documents:

- `destination_exists()` — the destination collection exists after materialization.
- `document_count(min=, max=)` — reads `CollectionSnapshot.metadata["document_count"]`.
- `required_fields([...])` — checks field presence within a **bounded sample** (default 100
  documents) of the destination. Fields absent from every sampled document read as missing even
  if present in unsampled documents — a documented limitation, not a bug.

## What is out of scope (v0)

- Any NoSQL engine other than MongoDB.
- `db.describe()` / schema discovery — collections are schemaless.
- Exhaustive (non-sampled) field presence guarantees.
- Drivers other than `pymongo.AsyncMongoClient` (no `motor`).

## Worked example

`examples/mongodb_rollup.py` is a runnable version of the query and materialize paths above,
against a local MongoDB started with `docker run mongo`. See
[examples/README.md](https://github.com/arvindram03/gantry/tree/main/examples) for setup.
