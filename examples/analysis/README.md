# Analysis: the guarantee boundary

The scenario from the design document, runnable. A deploy at 12:00 makes
checkout slower; the Analysis attributes each request to the deploy that was
live when it happened and reports what changed.

The specs are the canonical ones under [`spec/examples/`](../../spec/examples)
rather than copies — a copied spec is one that drifts:

| File | What it declares |
|---|---|
| [`dataset-request-logs.yaml`](../../spec/examples/dataset-request-logs.yaml) | that `event_time` is what orders this Dataset |
| [`dataset-deploy-events.yaml`](../../spec/examples/dataset-deploy-events.yaml) | the same, for deploys |
| [`analysis-checkout-regression.yaml`](../../spec/examples/analysis-checkout-regression.yaml) | inputs, window, normalisation, temporal join, signals, verification |

## Run it

```bash
make dev-up
uv run alembic upgrade head
uv run gantry seed --scenario checkout
make demo
```

`make demo` runs the Analysis twice against the same data on the same engine.
The only difference is whether the join keeps its temporal qualifier:

```text
well-formed  (temporal join: nearest_preceding)
  ENGINE  SUCCESS (2 rows returned)
  GANTRY  PASSED  row_expansion 1.0000x (allowed <= 1.1x)
  RESULT  4 findings published

expanding    (same SQL, no temporal qualifier)
  ENGINE  SUCCESS (2 rows returned)
  GANTRY  FAILED  row_expansion 2.0000x (allowed <= 1.1x)
          the join expanded 40,000 rows to 80,000 (2.00x)
  RESULT  verification_failed, findings withheld
```

PostgreSQL executed both without complaint — it did exactly what it was asked.
Whether the numbers mean anything is a different question, and that is the one
Gantry answers.

## Then ask why you should believe it

```bash
uv run gantry results get checkout-regression.analysis
uv run gantry results explain checkout-regression.analysis
uv run gantry results provenance checkout-regression.analysis
uv run gantry results refresh checkout-regression.analysis
```

`provenance` walks finding → Result → artifact → the exact Dataset versions
read → the Movement that produced them → the checkpoints that Movement had
reached. Any link it cannot resolve is named, not omitted.

## What the spec is doing

**`normalize`** reconciles the two sources' names for one field: requests call
it `svc`, deploys call it `service_name`. Without that, nothing joins.

**`temporal: {strategy: nearest_preceding}`** is what makes the join mean
something. A plain time comparison matches every deploy in range and multiplies
the left side; this takes exactly the one that was live.

**`signals`** are named, not written as expressions. A spec that accepted
arbitrary SQL would make Gantry a query language, and engines already have one.

**`verify`** is the part that decides whether the Result may be believed —
`rowExpansion` catches the join multiplying its input, `nullRate` catches a
grouping field being absent often enough that the groups are not the ones
named.
