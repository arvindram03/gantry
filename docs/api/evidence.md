# Evidence and runs

A result says a run was accepted. Evidence says why — in enough detail that
someone who was not there, and who does not have the agent conversation that
produced the work, can decide whether they agree.

## Observations

Three sources, kept apart because they are trusted differently. The engine's
account of its own execution is not the same kind of fact as a measurement
Gantry took at the destination.

::: gantry.Observation

::: gantry.ObservationSource

::: gantry.CheckSource

::: gantry.VerificationSource

## The bundle

::: gantry.EvidenceBundle

!!! note "References, not data"
    An observation is a name, a scalar and a source, so a bundle stays small
    however large the output it describes. The row count of a billion-row table
    is one integer. Gantry is not the data plane, and evidence is not a copy of
    the data.

## The same checks on both paths

`gantry.verify` serves a query and a materialization. A materialization is
checked against the destination it created; a query against the rows it
returned, described as a table so one check means one thing.

```python
checks = [gantry.verify.row_count(min=1), gantry.verify.required_columns(["id"])]

await db.query(schemas=["analytics"], checks=checks)(sql)
await db.materialize(sources=["analytics.*"], destinations=["reporting.*"], checks=checks)(sql)
```

Configured `checks=` are trusted application commitments. A caller may add
task-specific checks at invocation time; the merge is additive and Gantry
assigns provenance itself:

```python
query = db.query(
    schemas=["analytics"],
    checks=[gantry.verify.row_count(max=1000)],
)

result = await query(
    sql,
    verify=[
        gantry.verify.not_empty(),
        gantry.verify.required_columns(["customer_id", "revenue"]),
    ],
)
```

`query.tool()` exposes `verify` as an allowlisted declarative JSON schema. An
unsupported check, attempted `source` override, or
executable callback is rejected. Statically contradictory count bounds return
`VERIFICATION_CONFLICT` before the engine is started.

Two cases cannot be the same, and both fail closed rather than pretending:

- `destination_exists` and `output_exists` ask about something a query never
  creates, so on a query they report themselves unsupported. Answering them
  against the result set would make them trivially true, and a caller would
  believe a destination had been checked.
- A result truncated by `max_rows` describes the rows returned, not the rows
  matched, so the count-based checks are unsupported there. Counting what came
  back would measure the policy rather than the data.

Both arrive as `FailureKind.UNSUPPORTED_VERIFICATION`, which is distinct from a
check that ran and failed.

## Durable runs

Evidence is attached to a run, which is the durable record of the whole
operation. See [Runs](runs.md).
