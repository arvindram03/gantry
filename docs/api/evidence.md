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

## The bundle

::: gantry.EvidenceBundle

!!! note "References, not data"
    An observation is a name, a scalar and a source, so a bundle stays small
    however large the output it describes. The row count of a billion-row table
    is one integer. Gantry is not the data plane, and evidence is not a copy of
    the data.

## Durable runs

Evidence that lives only in the calling process answers nothing later. The run
store keeps it past the process, and past the agent conversation.

```python
gantry.runs.configure(gantry.runs.SQLiteRunStore(".gantry/runs.db"))

result = await build(sql)
gantry.runs.record(result.evidence)

# …in another process, holding only the id
run = gantry.runs.get(run_id)
print(run.render())
```

::: gantry.runs.RunRecord

::: gantry.runs.RunStore

::: gantry.runs.SQLiteRunStore

::: gantry.runs.MemoryRunStore

::: gantry.runs.record

::: gantry.runs.get

::: gantry.runs.recent

::: gantry.runs.configure
