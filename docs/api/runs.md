# Runs

Every governed operation produces a run: the durable record of who asked for
work, what was allowed, what executed, what was checked, and what Gantry
decided. It outlives the agent and the process that started it, which is what
makes it possible to answer those questions later.

```python
gantry.runs.configure(gantry.runs.SQLiteRunStore(".gantry/runs.db"))

with gantry.actor.context(actor=gantry.actor.actor("agent", "migration-agent")):
    run = await db.query(schemas=["analytics"], verify=[...])(sql)

run.id, run.status, run.rows

# …in another process, holding only the id
print(gantry.runs.get(run_id).render())
```

!!! note "Recorded before anything runs"
    The run is created before the proposal reaches the engine. If that first
    write fails, nothing is submitted — an engine job that exists without a
    record of why it was allowed to is the one outcome this ordering prevents.

::: gantry.Run

::: gantry.RunStatus

## Identity

::: gantry.runs.OperationKind

::: gantry.runs.OperationRef

::: gantry.ActorRef

::: gantry.ActorType

::: gantry.actor.actor

::: gantry.actor.context

## What a run records

::: gantry.runs.ProposalRecord

::: gantry.runs.ProposalStorage

::: gantry.runs.ResourceRef

::: gantry.runs.QueryResultRef

::: gantry.runs.AdmissionRecord

::: gantry.runs.ExecutionRecord

## Storage

::: gantry.runs.RunStore

::: gantry.runs.SQLiteRunStore

::: gantry.runs.MemoryRunStore

::: gantry.runs.RunPersistenceError

::: gantry.runs.get

::: gantry.runs.recent

::: gantry.runs.configure
