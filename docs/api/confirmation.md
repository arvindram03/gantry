# Confirmation

Some operations are allowed and still deserve a question first.

> **Gantry Confirmation v0 is not an authentication or authorization mechanism.**

Policy handles authorization. Confirmation records that the host application
indicated user consent before execution — nothing more. Gantry never claims that
a particular authenticated person approved anything, which is why there is no
`approved_by` field anywhere in this model.

```python
policy = gantry.Policy(
    name="prod-data-agents",
    rules=[
        gantry.allow.materialize(
            sources=["raw.*"],
            destinations=["prod.*"],
            require_confirmation=True,
            confirmation_code="PRODUCTION_WRITE",
            confirmation_message="This will write to production data.",
        ),
    ],
)

run = await materialize(sql)

if run.status is gantry.RunStatus.AWAITING_CONFIRMATION:
    print(run.confirmation.message)
    run = await gantry.runs.confirm(run.id)  # or gantry.runs.decline(run.id)
```

!!! note "Nothing has happened while a run is parked"
    `AWAITING_CONFIRMATION` is neither a refusal nor an execution. The run is
    durable, the policy decision is recorded, and no engine has been asked to do
    anything. It resumes as the same run — the same immutable proposal — if the
    host confirms.

## Host-side control

::: gantry.runs.confirm

::: gantry.runs.decline

::: gantry.runs.awaiting

::: gantry.runs.service.ConfirmationError

## What was asked

::: gantry.ConfirmationRequirement

::: gantry.ConfirmationReason

::: gantry.ConfirmationReasonCode

## What was answered

::: gantry.ConfirmationRecord

::: gantry.ConfirmationStatus

## The transition

::: gantry.runs.compare_and_set
