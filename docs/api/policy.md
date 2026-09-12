# Policy

Policy answers one question, before anything external happens:

> May this actor perform this proposed operation on these resources?

It is trusted configuration. The application writes it; the agent proposes work
against it and can neither choose it, weaken it, nor name itself as someone
else.

```python
policy = gantry.Policy(
    name="data-agents",
    rules=[
        gantry.allow.query(sources=["analytics.*"]),
        gantry.allow.materialize(sources=["raw.*"], destinations=["agent_scratch.*"]),
        gantry.deny.materialize(destinations=["prod.*"]),
    ],
)

db = gantry.sql.connect("postgres", url=DATABASE_URL, policy=policy)

with gantry.actor.context(actor=gantry.actor.actor("agent", "etl-agent"), environment="prod"):
    run = await db.query(schemas=["analytics"])(sql)

run.admission.policy        # "data-agents"
run.admission.policy_hash   # "sha256:…"
run.admission.matched_rules # ("allow-query-0",)
```

!!! note "Policy decides before the engine is asked"
    A refused proposal is never submitted. The run exists — it is created before
    admission — and carries the decision, with no execution attached to it.

## Defining a policy

::: gantry.Policy

::: gantry.PolicyRule

### Allow rules

Rules that grant authority. Omitting `sources` allows any source to be read;
omitting `destinations` grants no write authority at all, so the writing
operations require them.

::: gantry.policy.allow.query

::: gantry.policy.allow.materialize

::: gantry.policy.allow.batch

::: gantry.policy.allow.stream

### Deny rules

A matching deny wins over every allow.

::: gantry.policy.deny.query

::: gantry.policy.deny.materialize

::: gantry.policy.deny.batch

::: gantry.policy.deny.stream

::: gantry.policy.deny.anything

## What policy decides on

::: gantry.PolicyRequest

## What it returns

::: gantry.PolicyDecision

::: gantry.PolicyReason

::: gantry.PolicyReasonCode

## When a policy cannot mean anything

::: gantry.PolicyConfigurationError

## Resource patterns

::: gantry.policy.normalize_pattern

::: gantry.policy.matches
