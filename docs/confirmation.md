# Confirmation

Policy answers whether an operation may happen. Confirmation answers a different
question:

> Should the host ask the user before it happens?

Some work is valid according to policy and still deserves explicit awareness — a
write to production, a replaced table, an expensive warehouse query, a streaming
job that will keep running. Gantry signals *ask before proceeding*; the host
decides how to ask.

## What v0 claims, and what it does not

> **Gantry Confirmation v0 is not an authentication or authorization mechanism.**

Gantry does not claim:

```text
a trusted human approved this
```

It claims only:

```text
this operation was marked as requiring confirmation,
and the host application supplied confirmation before execution
```

That is why this is called confirmation rather than approval, and why no field in
the record names a person. `approval` implies guarantees around identity, roles,
auditability and separation of duties that v0 does not provide. An authenticated
approval system can later back the same control point without changing the
lifecycle around it.

## Asking

Confirmation is expressed on the rules that already grant authority:

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

db = gantry.sql.connect("postgres", url=DATABASE_URL, policy=policy)
```

The decision stays `allowed=True`. Nothing about it is a refusal:

```text
POLICY_ALLOWED
CONFIRMATION_REQUIRED
```

A rule that names a source and a destination asks about that pairing. A rule for
`raw.* -> prod.*` raises no prompt on a `raw.* -> scratch.*` run, even though it
recognises the source.

### Reason codes

| Code | For |
| --- | --- |
| `SENSITIVE_DESTINATION` | a destination the policy treats as sensitive |
| `PRODUCTION_WRITE` | a write to production data |
| `DESTRUCTIVE_OPERATION` | replacing or removing existing data |
| `HIGH_COST` | work that may be expensive |
| `LONG_RUNNING_JOB` | a job that keeps running after acceptance |
| `CUSTOM_POLICY_REQUIREMENT` | anything else; the default when no code is given |

A `confirmation_message` or `confirmation_code` without `require_confirmation=True`
is a configuration error, not a preference — a prompt nobody will ever see is a
mistake worth catching where it is written.

## Waiting

A parked run is durable, allowed, and has touched nothing:

```text
Run run_01M2…

Admission
  ✓ allowed
  policy: prod-data-agents
    ✓ read raw.invoices
    ✗ write prod.totals

Confirmation
  ! awaiting user confirmation
    PRODUCTION_WRITE: This will write to production data.

Decision
  AWAITING_CONFIRMATION
```

`AWAITING_CONFIRMATION` is not terminal and not a refusal. No engine has been
asked to do anything: no connection opened, no job submitted, no table created.

## Answering

The host confirms or declines on a **separate path** from the one the agent
proposes on:

```python
run = await materialize(sql)

if run.status is gantry.RunStatus.AWAITING_CONFIRMATION:
    answer = input(f"{run.confirmation.message} Continue? [y/N] ")
    if answer.lower() == "y":
        run = await gantry.runs.confirm(run.id, metadata={"channel": "cli"})
    else:
        run = await gantry.runs.decline(run.id)
```

Confirming resumes the same run, with the same immutable proposal, and execution
and verification proceed exactly as they would have. Declining makes the run
terminal at `CONFIRMATION_DECLINED` with nothing executed.

`metadata` is for how the host asked — `{"channel": "chat"}` — not for who
answered. Gantry has no way to verify a name, so it does not record one.

The separation is the point: the agent can discover that confirmation is
required, and has no capability to supply it.

```text
proposal path != confirmation path
```

## What the agent sees

`.tool()` reports the requirement and its reasons, and exposes no way to satisfy
them:

```json
{
  "run_id": "run_01M2…",
  "status": "AWAITING_CONFIRMATION",
  "confirmation": {
    "status": "required",
    "reasons": [
      {"code": "PRODUCTION_WRITE", "message": "This will write to production data."}
    ]
  }
}
```

The tool schema still carries only `sql` and `verify`. A tool call that tries to
pass `confirmed` is refused rather than ignored — an ignored field would look to
the agent like it had worked.

## Ordering

```text
policy
  |
  +-- denied ------------------------> POLICY_REJECTED        (nothing to confirm)
  |
  +-- allowed
        |
        +-- no confirmation ---------> execute
        |
        +-- confirmation required
                  |
            AWAITING_CONFIRMATION
                  |
            host answers
             /          \
        confirm        decline
           |              |
        execute     CONFIRMATION_DECLINED
           |
        verify
```

Two consequences worth stating plainly:

- **Confirmation cannot override policy.** A denial never parks, so there is
  nothing for a host to say yes to. `POLICY_REJECTED + confirmed` is not a state
  that exists.
- **Confirmation cannot weaken verification.** A user saying yes is not a
  measurement. Execution can still succeed and the run still be `REJECTED`
  because a check failed.

## Idempotency and concurrency

Confirming twice does not execute twice. The transition out of
`AWAITING_CONFIRMATION` is a compare-and-set in the run store, so of any number
of repeated or concurrent confirmations exactly one releases the work and the
rest return the run as it now is. Declining something already confirmed — or
confirming something already declined — is refused rather than answered quietly.

## Limits of v0

- A parked run resumes **in the process that proposed it**. The run record is
  durable and readable anywhere; the work waiting on it is not. Confirming from
  another process raises rather than pretending to resume.
- Each process keeps a bounded number of runs resumable at once
  (`gantry.runs.service.MAX_PENDING`). A run nobody ever answers stays in the
  store and stays readable, but eventually stops being resumable rather than
  pinning its proposal in memory forever.
- A custom `RunStore` needs `compare_and_set` to back confirmation. One without
  it raises a clear error instead of making a weaker guarantee quietly.
