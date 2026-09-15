# Failures

Every adapter's errors are normalized into one taxonomy, so calling code can
decide what to do without knowing which engine produced the problem.

```python
if not run.ok and run.failure is not None:
    run.failure.kind  # FailureKind.TIMEOUT — portable
    run.failure.retryable  # the condition may pass on its own
    run.safe_to_retry  # …and running this proposal again is harmless
    run.failure.native_code  # what the engine itself said
```

## Gantry never retries

That is a deliberate boundary, not a missing feature. A retried write is a
second attempt at something policy admitted once, and the layer whose job is to
be the durable record of what ran is the wrong place to quietly run things
twice. The flags below are advice for your code; nothing in Gantry acts on them.

## `retryable` says one narrow thing

**The condition that caused this failure may pass on its own.** That is all. It
is derived from the kind — `FailureKind.transient` is the single rule every
adapter follows, and a test enforces it across the library so a timeout means
the same thing on PostgreSQL as on BigQuery.

| Transient | Not transient |
| --- | --- |
| `TIMEOUT`, `RESOURCE_ERROR`, `RESOURCE_EXHAUSTED` | everything else |

Three kinds are left out on purpose. `CONNECTOR_ERROR` is a broker that is down
*or* a sink table that does not exist, and the message rarely says which.
`ENGINE_ERROR` and `UNKNOWN` mean Gantry could not tell what went wrong — and
advice to retry on that basis has nothing behind it.

## `safe_to_retry` answers the half that can destroy something

A transient condition does not make the work repeatable. Whether you may run the
same proposal again depends on what it was and how far it got:

| Operation | Safe to run again |
| --- | --- |
| **query** | Always, on a transient failure. Reads are idempotent. |
| **materialize** | Only if nothing reached the engine. |
| **batch**, **stream** | Only if nothing was submitted. |

Materialization is create-only by design, and that is exactly what makes the
retry unsafe: a run that reached the engine may have left its destination behind
even though it failed, so running it again does not succeed — it comes back
`DESTINATION_EXISTS`. A caller who saw `retryable=True` and simply tried again
would turn a transient failure into one that looks permanent.

Submitted jobs are worse. Re-submitting a Flink job does not replace the first
one, it adds a second.

```python
run = await materialize(sql)

if run.safe_to_retry:
    run = await materialize(sql)  # nothing was created; this is a fresh attempt
elif run.failure and run.failure.retryable:
    ...  # transient, but the destination may exist —
    # check it, or write to a new destination
```

`safe_to_retry` returning `False` is not a prediction that the retry will fail.
It is Gantry declining to promise the retry is harmless, which for a write is
the answer that matters.

## Transport-level retries

A dropped connection during `describe()` is unambiguously safe to retry and
invisible to policy, so adapter-level retries for pure metadata reads would be
reasonable. None are implemented today, and nothing that touches a governed
operation will be retried this way — the distinction is whether policy admitted
the call, not whether it looks cheap.

::: gantry.Failure

::: gantry.FailureKind

::: gantry.SubmissionError
