# Gantry

Gantry is a governed execution boundary for agent-generated SQL and data jobs.
It decides whether a statement may run, bounds it while it runs, and decides
afterwards whether the result may be believed.

```bash
pip install data-gantry
```

The distribution is `data-gantry`; the import is `gantry`.

## Where to start

| | |
|---|---|
| [SQL](sql.md) | the governed query path, and the agent-facing tool |
| [Policies](policy.md) | what a policy can express, and who enforces it |
| [Materialization](materialization.md) | letting an agent build a table, and checking it |
| [Flink backends](flink.md) | batch and stream jobs, and what each guarantees |
| [Adapters](adapters.md) | the interfaces a new backend implements |
| [API reference](api/index.md) | every public symbol, generated from source |

Seven runnable examples live in
[`examples/`](https://github.com/arvindram03/gantry/tree/main/examples), indexed
by task. The quickest needs nothing but a Python environment.

## The idea in one paragraph

An engine answers "did the statement run". That is not the same question as "is
the answer usable" — a `CREATE TABLE AS` whose join matched nothing succeeds,
and a streaming job that has restarted forty times is `RUNNING`. Gantry answers
the second question, and refuses the statement outright when a policy says it
should never have been asked.
