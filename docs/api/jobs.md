# Batch and stream jobs

Two entry points named for intent rather than for the engine behind them.
`gantry.batch` is for work that ends; `gantry.stream` is for work that does not,
where "did it succeed" is the wrong question and "is it healthy" is the right
one.

See [Flink backends](../flink.md) for what each guarantees, and for the setup a
real cluster needs.

## Batch

::: gantry.batch.connect

::: gantry.batch.BatchConnection

## Stream

::: gantry.stream.connect

::: gantry.stream.StreamConnection

## Verification checks

::: gantry.verify
    options:
      show_root_heading: false
      members_order: source
