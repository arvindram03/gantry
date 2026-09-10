# SQL

The path most callers use: connect once, configure a policy once, hand the agent
a tool.

## Connecting

::: gantry.sql.connect

::: gantry.sql.SQLConnection

## Configuring an operation

::: gantry.sql.SQLQuery

::: gantry.sql.SQLPolicy

!!! note "A policy is a claim about what the adapter will enforce"
    Declaring a field is not the same as it being enforced. `read_only=True` is
    admitted only when the adapter can hold a read-only session in the engine;
    a row limit is admitted only when the adapter can bound the result. See the
    [capability matrix](capabilities.md) for which provider enforces what, and
    what happens when it cannot.

## Materialization

::: gantry.sql.SQLMaterializer

::: gantry.sql.MaterializationPolicy

::: gantry.sql.MaterializationResult

## Schema and classification

::: gantry.sql.DatabaseSchema

::: gantry.sql.Table

::: gantry.sql.Column

::: gantry.sql.SQLClassification

::: gantry.sql.SQLOperation

::: gantry.sql.ExplainResult

## Extending

::: gantry.sql.register_provider

::: gantry.sql.providers

::: gantry.sql.SQLAdapter

::: gantry.sql.SQLCapabilities
