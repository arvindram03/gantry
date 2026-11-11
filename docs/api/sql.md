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

## Writes

`INSERT`, `UPDATE` and `DELETE` against tables you named, one statement at a
time, with a ceiling on how many rows it may change.

::: gantry.sql.SQLWrite

::: gantry.sql.WritePolicy

!!! note "The row ceiling is enforced by rolling back"
    The statement runs in a transaction, the engine reports how many rows it
    changed, and a count over `max_rows_affected` undoes it. An adapter that
    cannot do that refuses the operation — see `affected_row_limit` in the
    [capability matrix](capabilities.md).

## Materialization

Create-only: one `CREATE TABLE AS` or `CREATE VIEW AS`, to a schema-qualified
destination that does not already exist.

::: gantry.sql.SQLMaterializer

::: gantry.sql.MaterializationPolicy

::: gantry.sql.MaterializationResult

::: gantry.sql.MaterializationError

### Parsing a proposal

What turns proposed SQL into something checkable. Call these directly to
inspect what a statement would do before submitting it.

::: gantry.sql.parse_materialization

::: gantry.sql.MaterializationProposal

::: gantry.sql.MaterializationPlan

::: gantry.sql.MaterializationOperation

::: gantry.sql.TableRef

## Schema and classification

::: gantry.sql.DatabaseSchema

::: gantry.sql.Table

::: gantry.sql.Column

::: gantry.sql.SQLClassification

::: gantry.sql.SQLOperation

::: gantry.sql.SQLObjectRef

::: gantry.sql.ParsedSQL

::: gantry.sql.ExplainResult

## Extending

::: gantry.sql.register_provider

::: gantry.sql.providers

::: gantry.sql.SQLAdapter

::: gantry.sql.SQLCapabilities

::: gantry.sql.MaterializationAdapter

::: gantry.sql.MaterializationCapabilities

::: gantry.sql.SQLTarget

::: gantry.sql.register

### Dialects

A dialect decides how a submission is split and classified. It never rewrites
SQL — what the caller wrote is what the engine receives.

::: gantry.sql.SQLDialect

::: gantry.sql.ConservativeDialect

::: gantry.sql.MySQLDialect

::: gantry.sql.register_dialect
