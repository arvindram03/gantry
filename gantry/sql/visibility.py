# SPDX-License-Identifier: Apache-2.0
"""Narrowing a described schema to what may actually be read.

`describe()` asks the database what the credential can see, which is a wider
question than what the caller has decided an agent may read. Handing the first
answer to a model leaks the second: the names, the columns and the existence of
every table someone deliberately denied — and a denied table is usually denied
because its *shape* is the sensitive part.

So the schema is filtered here, by the same two layers that decide a query: the
operation's own bounds, and the reusable policy. A table an agent could not
read is a table it is not shown.
"""

from __future__ import annotations

from gantry.actor import current_actor
from gantry.policy.evaluator import evaluate
from gantry.policy.model import Policy
from gantry.policy.request import PolicyRequest
from gantry.runs.model import OperationKind, ResourceRef
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import DatabaseSchema, Table


def qualified(table: Table) -> str:
    """The name policy uses for this table: `schema.table`, or just the table."""
    return f"{table.schema}.{table.name}" if table.schema else table.name


def _within_bounds(table: Table, policy: SQLPolicy) -> bool:
    """Whether the operation's own allow-list and deny-list admit this table."""
    name = qualified(table).lower()
    schema = (table.schema or "").lower()

    if policy.denied_tables and name in {denied.lower() for denied in policy.denied_tables}:
        return False
    if policy.allowed_schemas and schema not in {
        allowed.lower() for allowed in policy.allowed_schemas
    }:
        return False
    return not (
        policy.allowed_tables and name not in {allowed.lower() for allowed in policy.allowed_tables}
    )


def _admitted(table: Table, provider: str, policy: Policy, bounds: SQLPolicy) -> bool:
    """Whether the reusable policy would admit reading this table.

    Asked as the question it is — a query whose only input is this table — so
    the answer comes from the same evaluator that decides the real thing rather
    than from a second reading of the rules.
    """
    request = PolicyRequest(
        actor=current_actor(),
        operation=OperationKind.QUERY,
        engine=provider,
        inputs=(ResourceRef(system=provider, resource=qualified(table)),),
        constraints={"max_rows": float(bounds.max_rows)},
    )
    return evaluate(policy, request).allowed


def visible(
    schema: DatabaseSchema,
    *,
    provider: str,
    bounds: SQLPolicy | None = None,
    policy: Policy | None = None,
) -> DatabaseSchema:
    """The part of `schema` an agent under these rules may read.

    Both layers narrow and neither widens, which is the same composition the
    engine applies to a query: a table has to pass the operation's bounds *and*
    the reusable policy to be shown.
    """
    if bounds is None and policy is None:
        return schema

    tables = tuple(
        table
        for table in schema.tables
        if (bounds is None or _within_bounds(table, bounds))
        and (policy is None or _admitted(table, provider, policy, bounds or SQLPolicy()))
    )
    kept = {table.schema for table in tables if table.schema}
    return DatabaseSchema(
        catalogs=schema.catalogs,
        # A schema with nothing visible in it is not named either: the list of
        # schemas is itself a description of what exists.
        schemas=tuple(name for name in schema.schemas if name in kept),
        tables=tables,
        metadata=schema.metadata,
    )
