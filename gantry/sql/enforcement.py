# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.classification import SQLClassification, SQLOperation
from gantry.sql.explain import ExplainResult
from gantry.sql.policy import SQLPolicy


def policy_errors(
    classification: SQLClassification,
    policy: SQLPolicy,
    capabilities: SQLCapabilities,
    explain: ExplainResult | None = None,
) -> tuple[str, ...]:
    errors: list[str] = []
    if classification.statement_count != 1 and not policy.allow_multiple_statements:
        errors.append("multiple SQL statements are not allowed")
    if policy.read_only and not classification.read_only:
        # A SELECT that is not a read has to say why, or the refusal reads as a
        # bug: "SELECT is not allowed by read-only policy" tells an operator
        # nothing about the FOR UPDATE clause that caused it.
        errors.append(
            f"{classification.operation.value} is not allowed by read-only policy"
            if classification.read_only_reason is None
            else f"{classification.operation.value} is not read-only: "
            f"{classification.read_only_reason}"
        )
    if policy.read_only and not capabilities.read_only_session:
        errors.append("adapter cannot enforce a read-only session")
    if classification.operation is SQLOperation.UNKNOWN:
        errors.append("SQL operation could not be classified safely")
    if policy.max_rows and not capabilities.row_limit:
        errors.append("adapter cannot enforce the row limit")
    if policy.timeout_seconds and not (
        capabilities.statement_timeout or (capabilities.reconnect and capabilities.cancellation)
    ):
        errors.append("adapter cannot enforce or monitor the statement timeout")

    for reference in classification.tables:
        qualified = reference.qualified_name.lower()
        name = reference.name.lower()
        if policy.allowed_schemas and (
            reference.schema is None or reference.schema.lower() not in policy.allowed_schemas
        ):
            errors.append(f"schema is not allowed for table: {qualified}")
        if (
            policy.allowed_tables
            and name not in policy.allowed_tables
            and qualified not in policy.allowed_tables
        ):
            errors.append(f"table is not allowed: {qualified}")
        if name in policy.denied_tables or qualified in policy.denied_tables:
            errors.append(f"table is denied: {qualified}")

    if policy.max_bytes_scanned is not None:
        if not capabilities.bytes_scanned or explain is None or explain.estimated_bytes is None:
            errors.append("adapter cannot enforce maximum bytes scanned")
        elif explain.estimated_bytes > policy.max_bytes_scanned:
            errors.append(
                f"estimated bytes {explain.estimated_bytes} exceed limit {policy.max_bytes_scanned}"
            )
    if policy.max_cost_usd is not None:
        if not capabilities.cost_limit or explain is None or explain.estimated_cost is None:
            errors.append("adapter cannot enforce maximum cost")
        elif explain.estimated_cost > policy.max_cost_usd:
            errors.append(
                f"estimated cost {explain.estimated_cost} exceeds limit {policy.max_cost_usd}"
            )
    return tuple(dict.fromkeys(errors))
