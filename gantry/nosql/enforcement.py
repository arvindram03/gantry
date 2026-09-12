# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.pipeline import PipelineClassification
from gantry.nosql.policy import NoSQLPolicy


def policy_errors(
    classification: PipelineClassification,
    policy: NoSQLPolicy,
    capabilities: NoSQLCapabilities,
) -> tuple[str, ...]:
    errors: list[str] = []
    if policy.read_only and not classification.read_only:
        errors.append(f"{classification.operation.value} is not allowed by read-only policy")
    if policy.read_only and not capabilities.read_only_session:
        errors.append("adapter cannot enforce a read-only session")
    if policy.max_documents and not capabilities.document_limit:
        errors.append("adapter cannot enforce the document limit")
    if policy.timeout_seconds and not (
        capabilities.operation_timeout or (capabilities.reconnect and capabilities.cancellation)
    ):
        errors.append("adapter cannot enforce or monitor the operation timeout")

    for reference in classification.collections:
        name = reference.name.lower()
        if policy.allowed_collections and name not in policy.allowed_collections:
            errors.append(f"collection is not allowed: {name}")
        if name in policy.denied_collections:
            errors.append(f"collection is denied: {name}")

    if policy.max_bytes_scanned is not None and not capabilities.bytes_scanned:
        errors.append("adapter cannot enforce maximum bytes scanned")
    if policy.max_cost_usd is not None and not capabilities.cost_limit:
        errors.append("adapter cannot enforce maximum cost")
    return tuple(dict.fromkeys(errors))
