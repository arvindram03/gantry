# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass

from gantry.capabilities import AdapterCapabilities
from gantry.policy import PolicyRequirements
from gantry.sql.policy import SQLPolicy


@dataclass(frozen=True, slots=True)
class SQLCapabilities:
    """What one SQL adapter can enforce, declared per provider.

    The source of truth behind the published
    [capability matrix](../api/capabilities.md): `core_capabilities()` projects
    these onto the engine-neutral `AdapterCapabilities` that admission checks,
    and `policy_requirements()` derives what a given `SQLPolicy` demands. Note
    `write_execution` defaults to true while everything else defaults to false —
    a new adapter is assumed able to write and assumed unable to bound.
    """

    describe_schema: bool = False
    explain: bool = False
    dry_run: bool = False
    async_jobs: bool = False
    reconnect: bool = False
    cancellation: bool = False
    read_only_session: bool = False
    write_execution: bool = True
    statement_timeout: bool = False
    row_limit: bool = False
    cost_estimate: bool = False
    cost_limit: bool = False
    bytes_scanned: bool = False
    query_metrics: bool = False
    result_reference: bool = False
    create_table_as: bool = False
    create_view_as: bool = False
    destination_introspection: bool = False
    materialization_reference: bool = False

    def core_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            reconnect=self.reconnect,
            cancellation=self.cancellation,
            runtime_limit=self.statement_timeout,
            cost_estimation=self.cost_estimate,
            cost_limit=self.cost_limit,
            read_only_execution=self.read_only_session,
            write_execution=self.write_execution,
            remote_status=self.async_jobs,
            metrics=self.query_metrics,
            result_reference=self.result_reference,
        )

    def policy_requirements(self, policy: SQLPolicy) -> PolicyRequirements:
        return PolicyRequirements(
            read_only=policy.read_only,
            allow_writes=not policy.read_only,
            max_runtime_seconds=policy.timeout_seconds,
            max_cost_usd=policy.max_cost_usd,
            require_reconnect=self.async_jobs,
            require_metrics=policy.max_bytes_scanned is not None,
            require_result_reference=False,
        )
