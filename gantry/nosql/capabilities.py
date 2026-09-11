# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass

from gantry.capabilities import AdapterCapabilities
from gantry.nosql.policy import NoSQLPolicy
from gantry.policy import PolicyRequirements


@dataclass(frozen=True, slots=True)
class NoSQLCapabilities:
    reconnect: bool = False
    cancellation: bool = False
    read_only_session: bool = False
    write_execution: bool = True
    operation_timeout: bool = False
    document_limit: bool = False
    cost_estimate: bool = False
    cost_limit: bool = False
    bytes_scanned: bool = False
    query_metrics: bool = False
    result_reference: bool = False
    out_merge_writes: bool = False
    destination_introspection: bool = False
    materialization_reference: bool = False

    def core_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            reconnect=self.reconnect,
            cancellation=self.cancellation,
            runtime_limit=self.operation_timeout,
            cost_estimation=self.cost_estimate,
            cost_limit=self.cost_limit,
            read_only_execution=self.read_only_session,
            write_execution=self.write_execution,
            remote_status=self.reconnect,
            metrics=self.query_metrics,
            result_reference=self.result_reference,
        )

    def policy_requirements(self, policy: NoSQLPolicy) -> PolicyRequirements:
        return PolicyRequirements(
            read_only=policy.read_only,
            allow_writes=not policy.read_only,
            max_runtime_seconds=policy.timeout_seconds,
            max_cost_usd=policy.max_cost_usd,
            require_reconnect=False,
            require_metrics=policy.max_bytes_scanned is not None,
            require_result_reference=False,
        )
