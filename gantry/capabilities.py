# SPDX-License-Identifier: Apache-2.0
"""Operational guarantees declared by execution adapters."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AdapterCapabilities:
    reconnect: bool = False
    cancellation: bool = False
    runtime_limit: bool = False
    cost_estimation: bool = False
    cost_limit: bool = False
    read_only_execution: bool = False
    write_execution: bool = False
    scoped_credentials: bool = False
    network_isolation: bool = False
    filesystem_isolation: bool = False
    ephemeral_environment: bool = False
    remote_status: bool = False
    metrics: bool = False
    result_reference: bool = False
