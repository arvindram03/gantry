# SPDX-License-Identifier: Apache-2.0
"""Operational guarantees declared by execution adapters."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AdapterCapabilities:
    """What an adapter can actually enforce, declared rather than assumed.

    Admission compares this against `PolicyRequirements`: a requirement with no
    matching capability is refused, because a bound the engine never applies is
    worse than no bound. Every field defaults to false, so a new adapter is
    trusted with nothing until it says otherwise.
    """

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
