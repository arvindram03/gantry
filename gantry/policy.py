# SPDX-License-Identifier: Apache-2.0
"""Portable execution requirements produced by an application policy."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PolicyRequirements:
    read_only: bool = False
    allow_writes: bool = False
    max_runtime_seconds: float | None = None
    max_cost_usd: float | None = None
    require_cancel: bool = False
    require_reconnect: bool = False
    require_scoped_credentials: bool = False
    require_network_isolation: bool = False
    require_filesystem_isolation: bool = False
    require_ephemeral_environment: bool = False
    require_metrics: bool = False
    require_result_reference: bool = False

    def __post_init__(self) -> None:
        if self.read_only and self.allow_writes:
            raise ValueError("policy cannot require read-only execution and allow writes")
        if self.max_runtime_seconds is not None and self.max_runtime_seconds <= 0:
            raise ValueError("max runtime must be positive")
        if self.max_cost_usd is not None and self.max_cost_usd < 0:
            raise ValueError("max cost must not be negative")
