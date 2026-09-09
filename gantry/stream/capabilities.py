# SPDX-License-Identifier: Apache-2.0
"""Capabilities exposed by stream execution providers."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StreamCapabilities:
    insert_into: bool = False
    durable_job_id: bool = False
    cancel: bool = False
    health: bool = False
    metrics: bool = False
    watermark_metrics: bool = False
