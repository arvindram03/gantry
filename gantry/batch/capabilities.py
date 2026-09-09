# SPDX-License-Identifier: Apache-2.0
"""Capabilities exposed by batch execution providers."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BatchCapabilities:
    insert_into: bool = False
    insert_overwrite: bool = False
    durable_job_id: bool = False
    cancel: bool = False
    metrics: bool = False
    output_verification: bool = False
