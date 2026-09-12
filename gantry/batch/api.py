# SPDX-License-Identifier: Apache-2.0
"""Public batch execution surface."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence

from gantry.batch.capabilities import BatchCapabilities
from gantry.flink.api import FlinkRuntime, connect_runtime
from gantry.flink.operation import FlinkBatchJob
from gantry.flink.verification import FlinkHealthCheck
from gantry.policy.model import Policy
from gantry.verify import MaterializationCheck


class BatchConnection:
    """A configured batch provider connection."""

    def __init__(self, provider: str, runtime: FlinkRuntime) -> None:
        self._provider = provider
        self._runtime = runtime

    @property
    def provider(self) -> str:
        return self._provider

    def capabilities(self) -> BatchCapabilities:
        capabilities = self._runtime.capabilities()
        return BatchCapabilities(
            insert_into=capabilities.write_execution,
            insert_overwrite=capabilities.write_execution,
            durable_job_id=capabilities.reconnect,
            cancel=capabilities.cancellation,
            metrics=capabilities.metrics,
            output_verification=capabilities.result_reference,
        )

    def job(
        self,
        *,
        inputs: Collection[str],
        outputs: Collection[str],
        checks: Sequence[FlinkHealthCheck | MaterializationCheck] = (),
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> FlinkBatchJob:
        return FlinkBatchJob(
            self._runtime,
            inputs=inputs,
            outputs=outputs,
            checks=checks,
            timeout=timeout,
            poll_interval=poll_interval,
        )


def connect(
    provider: str,
    *,
    endpoint: str,
    config: Mapping[str, object] | None = None,
    policy: Policy | None = None,
    **options: object,
) -> BatchConnection:
    normalized = provider.strip().lower()
    if normalized != "flink":
        raise ValueError(f"unsupported batch provider: {provider}")
    return BatchConnection(
        normalized, connect_runtime(endpoint, config=config, policy=policy, **options)
    )
