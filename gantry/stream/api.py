# SPDX-License-Identifier: Apache-2.0
"""Public streaming execution surface."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence

from gantry.flink.api import FlinkRuntime, connect_runtime
from gantry.flink.operation import FlinkStreamJob
from gantry.flink.verification import FlinkHealthCheck
from gantry.policy.model import Policy
from gantry.stream.capabilities import StreamCapabilities


class StreamConnection:
    """A configured streaming provider connection."""

    def __init__(self, provider: str, runtime: FlinkRuntime) -> None:
        self._provider = provider
        self._runtime = runtime

    @property
    def provider(self) -> str:
        return self._provider

    def capabilities(self) -> StreamCapabilities:
        capabilities = self._runtime.capabilities()
        return StreamCapabilities(
            insert_into=capabilities.write_execution,
            durable_job_id=capabilities.reconnect,
            cancel=capabilities.cancellation,
            health=capabilities.remote_status,
            metrics=capabilities.metrics,
            watermark_metrics=capabilities.metrics,
        )

    def job(
        self,
        *,
        inputs: Collection[str],
        outputs: Collection[str],
        checks: Sequence[FlinkHealthCheck] = (),
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> FlinkStreamJob:
        return FlinkStreamJob(
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
) -> StreamConnection:
    normalized = provider.strip().lower()
    if normalized != "flink":
        raise ValueError(f"unsupported stream provider: {provider}")
    return StreamConnection(
        normalized, connect_runtime(endpoint, config=config, policy=policy, **options)
    )
