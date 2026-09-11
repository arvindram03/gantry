# SPDX-License-Identifier: Apache-2.0
"""Small public surface for governed NoSQL connections."""

from __future__ import annotations

from collections.abc import Collection, Sequence

from gantry.artifact import Artifact
from gantry.context import Context
from gantry.execution import Execution
from gantry.handle import ExecutionHandle
from gantry.nosql.adapter import NoSQLAdapter
from gantry.nosql.bridge import NoSQLExecutionAdapter
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.materialization import (
    MaterializationAdapter,
    MaterializationPolicy,
    NoSQLMaterializer,
)
from gantry.nosql.output import InlineDocuments
from gantry.nosql.pipeline import CollectionRef, Pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.query import NoSQLQuery
from gantry.nosql.registry import resolve_provider
from gantry.nosql.result import NoSQLResult
from gantry.nosql.target import NoSQLTarget
from gantry.nosql.verify import CollectionSnapshot, DocumentCheck
from gantry.output import OutputKind, OutputRef
from gantry.result import Result
from gantry.runtime import ControlPlane
from gantry.target import ExecutionTarget


class NoSQLConnection:
    """A provider-neutral, governed MongoDB connection.

    Configuration and the native adapter remain private so an agent tool
    cannot accidentally expose credentials or a raw database handle.
    """

    def __init__(self, target: NoSQLTarget, adapter: NoSQLAdapter) -> None:
        self._target = target
        self._adapter = adapter
        self._plane = ControlPlane()

    @property
    def provider(self) -> str:
        return self._target.provider

    def capabilities(self) -> NoSQLCapabilities:
        return self._adapter.capabilities()

    def query(
        self,
        *,
        read_only: bool = True,
        collections: Collection[str] = (),
        denied_collections: Collection[str] = (),
        max_documents: int = 1_000,
        timeout: float = 30,
        max_bytes_scanned: int | None = None,
        max_cost_usd: float | None = None,
        verify: Sequence[DocumentCheck] = (),
    ) -> NoSQLQuery:
        """Configure a governed query operation."""

        policy = NoSQLPolicy(
            read_only=read_only,
            allowed_collections=collections,
            denied_collections=denied_collections,
            max_documents=max_documents,
            timeout_seconds=timeout,
            max_bytes_scanned=max_bytes_scanned,
            max_cost_usd=max_cost_usd,
        )
        return NoSQLQuery(self, policy, tuple(verify))

    def materialize(
        self,
        *,
        sources: Collection[str] = (),
        destinations: Collection[str],
        timeout: float = 300,
        max_bytes_scanned: int | None = None,
        max_cost_usd: float | None = None,
        verify: Sequence[DocumentCheck] = (),
    ) -> NoSQLMaterializer:
        """Configure a governed, create-only MongoDB materialization operation."""

        policy = MaterializationPolicy(
            sources=sources,
            destinations=destinations,
            timeout_seconds=timeout,
            max_bytes_scanned=max_bytes_scanned,
            max_cost_usd=max_cost_usd,
        )
        return NoSQLMaterializer(self, policy, tuple(verify))

    async def _query(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy,
        context: Context | None = None,
        verify: Sequence[DocumentCheck] = (),
    ) -> NoSQLResult:
        """Execute a pipeline for a configured query operation."""

        return await self.execute(collection, pipeline, policy=policy, context=context)

    async def submit(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy | None = None,
        context: Context | None = None,
    ) -> ExecutionHandle:
        active_policy = policy or NoSQLPolicy()
        bridge = self._install_bridge(active_policy)
        nosql_context = _with_collection(context, collection)
        return await self._plane.submit(
            Artifact(pipeline, "nosql"),
            target=self._execution_target(),
            context=nosql_context,
            policy=bridge.adapter.capabilities().policy_requirements(active_policy),
        )

    async def execute(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy | None = None,
        context: Context | None = None,
        verify: Sequence[DocumentCheck] = (),
        poll_interval_seconds: float = 0.05,
    ) -> NoSQLResult:
        active_policy = policy or NoSQLPolicy()
        bridge = self._install_bridge(active_policy)
        nosql_context = _with_collection(context, collection)
        result = await self._plane.run(
            Artifact(pipeline, "nosql"),
            target=self._execution_target(),
            context=nosql_context,
            policy=bridge.adapter.capabilities().policy_requirements(active_policy),
            poll_interval_seconds=poll_interval_seconds,
        )
        return _from_engine_result(result, active_policy.max_documents)

    async def status(self, handle: ExecutionHandle) -> Execution:
        return await self._adapter.status(handle)

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> Execution:
        return await self._adapter.cancel(handle, mode)

    async def wait(
        self,
        handle: ExecutionHandle,
        *,
        poll_interval_seconds: float = 0.05,
    ) -> NoSQLResult:
        """Observe a submitted job and recover its result."""

        result = await self._plane.wait(handle, poll_interval_seconds=poll_interval_seconds)
        max_documents = handle.metadata.get("max_documents", 1_000)
        return _from_engine_result(
            result, max_documents if isinstance(max_documents, int) else 1_000
        )

    async def _inspect_collection(
        self,
        reference: CollectionRef,
        *,
        include_document_count: bool = False,
    ) -> CollectionSnapshot | None:
        if not isinstance(self._adapter, MaterializationAdapter):
            raise NotImplementedError(f"{self.provider} does not support destination inspection")
        return await self._adapter.inspect_collection(
            reference, self._target, include_document_count=include_document_count
        )

    def _bridge(self, policy: NoSQLPolicy) -> NoSQLExecutionAdapter:
        return NoSQLExecutionAdapter(self._adapter, self._target, policy)

    def _install_bridge(self, policy: NoSQLPolicy) -> NoSQLExecutionAdapter:
        bridge = self._bridge(policy)
        self._plane.register_adapter(self.provider, bridge)
        return bridge

    def _execution_target(self) -> ExecutionTarget:
        # Deliberately exclude provider configuration and credentials.
        return ExecutionTarget(self.provider, {})


def _with_collection(context: Context | None, collection: str) -> Context:
    base = context or Context()
    return Context(
        resources=base.resources,
        metadata={**base.metadata, "gantry.nosql.collection": collection},
    )


def _find_inline(outputs: Sequence[OutputRef], max_documents: int) -> InlineDocuments | None:
    for output in outputs:
        if output.kind is OutputKind.INLINE:
            inline = output.metadata.get("inline")
            if isinstance(inline, InlineDocuments):
                documents = inline.documents[:max_documents]
                return InlineDocuments(
                    documents,
                    truncated=inline.truncated or len(inline.documents) > max_documents,
                )
    return None


def _from_engine_result(result: Result, max_documents: int) -> NoSQLResult:
    return NoSQLResult(
        status=result.status,
        handle=result.handle,
        inline=_find_inline(result.outputs, max_documents),
        outputs=result.outputs,
        metrics=result.metrics,
        verification=result.verification,
        failure=result.failure,
    )


def connect(provider: str, **config: object) -> NoSQLConnection:
    """Resolve a provider preset and create a governed NoSQL connection."""

    entry = resolve_provider(provider)
    entry.validate_config(config)
    target = NoSQLTarget(provider, entry.driver, config)
    return NoSQLConnection(target, entry.adapter_factory(target))
