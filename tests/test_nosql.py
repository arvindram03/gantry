# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.target import NoSQLTarget


def test_target_rejects_empty_provider_or_driver() -> None:
    NoSQLTarget("mongodb", "pymongo", {"uri": "mongodb://localhost", "database": "d"})

    with pytest.raises(ValueError, match="provider"):
        NoSQLTarget("", "pymongo", {})
    with pytest.raises(ValueError, match="driver"):
        NoSQLTarget("mongodb", "", {})


def test_policy_normalizes_collection_names_and_validates_types() -> None:
    policy = NoSQLPolicy(allowed_collections=["Orders"], denied_collections=["Secrets"])

    assert policy.allowed_collections == frozenset({"orders"})
    assert policy.denied_collections == frozenset({"secrets"})

    with pytest.raises(TypeError, match="collection"):
        NoSQLPolicy(allowed_collections="orders")
    with pytest.raises(TypeError, match="read_only"):
        NoSQLPolicy(read_only="yes")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max documents"):
        NoSQLPolicy(max_documents=0)
    with pytest.raises(ValueError, match="timeout"):
        NoSQLPolicy(timeout_seconds=-1)
    with pytest.raises(ValueError, match="max bytes scanned"):
        NoSQLPolicy(max_bytes_scanned=-1)
    with pytest.raises(ValueError, match="max cost"):
        NoSQLPolicy(max_cost_usd=-1)


from gantry.nosql.capabilities import NoSQLCapabilities


def test_capabilities_map_to_core_contract_and_policy_requirements() -> None:
    capabilities = NoSQLCapabilities(
        cancellation=True,
        read_only_session=True,
        operation_timeout=True,
        cost_limit=True,
        query_metrics=True,
        result_reference=True,
    )
    policy = NoSQLPolicy(read_only=True, timeout_seconds=15, max_bytes_scanned=100)

    core = capabilities.core_capabilities()
    requirements = capabilities.policy_requirements(policy)

    assert core.cancellation is True
    assert core.read_only_execution is True
    assert core.metrics is True
    assert requirements.read_only is True
    assert requirements.allow_writes is False
    assert requirements.max_runtime_seconds == 15
    assert requirements.require_reconnect is False
    assert requirements.require_metrics is True


from gantry.nosql.output import InlineDocuments


def test_inline_documents_holds_heterogeneous_documents() -> None:
    inline = InlineDocuments(({"a": 1}, {"a": 1, "b": 2}), truncated=True)

    assert inline.documents == ({"a": 1}, {"a": 1, "b": 2})
    assert inline.truncated is True


from gantry.nosql.pipeline import (
    CollectionRef,
    PipelineOperation,
    classify_pipeline,
    normalize_pipeline,
)


def test_normalize_pipeline_wraps_a_plain_filter_dict() -> None:
    stages = normalize_pipeline({"status": "open"})

    assert stages == ({"$match": {"status": "open"}},)

    with pytest.raises(TypeError, match="mapping filter"):
        normalize_pipeline("not a pipeline")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="every pipeline stage must be a mapping"):
        normalize_pipeline([{"$match": {}}, "not a stage"])  # type: ignore[list-item]


def test_classify_pipeline_reads_lookup_and_reports_read_only() -> None:
    classification = classify_pipeline(
        "orders",
        [{"$match": {"status": "open"}}, {"$lookup": {"from": "customers", "as": "c"}}],
    )

    assert classification.operation is PipelineOperation.READ
    assert classification.read_only is True
    assert CollectionRef("orders") in classification.collections
    assert CollectionRef("customers") in classification.collections
    assert classification.write_destination is None


def test_classify_pipeline_recognizes_out_and_merge_and_rejects_bad_stages() -> None:
    out = classify_pipeline("orders", [{"$match": {}}, {"$out": "reporting.rollup"}])
    merge = classify_pipeline(
        "orders",
        [{"$match": {}}, {"$merge": {"into": "reporting.rollup", "whenMatched": "merge"}}],
    )

    assert out.operation is PipelineOperation.WRITE
    assert out.read_only is False
    assert out.write_destination == CollectionRef("reporting.rollup")
    assert out.write_stage == "$out"
    assert merge.write_destination == CollectionRef("reporting.rollup")

    with pytest.raises(ValueError, match="unsupported pipeline stage"):
        classify_pipeline("orders", [{"$graphLookup": {}}])
    with pytest.raises(ValueError, match="must be the last stage"):
        classify_pipeline("orders", [{"$out": "x"}, {"$match": {}}])
    with pytest.raises(ValueError, match="whenMatched"):
        classify_pipeline(
            "orders", [{"$merge": {"into": "x", "whenMatched": "keepExisting"}}]
        )
    with pytest.raises(ValueError, match="exactly one operator"):
        classify_pipeline("orders", [{"$match": {}, "$project": {}}])
    with pytest.raises(ValueError, match="collection name must not be empty"):
        classify_pipeline("  ", [{"$match": {}}])


from gantry.nosql.enforcement import policy_errors


def test_policy_errors_flags_denied_collections_and_missing_capabilities() -> None:
    classification = classify_pipeline(
        "orders", [{"$match": {}}, {"$lookup": {"from": "secrets", "as": "s"}}]
    )
    policy = NoSQLPolicy(
        read_only=True,
        allowed_collections=["orders"],
        denied_collections=["secrets"],
        max_bytes_scanned=10,
    )
    capabilities = NoSQLCapabilities()

    errors = policy_errors(classification, policy, capabilities)

    assert "collection is not allowed: secrets" in errors
    assert "collection is denied: secrets" in errors
    assert "adapter cannot enforce a read-only session" in errors
    assert "adapter cannot enforce the document limit" in errors
    assert "adapter cannot enforce or monitor the operation timeout" in errors
    assert "adapter cannot enforce maximum bytes scanned" in errors


def test_policy_errors_flags_write_under_read_only_policy() -> None:
    classification = classify_pipeline("orders", [{"$out": "reporting.rollup"}])
    policy = NoSQLPolicy(read_only=True)
    capabilities = NoSQLCapabilities(read_only_session=True, document_limit=True, operation_timeout=True)

    errors = policy_errors(classification, policy, capabilities)

    assert any("not allowed by read-only policy" in error for error in errors)


from gantry import (
    Context,
    Execution,
    ExecutionHandle,
    ExecutionResult,
    ExecutionState,
    ExecutionTarget,
    Failure,
    FailureKind,
    OutputKind,
    OutputRef,
)
from gantry.artifact import Artifact
from gantry.nosql.adapter import NoSQLAdapter
from gantry.nosql.bridge import NoSQLExecutionAdapter
from gantry.nosql.output import InlineDocuments
from gantry.nosql.pipeline import Pipeline
from gantry.nosql.target import NoSQLTarget


class StubMongoAdapter:
    def __init__(self, *, read_only_session: bool = True) -> None:
        self._capabilities = NoSQLCapabilities(
            cancellation=True,
            read_only_session=read_only_session,
            operation_timeout=True,
            document_limit=True,
            query_metrics=True,
            result_reference=True,
            out_merge_writes=True,
            destination_introspection=True,
            materialization_reference=True,
        )
        self.handle: ExecutionHandle | None = None
        self.policy: NoSQLPolicy | None = None
        self.submitted_collection: str | None = None
        self.submissions = 0
        self.collections: dict[str, dict[str, object]] = {}

    def capabilities(self) -> NoSQLCapabilities:
        return self._capabilities

    async def validate(
        self,
        pipeline: Pipeline,
        target: NoSQLTarget,
        context: Context,
        policy: NoSQLPolicy,
    ) -> object:
        from gantry import ValidationResult

        return ValidationResult.accepted(metadata={"provider": target.provider})

    async def submit(
        self,
        pipeline: Pipeline,
        target: NoSQLTarget,
        context: Context,
    ) -> ExecutionHandle:
        self.submissions += 1
        policy_value = context.metadata.get("gantry.nosql.policy")
        assert isinstance(policy_value, NoSQLPolicy)
        self.policy = policy_value
        collection_value = context.metadata.get("gantry.nosql.collection")
        assert isinstance(collection_value, str)
        self.submitted_collection = collection_value
        self.handle = ExecutionHandle("nosql-run", "nosql", target.provider, "native-op")
        return self.handle

    async def status(self, handle: ExecutionHandle) -> Execution:
        return Execution(handle, ExecutionState.SUCCEEDED)

    async def result(self, handle: ExecutionHandle) -> ExecutionResult:
        inline = InlineDocuments(({"_id": 1}, {"_id": 2}))
        return ExecutionResult.succeeded(
            handle,
            outputs=(OutputRef(OutputKind.INLINE, "inline://nosql-run", {"inline": inline}),),
        )

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, f"cancelled ({mode})"),
        )


def _nosql_target() -> NoSQLTarget:
    return NoSQLTarget("mongodb", "pymongo", {"uri": "mongodb://localhost", "database": "d"})


async def test_bridge_validates_pipeline_and_injects_collection_and_policy() -> None:
    adapter = StubMongoAdapter()
    target = _nosql_target()
    policy = NoSQLPolicy(allowed_collections=["orders"])
    bridge = NoSQLExecutionAdapter(adapter, target, policy)
    artifact = Artifact({"status": "open"}, "nosql")
    execution_target = ExecutionTarget(target.provider, {})
    context = Context(metadata={"gantry.nosql.collection": "orders"})

    validation = await bridge.validate(
        artifact=artifact, target=execution_target, context=context, policy=policy
    )
    handle = await bridge.submit(artifact=artifact, target=execution_target, context=context)

    assert validation.ok
    assert adapter.submitted_collection == "orders"
    assert adapter.policy is policy
    assert handle is adapter.handle


async def test_bridge_rejects_when_collection_is_missing_from_context() -> None:
    adapter = StubMongoAdapter()
    target = _nosql_target()
    policy = NoSQLPolicy()
    bridge = NoSQLExecutionAdapter(adapter, target, policy)
    artifact = Artifact({"status": "open"}, "nosql")
    execution_target = ExecutionTarget(target.provider, {})

    validation = await bridge.validate(
        artifact=artifact, target=execution_target, context=Context(), policy=policy
    )

    assert not validation.ok
    assert any("collection" in error for error in validation.errors)


async def test_bridge_rejects_unclassifiable_pipeline() -> None:
    adapter = StubMongoAdapter()
    target = _nosql_target()
    policy = NoSQLPolicy()
    bridge = NoSQLExecutionAdapter(adapter, target, policy)
    artifact = Artifact([{"$graphLookup": {}}], "nosql")
    execution_target = ExecutionTarget(target.provider, {})
    context = Context(metadata={"gantry.nosql.collection": "orders"})

    validation = await bridge.validate(
        artifact=artifact, target=execution_target, context=context, policy=policy
    )

    assert not validation.ok
    assert any("pipeline classification failed" in error for error in validation.errors)


from gantry.nosql.registry import providers, register, register_provider, resolve_provider


def test_registry_registers_and_resolves_providers() -> None:
    register_provider(
        "test-mongo",
        driver="pymongo",
        adapter_factory=lambda target: StubMongoAdapter(),
        replace=True,
    )

    resolved = resolve_provider("test-mongo")

    assert resolved.name == "test-mongo"
    assert "test-mongo" in providers()
    with pytest.raises(ValueError, match="unknown NoSQL provider"):
        resolve_provider("missing")
    with pytest.raises(ValueError, match="already registered"):
        register_provider("test-mongo", driver="pymongo", adapter_factory=lambda target: StubMongoAdapter())


def test_register_wraps_a_ready_adapter_instance() -> None:
    adapter = StubMongoAdapter()
    register("test-mongo-instance", adapter=adapter, replace=True)

    resolved = resolve_provider("test-mongo-instance")

    assert resolved.adapter_factory(_nosql_target()) is adapter
