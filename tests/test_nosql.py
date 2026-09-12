# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import gantry
import pytest
from gantry import (
    Artifact,
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
    ValidationResult,
)
from gantry.nosql.bridge import NoSQLExecutionAdapter
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.enforcement import policy_errors
from gantry.nosql.materialization import MaterializationPolicy, NoSQLMaterializer
from gantry.nosql.output import InlineDocuments
from gantry.nosql.pipeline import (
    CollectionRef,
    Pipeline,
    PipelineOperation,
    classify_pipeline,
    normalize_pipeline,
)
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.query import NoSQLQuery
from gantry.nosql.registry import providers, register, register_provider, resolve_provider
from gantry.nosql.result import NoSQLResult
from gantry.nosql.target import NoSQLTarget
from gantry.nosql.verify import (
    CollectionSnapshot,
    destination_exists,
    document_count,
    required_fields,
)
from gantry.result import ResultStatus
from gantry.runs.model import Run
from gantry.runs.status import RunStatus

from _runs import make_run


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


def test_inline_documents_holds_heterogeneous_documents() -> None:
    inline = InlineDocuments(({"a": 1}, {"a": 1, "b": 2}), truncated=True)

    assert inline.documents == ({"a": 1}, {"a": 1, "b": 2})
    assert inline.truncated is True


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
        classify_pipeline("orders", [{"$merge": {"into": "x", "whenMatched": "keepExisting"}}])
    with pytest.raises(ValueError, match="exactly one operator"):
        classify_pipeline("orders", [{"$match": {}, "$project": {}}])
    with pytest.raises(ValueError, match="collection name must not be empty"):
        classify_pipeline("  ", [{"$match": {}}])


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
    capabilities = NoSQLCapabilities(
        read_only_session=True, document_limit=True, operation_timeout=True
    )

    errors = policy_errors(classification, policy, capabilities)

    assert any("not allowed by read-only policy" in error for error in errors)


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
    ) -> ValidationResult:
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
        register_provider(
            "test-mongo", driver="pymongo", adapter_factory=lambda target: StubMongoAdapter()
        )


def test_register_wraps_a_ready_adapter_instance() -> None:
    adapter = StubMongoAdapter()
    register("test-mongo-instance", adapter=adapter, replace=True)

    resolved = resolve_provider("test-mongo-instance")

    assert resolved.adapter_factory(_nosql_target()) is adapter


def test_verification_checks_against_a_collection_snapshot() -> None:
    snapshot = CollectionSnapshot("orders", {"document_count": 5}, ("status", "amount"))

    exists = destination_exists().evaluate(snapshot)
    missing = destination_exists().evaluate(None)
    count_ok = document_count(min=1, max=10).evaluate(snapshot)
    count_bad = document_count(min=100).evaluate(snapshot)
    fields_ok = required_fields(["status"]).evaluate(snapshot)
    fields_missing = required_fields(["region"]).evaluate(snapshot)

    assert exists.ok and not missing.ok
    assert count_ok.ok and not count_bad.ok
    assert fields_ok.ok
    assert not fields_missing.ok
    assert "region" in (fields_missing.message or "")

    with pytest.raises(ValueError, match="minimum must not exceed maximum"):
        document_count(min=10, max=1)
    with pytest.raises(ValueError, match="required fields must not be empty"):
        required_fields([])


class _FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, NoSQLPolicy]] = []

    async def _query(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy,
        context: Context | None = None,
        trusted_verify: Sequence[object] = (),
        agent_verify: Sequence[object] = (),
    ) -> Run:
        self.calls.append((collection, pipeline, policy))
        return make_run(id="run_fake", engine="mongodb")


async def test_query_calls_connection_with_collection_and_pipeline() -> None:
    connection = _FakeConnection()
    policy = NoSQLPolicy(max_documents=5)
    query = NoSQLQuery(connection, policy, ())

    result = await query("orders", {"status": "open"})

    assert result.ok
    assert connection.calls == [("orders", {"status": "open"}, policy)]


async def test_query_tool_exposes_collection_and_pipeline_only() -> None:
    connection = _FakeConnection()
    policy = NoSQLPolicy()
    query = NoSQLQuery(connection, policy, ())
    tool = query.tool()

    result = await tool.invoke(collection="orders", pipeline={"status": "open"})

    schema = cast(dict[str, Any], tool.input_schema)
    assert schema["required"] == ["collection", "pipeline"]
    assert set(schema["properties"]) == {"collection", "pipeline", "verify"}
    variants = schema["properties"]["verify"]["items"]["oneOf"]
    assert {item["properties"]["type"]["const"] for item in variants} == {
        "document_count",
        "not_empty",
        "required_fields",
    }
    assert result.ok
    with pytest.raises(TypeError, match="collection must be a string"):
        await tool.invoke(pipeline={})
    with pytest.raises(ValueError, match="unexpected query tool arguments"):
        await tool.invoke(collection="orders", pipeline={}, policy="untrusted")


class _FakeMaterializeConnection:
    def __init__(
        self, *, capabilities: NoSQLCapabilities, snapshot: CollectionSnapshot | None
    ) -> None:
        self._capabilities = capabilities
        self._snapshot = snapshot
        self.submit_calls: list[tuple[str, object, NoSQLPolicy | None]] = []

    def capabilities(self) -> NoSQLCapabilities:
        return self._capabilities

    async def submit(
        self,
        collection: str,
        pipeline: Pipeline,
        *,
        policy: NoSQLPolicy | None = None,
        context: Context | None = None,
    ) -> ExecutionHandle:
        self.submit_calls.append((collection, pipeline, policy))
        return ExecutionHandle("mat-run", "nosql", "mongodb", "native-mat")

    async def wait(
        self, handle: ExecutionHandle, *, poll_interval_seconds: float = 0.05
    ) -> NoSQLResult:
        # A stub standing in for a provider, so it reports a `ResultStatus`
        # like a real one. The `RunStatus` elsewhere in this file belongs to
        # the governed path above it, which is what turns this into a run.
        return NoSQLResult(
            ResultStatus.ACCEPTED,
            handle=handle,
            outputs=(OutputRef(OutputKind.TABLE, "mongodb://reporting.rollup"),),
        )

    async def status(self, handle: ExecutionHandle) -> object:
        raise NotImplementedError

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> object:
        raise NotImplementedError

    async def _inspect_collection(
        self, reference: CollectionRef, *, include_document_count: bool = False
    ) -> CollectionSnapshot | None:
        return self._snapshot


def _full_capabilities() -> NoSQLCapabilities:
    return NoSQLCapabilities(
        cancellation=True,
        read_only_session=True,
        operation_timeout=True,
        document_limit=True,
        query_metrics=True,
        result_reference=True,
        out_merge_writes=True,
        destination_introspection=True,
        materialization_reference=True,
    )


async def test_materializer_rejects_out_when_destination_already_exists() -> None:
    connection = _FakeMaterializeConnection(
        capabilities=_full_capabilities(),
        snapshot=CollectionSnapshot("reporting.rollup"),
    )
    policy = MaterializationPolicy(sources=["orders"], destinations=["reporting.rollup"])
    materializer = NoSQLMaterializer(connection, policy, ())

    result = await materializer("orders", [{"$match": {}}, {"$out": "reporting.rollup"}])

    assert result.status is RunStatus.POLICY_REJECTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.DESTINATION_EXISTS


async def test_materializer_allows_merge_into_an_existing_destination() -> None:
    connection = _FakeMaterializeConnection(
        capabilities=_full_capabilities(),
        snapshot=CollectionSnapshot("reporting.rollup", {"document_count": 3}),
    )
    policy = MaterializationPolicy(sources=["orders"], destinations=["reporting.rollup"])
    materializer = NoSQLMaterializer(
        connection, policy, (destination_exists(), document_count(min=1))
    )

    result = await materializer(
        "orders", [{"$match": {}}, {"$merge": {"into": "reporting.rollup", "whenMatched": "merge"}}]
    )

    assert result.ok
    assert connection.submit_calls[0][0] == "orders"
    assert result.uri == "mongodb://reporting.rollup"


async def test_materializer_accepts_agent_document_checks_and_records_evidence() -> None:
    connection = _FakeMaterializeConnection(
        capabilities=_full_capabilities(),
        snapshot=CollectionSnapshot(
            "reporting.rollup", {"document_count": 3}, ("customer_id", "total")
        ),
    )
    policy = MaterializationPolicy(sources=["orders"], destinations=["reporting.rollup"])
    materializer = NoSQLMaterializer(connection, policy, (gantry.verify.document_count(max=10),))

    result = await materializer(
        "orders",
        [{"$match": {}}, {"$merge": "reporting.rollup"}],
        verify=[
            gantry.verify.not_empty(),
            gantry.verify.required_fields(["customer_id", "total"]),
        ],
    )

    assert result.status is RunStatus.ACCEPTED
    assert result.verification is not None
    assert [check.source for check in result.verification.checks] == [
        gantry.CheckSource.TRUSTED,
        gantry.CheckSource.AGENT,
        gantry.CheckSource.AGENT,
    ]
    assert result.evidence is not None
    assert len(result.evidence.agent_checks) == 2


def test_materializer_tool_schema_exposes_only_mongodb_checks() -> None:
    connection = _FakeMaterializeConnection(capabilities=_full_capabilities(), snapshot=None)
    policy = MaterializationPolicy(sources=["orders"], destinations=["reporting.rollup"])
    schema = cast(dict[str, Any], NoSQLMaterializer(connection, policy, ()).input_schema)

    variants = schema["properties"]["verify"]["items"]["oneOf"]
    assert {item["properties"]["type"]["const"] for item in variants} == {
        "destination_exists",
        "document_count",
        "not_empty",
        "required_fields",
    }


async def test_materializer_rejects_sources_outside_policy() -> None:
    connection = _FakeMaterializeConnection(capabilities=_full_capabilities(), snapshot=None)
    policy = MaterializationPolicy(sources=["orders"], destinations=["reporting.rollup"])
    materializer = NoSQLMaterializer(connection, policy, ())

    result = await materializer("secrets", [{"$match": {}}, {"$out": "reporting.rollup"}])

    assert result.status is RunStatus.POLICY_REJECTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.SOURCE_NOT_ALLOWED


def test_mongo_adapter_reports_a_clear_error_without_pymongo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins

    real_import = builtins.__import__

    def _blocked_import(
        name: str,
        import_globals: Mapping[str, object] | None = None,
        import_locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> object:
        if name == "pymongo":
            raise ImportError("no module named pymongo")
        return real_import(name, import_globals, import_locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)

    from gantry.nosql.adapters.mongodb import MongoAdapter

    with pytest.raises(ImportError, match='pip install "data-gantry\\[mongodb\\]"'):
        MongoAdapter(_nosql_target())


def test_mongodb_provider_config_is_validated_before_driver_creation() -> None:
    from gantry.nosql.providers import register_builtin_providers
    from gantry.nosql.registry import resolve_provider

    register_builtin_providers()
    provider = resolve_provider("mongodb")

    with pytest.raises(ValueError, match="requires uri"):
        provider.validate_config({"database": "d"})
    with pytest.raises(ValueError, match="requires database"):
        provider.validate_config({"uri": "mongodb://localhost"})
    with pytest.raises(ValueError, match="unknown MongoDB provider option"):
        provider.validate_config({"uri": "mongodb://localhost", "database": "d", "typo": True})
    provider.validate_config({"uri": "mongodb://localhost", "database": "d"})


async def test_connect_query_runs_lifecycle_and_bounds_inline_documents() -> None:
    from gantry.nosql.api import connect
    from gantry.nosql.registry import register

    adapter = StubMongoAdapter()
    register("test-nosql", adapter=adapter, replace=True)
    db = connect("test-nosql", uri="mongodb://localhost", database="d")
    query = db.query(collections=["orders"], max_documents=1)

    result = await query("orders", {"status": "open"})

    assert result.status is RunStatus.ACCEPTED
    assert result.inline is not None
    assert len(result.documents) <= 1
    assert result.truncated is True
    assert adapter.submitted_collection == "orders"
    assert adapter.policy is not None
    assert adapter.policy.max_documents == 1


async def test_mongodb_query_merges_trusted_and_agent_verification() -> None:
    adapter = StubMongoAdapter()
    register("test-nosql-verification", adapter=adapter, replace=True)
    query = gantry.nosql.connect(
        "test-nosql-verification", uri="mongodb://localhost", database="d"
    ).query(checks=[gantry.verify.document_count(max=3)])

    result = await query(
        "orders",
        {"status": "open"},
        verify=[gantry.verify.not_empty(), gantry.verify.required_fields(["_id"])],
    )

    assert result.status is RunStatus.ACCEPTED
    assert result.verification is not None
    assert [check.source for check in result.verification.checks] == [
        gantry.CheckSource.TRUSTED,
        gantry.CheckSource.AGENT,
        gantry.CheckSource.AGENT,
    ]
    assert result.evidence is not None
    assert result.evidence.proposal["agent_verification"] == [
        {"type": "not_empty"},
        {"type": "required_fields", "fields": ["_id"]},
    ]


async def test_mongodb_query_conflict_stops_before_submission() -> None:
    adapter = StubMongoAdapter()
    register("test-nosql-conflict", adapter=adapter, replace=True)
    query = gantry.nosql.connect(
        "test-nosql-conflict", uri="mongodb://localhost", database="d"
    ).query(checks=[gantry.verify.document_count(max=1)])

    result = await query("orders", {}, verify=[gantry.verify.document_count(min=2)])

    assert result.status is RunStatus.VERIFICATION_CONFLICT
    assert adapter.submissions == 0


async def test_mongodb_not_empty_conflicts_with_zero_document_limit() -> None:
    adapter = StubMongoAdapter()
    register("test-nosql-empty-conflict", adapter=adapter, replace=True)
    query = gantry.nosql.connect(
        "test-nosql-empty-conflict", uri="mongodb://localhost", database="d"
    ).query(checks=[gantry.verify.document_count(max=0)])

    result = await query("orders", {}, verify=[gantry.verify.not_empty()])

    assert result.status is RunStatus.VERIFICATION_CONFLICT
    assert adapter.submissions == 0


async def test_mongodb_truncated_document_count_is_unsupported() -> None:
    adapter = StubMongoAdapter()
    register("test-nosql-truncated-verification", adapter=adapter, replace=True)
    query = gantry.nosql.connect(
        "test-nosql-truncated-verification", uri="mongodb://localhost", database="d"
    ).query(max_documents=1)

    result = await query("orders", {}, verify=[gantry.verify.document_count(max=10)])

    assert result.status is RunStatus.VERIFICATION_UNSUPPORTED
    assert result.failure is not None
    assert result.failure.kind is FailureKind.VERIFICATION_UNSUPPORTED
    assert result.verification is not None
    assert result.verification.unsupported_checks[0].name == "document_count"


async def test_connect_rejects_writes_under_a_read_only_query_policy() -> None:
    from gantry.nosql.api import connect
    from gantry.nosql.registry import register

    adapter = StubMongoAdapter()
    register("test-nosql-write", adapter=adapter, replace=True)
    db = connect("test-nosql-write", uri="mongodb://localhost", database="d")

    result = await db.query()("orders", [{"$match": {}}, {"$out": "reporting.rollup"}])

    assert result.status is RunStatus.POLICY_REJECTED
    assert result.failure is not None
    assert "read-only" in result.failure.message
    assert adapter.submissions == 0


def test_connect_provider_config_is_validated_before_driver_creation() -> None:
    from gantry.nosql.api import connect
    from gantry.nosql.providers import register_builtin_providers

    register_builtin_providers()

    with pytest.raises(ValueError, match="requires uri"):
        connect("mongodb", database="d")
    with pytest.raises(ValueError, match="unknown NoSQL provider"):
        connect("missing")


def test_nosql_package_exports_the_public_surface() -> None:
    import gantry.nosql as nosql

    assert {"mongodb"} <= set(nosql.providers())
    assert nosql.NoSQLConnection is not None
    assert nosql.NoSQLPolicy is not None
    assert nosql.NoSQLCapabilities is not None
    assert nosql.NoSQLTarget is not None
    assert nosql.CollectionSnapshot is not None
    assert nosql.destination_exists is not None


def test_gantry_top_level_exposes_the_nosql_module() -> None:
    import gantry

    assert gantry.nosql is not None
    assert "nosql" in gantry.__all__
