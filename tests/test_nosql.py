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
