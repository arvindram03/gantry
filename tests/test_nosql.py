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
