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
