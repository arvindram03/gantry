# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from gantry.nosql.target import NoSQLTarget


def test_target_rejects_empty_provider_or_driver() -> None:
    NoSQLTarget("mongodb", "pymongo", {"uri": "mongodb://localhost", "database": "d"})

    with pytest.raises(ValueError, match="provider"):
        NoSQLTarget("", "pymongo", {})
    with pytest.raises(ValueError, match="driver"):
        NoSQLTarget("mongodb", "", {})
