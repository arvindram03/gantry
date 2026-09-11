# SPDX-License-Identifier: Apache-2.0
"""Shared helper for the live-provider test files.

Skipping is right for a developer running the suite without the
infrastructure a live test needs. In CI, that skip is indistinguishable from
one caused by a broken fixture — both stay green. Setting
`GANTRY_REQUIRE_LIVE=1` removes the ambiguity for jobs that bring the
infrastructure up themselves: a skip becomes a failure instead.
"""

from __future__ import annotations

import os

import pytest


def require_live_or_skip(reason: str) -> None:
    """Skip, unless the caller promised this infrastructure would be up."""
    if os.environ.get("GANTRY_REQUIRE_LIVE") == "1":
        pytest.fail(reason)
    pytest.skip(reason)
