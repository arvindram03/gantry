# SPDX-License-Identifier: Apache-2.0
"""Execution context supplied by the application."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Context:
    """Resources and metadata made available for one run."""

    resources: Mapping[str, object] = field(default_factory=dict)
    metadata: Mapping[str, object] = field(default_factory=dict)
