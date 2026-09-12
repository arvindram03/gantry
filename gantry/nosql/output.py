# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class InlineDocuments:
    documents: tuple[Mapping[str, object], ...]
    truncated: bool = False
