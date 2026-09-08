# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    type: str
    nullable: bool
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Table:
    name: str
    schema: str | None = None
    catalog: str | None = None
    columns: tuple[Column, ...] = ()
    primary_key: tuple[str, ...] = ()
    kind: str = "table"
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DatabaseSchema:
    catalogs: tuple[str, ...] = ()
    schemas: tuple[str, ...] = ()
    tables: tuple[Table, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)
