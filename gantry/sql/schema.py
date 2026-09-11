# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Column:
    """One column as the engine reports it.

    `type` is the engine's own type name, not a normalized one: an agent
    writing SQL needs the name the engine will accept.
    """

    name: str
    type: str
    nullable: bool
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Table:
    """One table or view, with the columns an agent may write SQL against.

    `kind` distinguishes a table from a view. `columns` is empty when the
    adapter listed the table without describing it.
    """

    name: str
    schema: str | None = None
    catalog: str | None = None
    columns: tuple[Column, ...] = ()
    primary_key: tuple[str, ...] = ()
    kind: str = "table"
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DatabaseSchema:
    """What the connection can see, as the schema handed to an agent.

    Produced by `SQLConnection.describe()`. This is the right shape to put in a
    prompt: only what the credential can reach, so the agent is not invited to
    reference a table it cannot read.
    """

    catalogs: tuple[str, ...] = ()
    schemas: tuple[str, ...] = ()
    tables: tuple[Table, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)
