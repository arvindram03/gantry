# SPDX-License-Identifier: Apache-2.0
"""The CDC adapter interface."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Protocol

from gantry.core.changes import ChangeEvent, StreamPosition
from gantry.core.positions import SourcePosition


class CDCAdapter(Protocol):
    """Streams changes from a source."""

    async def start(self, *, resume_from: StreamPosition | None = None) -> None:
        """Begin streaming, optionally from a position the runtime recorded.

        Resumption uses Gantry's own recorded position rather than a consumer
        group's. A consumer group commit is a separate durability domain from
        the target write, and trusting it would mean the runtime believed
        progress it had not made.
        """
        ...

    def events(self) -> AsyncIterator[ChangeEvent]:
        """Change events in transport order."""
        ...

    async def source_position(self) -> SourcePosition:
        """The source's current position, for measuring how far behind we are."""
        ...

    async def lag(self) -> timedelta:
        """How far behind the source the stream currently is."""
        ...

    async def stop(self) -> None:
        """Stop streaming. Does not tear down the connector."""
        ...
