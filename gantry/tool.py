# SPDX-License-Identifier: Apache-2.0
"""Small framework-neutral tool contract for governed operations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Tool[T]:
    """A named JSON-schema operation that an agent framework can invoke."""

    name: str
    description: str
    input_schema: Mapping[str, object]
    _handler: Callable[[Mapping[str, object]], Awaitable[T]] = field(repr=False)

    async def invoke(
        self,
        arguments: Mapping[str, object] | None = None,
        /,
        **keyword_arguments: object,
    ) -> T:
        """Invoke the trusted handler with model-supplied arguments."""

        if arguments is not None and keyword_arguments:
            raise TypeError("pass tool arguments as a mapping or keywords, not both")
        values = dict(arguments) if arguments is not None else keyword_arguments
        return await self._handler(values)
