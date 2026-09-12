# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from gantry.nosql.adapter import NoSQLAdapter
from gantry.nosql.target import NoSQLTarget

AdapterFactory = Callable[[NoSQLTarget], NoSQLAdapter]
ConfigValidator = Callable[[Mapping[str, object]], None]


@dataclass(frozen=True, slots=True)
class Provider:
    name: str
    driver: str
    adapter_factory: AdapterFactory
    validate_config: ConfigValidator
    metadata: Mapping[str, object] = field(default_factory=dict)


_providers: dict[str, Provider] = {}


def register_provider(
    name: str,
    *,
    driver: str,
    adapter_factory: AdapterFactory,
    validate_config: ConfigValidator | None = None,
    metadata: Mapping[str, object] | None = None,
    replace: bool = False,
) -> None:
    key = name.strip().lower()
    if not key:
        raise ValueError("provider name must not be empty")
    if key in _providers and not replace:
        raise ValueError(f"NoSQL provider is already registered: {name}")
    _providers[key] = Provider(
        name=key,
        driver=driver,
        adapter_factory=adapter_factory,
        validate_config=validate_config or _accept_config,
        metadata={} if metadata is None else metadata,
    )


def register(
    name: str,
    *,
    adapter: NoSQLAdapter,
    driver: str | None = None,
    replace: bool = False,
) -> None:
    register_provider(
        name,
        driver=driver or name,
        adapter_factory=lambda target: adapter,
        replace=replace,
    )


def resolve_provider(name: str) -> Provider:
    try:
        return _providers[name.strip().lower()]
    except KeyError as error:
        available = ", ".join(sorted(_providers))
        raise ValueError(f"unknown NoSQL provider {name!r}; available: {available}") from error


def providers() -> tuple[str, ...]:
    return tuple(sorted(_providers))


def _accept_config(config: Mapping[str, object]) -> None:
    return None
