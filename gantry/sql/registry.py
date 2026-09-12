# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from gantry.sql.adapter import SQLAdapter
from gantry.sql.dialect import ConservativeDialect, MySQLDialect, SQLDialect
from gantry.sql.target import SQLTarget

AdapterFactory = Callable[[SQLTarget], SQLAdapter]
ConfigValidator = Callable[[Mapping[str, object]], None]


@dataclass(frozen=True, slots=True)
class Provider:
    name: str
    dialect: str
    driver: str
    adapter_factory: AdapterFactory
    validate_config: ConfigValidator
    metadata: Mapping[str, object] = field(default_factory=dict)


_providers: dict[str, Provider] = {}
_dialects: dict[str, SQLDialect] = {}


def register_dialect(name: str, dialect: SQLDialect, *, replace: bool = False) -> None:
    """Register a SQL dialect under a name, for classification and splitting."""
    key = name.strip().lower()
    if not key:
        raise ValueError("dialect name must not be empty")
    if key in _dialects and not replace:
        raise ValueError(f"SQL dialect is already registered: {name}")
    _dialects[key] = dialect


def register_provider(
    name: str,
    *,
    dialect: str,
    driver: str,
    adapter_factory: AdapterFactory,
    validate_config: ConfigValidator | None = None,
    metadata: Mapping[str, object] | None = None,
    replace: bool = False,
) -> None:
    """Register a provider that `gantry.sql.connect` can open by name.

    `adapter_factory` is called with the resolved `SQLTarget` each time a
    connection is opened, so one provider can serve many targets.
    `validate_config` runs before the factory and should raise on
    configuration the adapter cannot honour. Raises `ValueError` if the name
    is empty, or already registered and `replace` is false.
    """
    key = name.strip().lower()
    if not key:
        raise ValueError("provider name must not be empty")
    if key in _providers and not replace:
        raise ValueError(f"SQL provider is already registered: {name}")
    _providers[key] = Provider(
        name=key,
        dialect=dialect.strip().lower(),
        driver=driver,
        adapter_factory=adapter_factory,
        validate_config=validate_config or _accept_config,
        metadata={} if metadata is None else metadata,
    )


def register(
    name: str,
    *,
    adapter: SQLAdapter,
    dialect: str,
    driver: str | None = None,
    replace: bool = False,
) -> None:
    """Register a single already-built `adapter` as a provider named `name`.

    A shorthand over `register_provider` for tests and embedded adapters: every
    target resolved through this name shares the one adapter instance. `driver`
    defaults to `name`.
    """
    register_provider(
        name,
        dialect=dialect,
        driver=driver or name,
        adapter_factory=lambda target: adapter,
        replace=replace,
    )


def resolve_provider(name: str) -> Provider:
    try:
        return _providers[name.strip().lower()]
    except KeyError as error:
        available = ", ".join(sorted(_providers))
        raise ValueError(f"unknown SQL provider {name!r}; available: {available}") from error


def resolve_dialect(name: str) -> SQLDialect:
    try:
        return _dialects[name.strip().lower()]
    except KeyError as error:
        raise ValueError(f"unknown SQL dialect: {name}") from error


def providers() -> tuple[str, ...]:
    """Every registered provider name, sorted."""
    return tuple(sorted(_providers))


def _accept_config(config: Mapping[str, object]) -> None:
    return None


for _dialect_name in ("postgres", "sqlserver", "bigquery", "snowflake", "duckdb"):
    register_dialect(_dialect_name, ConservativeDialect())

# MySQL lexes strings differently enough that the shared rules mis-split it.
register_dialect("mysql", MySQLDialect())
