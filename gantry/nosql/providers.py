# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping

from gantry.nosql.adapter import NoSQLAdapter
from gantry.nosql.registry import register_provider
from gantry.nosql.target import NoSQLTarget


def _require_uri(config: Mapping[str, object]) -> None:
    allowed = {"uri", "database"}
    unknown = set(config) - allowed
    if unknown:
        raise ValueError(f"unknown MongoDB provider option(s): {', '.join(sorted(unknown))}")
    uri = config.get("uri")
    if not isinstance(uri, str) or not uri.strip():
        raise ValueError("MongoDB provider requires uri")
    database = config.get("database")
    if not isinstance(database, str) or not database.strip():
        raise ValueError("MongoDB provider requires database")


def _mongo_factory(target: NoSQLTarget) -> NoSQLAdapter:
    from gantry.nosql.adapters.mongodb import MongoAdapter

    return MongoAdapter(target)


def register_builtin_providers() -> None:
    register_provider(
        "mongodb",
        driver="pymongo",
        adapter_factory=_mongo_factory,
        validate_config=_require_uri,
        replace=True,
    )
