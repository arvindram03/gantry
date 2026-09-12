# SPDX-License-Identifier: Apache-2.0
"""Built-in SQL provider presets."""

from __future__ import annotations

from collections.abc import Mapping

from gantry.sql.adapter import SQLAdapter
from gantry.sql.adapters.duckdb import DuckDBAdapter
from gantry.sql.registry import register_provider
from gantry.sql.target import SQLTarget


def _require_url(config: Mapping[str, object]) -> None:
    url = config.get("url")
    if url is not None and (not isinstance(url, str) or not url.strip()):
        raise ValueError("provider url must be a non-empty string")
    explicit = all(
        isinstance(config.get(field), str) and bool(str(config[field]).strip())
        for field in ("host", "database", "user")
    )
    if url is None and not explicit:
        raise ValueError("provider requires url or host, database, and user")


def _validate_duckdb(config: Mapping[str, object]) -> None:
    unknown = set(config) - {"path", "read_only"}
    if unknown:
        raise ValueError(f"unknown DuckDB configuration fields: {', '.join(sorted(unknown))}")
    if "path" in config and not isinstance(config["path"], str):
        raise ValueError("DuckDB path must be a string")
    if "read_only" in config and not isinstance(config["read_only"], bool):
        raise ValueError("DuckDB read_only must be a boolean")


def _postgres_factory(target: SQLTarget) -> SQLAdapter:
    from gantry.sql.adapters.postgres import PostgresAdapter

    return PostgresAdapter(target)


def _mysql_factory(target: SQLTarget) -> SQLAdapter:
    from gantry.sql.adapters.mysql import MySQLAdapter

    return MySQLAdapter(target)


def _bigquery_factory(target: SQLTarget) -> SQLAdapter:
    from gantry.sql.adapters.bigquery import BigQueryAdapter

    return BigQueryAdapter(target)


def _snowflake_factory(target: SQLTarget) -> SQLAdapter:
    from gantry.sql.adapters.snowflake import SnowflakeAdapter

    return SnowflakeAdapter(target)


def _require_bigquery(config: Mapping[str, object]) -> None:
    allowed = {
        "project",
        "dataset",
        "location",
        "credentials",
        "client_options",
        "price_per_tb_usd",
    }
    unknown = set(config) - allowed
    if unknown:
        raise ValueError(f"unknown BigQuery configuration fields: {', '.join(sorted(unknown))}")
    if not isinstance(config.get("project"), str) or not str(config["project"]).strip():
        raise ValueError("BigQuery provider requires project")
    for field in ("dataset", "location"):
        if field in config and not isinstance(config[field], str):
            raise ValueError(f"BigQuery {field} must be a string")
    price = config.get("price_per_tb_usd")
    if price is not None and (isinstance(price, bool) or not isinstance(price, (int, float))):
        raise ValueError("BigQuery price_per_tb_usd must be numeric")
    if isinstance(price, (int, float)) and price < 0:
        raise ValueError("BigQuery price_per_tb_usd must not be negative")


def _require_snowflake(config: Mapping[str, object]) -> None:
    required = ("account", "database", "warehouse")
    missing = [field for field in required if not isinstance(config.get(field), str)]
    if missing:
        raise ValueError(f"Snowflake provider requires: {', '.join(missing)}")
    if "read_only" in config and not isinstance(config["read_only"], bool):
        raise ValueError("Snowflake read_only must be a boolean")


def register_builtin_providers() -> None:
    register_provider(
        "duckdb",
        dialect="duckdb",
        driver="duckdb",
        adapter_factory=DuckDBAdapter,
        validate_config=_validate_duckdb,
    )
    for name in ("postgres", "neon", "supabase"):
        register_provider(
            name,
            dialect="postgres",
            driver="postgres",
            adapter_factory=_postgres_factory,
            validate_config=_require_url,
        )
    register_provider(
        "mysql",
        dialect="mysql",
        driver="aiomysql",
        adapter_factory=_mysql_factory,
        validate_config=_require_url,
    )
    register_provider(
        "bigquery",
        dialect="bigquery",
        driver="bigquery",
        adapter_factory=_bigquery_factory,
        validate_config=_require_bigquery,
    )
    register_provider(
        "snowflake",
        dialect="snowflake",
        driver="snowflake",
        adapter_factory=_snowflake_factory,
        validate_config=_require_snowflake,
    )
