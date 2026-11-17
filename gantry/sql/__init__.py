# SPDX-License-Identifier: Apache-2.0
"""Governed, provider-neutral SQL access for agents."""

from gantry.sql.adapter import SQLAdapter
from gantry.sql.api import SQLConnection, connect
from gantry.sql.capabilities import SQLCapabilities
from gantry.sql.classification import ParsedSQL, SQLClassification, SQLObjectRef, SQLOperation
from gantry.sql.dialect import ConservativeDialect, MySQLDialect, SQLDialect
from gantry.sql.explain import ExplainResult
from gantry.sql.materialization import (
    MaterializationAdapter,
    MaterializationCapabilities,
    MaterializationError,
    MaterializationOperation,
    MaterializationPlan,
    MaterializationPolicy,
    MaterializationProposal,
    MaterializationResult,
    SQLMaterializer,
    TableRef,
    parse_materialization,
)
from gantry.sql.output import InlineRows
from gantry.sql.policy import SQLPolicy
from gantry.sql.providers import register_builtin_providers
from gantry.sql.query import SQLQuery
from gantry.sql.registry import providers, register, register_dialect, register_provider
from gantry.sql.result import SQLResult
from gantry.sql.schema import Column, DatabaseSchema, Table
from gantry.sql.schema_change import SchemaChangePolicy, SchemaStep, SQLSchemaChange
from gantry.sql.snapshot import Column as SnapshotColumn
from gantry.sql.snapshot import ObjectKind, SchemaObject, SchemaSnapshot
from gantry.sql.target import SQLTarget
from gantry.sql.write import SQLWrite, WritePolicy

register_builtin_providers()

__all__ = [
    "Column",
    "ConservativeDialect",
    "DatabaseSchema",
    "ExplainResult",
    "InlineRows",
    "MaterializationAdapter",
    "MaterializationCapabilities",
    "MaterializationError",
    "MaterializationOperation",
    "MaterializationPlan",
    "MaterializationPolicy",
    "MaterializationProposal",
    "MaterializationResult",
    "MySQLDialect",
    "ObjectKind",
    "ParsedSQL",
    "SQLAdapter",
    "SQLCapabilities",
    "SQLClassification",
    "SQLConnection",
    "SQLDialect",
    "SQLMaterializer",
    "SQLObjectRef",
    "SQLOperation",
    "SQLPolicy",
    "SQLQuery",
    "SQLResult",
    "SQLSchemaChange",
    "SQLTarget",
    "SQLWrite",
    "SchemaChangePolicy",
    "SchemaObject",
    "SchemaSnapshot",
    "SchemaStep",
    "SnapshotColumn",
    "Table",
    "TableRef",
    "WritePolicy",
    "connect",
    "parse_materialization",
    "providers",
    "register",
    "register_dialect",
    "register_provider",
]
