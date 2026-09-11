# SPDX-License-Identifier: Apache-2.0
"""Governed, provider-neutral MongoDB access for agents."""

from gantry.nosql.adapter import NoSQLAdapter
from gantry.nosql.api import NoSQLConnection, connect
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.materialization import (
    MaterializationAdapter,
    MaterializationCapabilities,
    MaterializationError,
    MaterializationOperation,
    MaterializationPlan,
    MaterializationPolicy,
    MaterializationResult,
    NoSQLMaterializer,
)
from gantry.nosql.output import InlineDocuments
from gantry.nosql.pipeline import (
    CollectionRef,
    PipelineClassification,
    PipelineOperation,
    classify_pipeline,
)
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.providers import register_builtin_providers
from gantry.nosql.query import NoSQLQuery
from gantry.nosql.registry import providers, register, register_provider
from gantry.nosql.result import NoSQLResult
from gantry.nosql.target import NoSQLTarget
from gantry.nosql.verify import (
    CollectionSnapshot,
    DestinationExists,
    DocumentCheck,
    DocumentCount,
    RequiredFields,
    destination_exists,
    document_count,
    required_fields,
)

register_builtin_providers()

__all__ = [
    "CollectionRef",
    "CollectionSnapshot",
    "DestinationExists",
    "DocumentCheck",
    "DocumentCount",
    "InlineDocuments",
    "MaterializationAdapter",
    "MaterializationCapabilities",
    "MaterializationError",
    "MaterializationOperation",
    "MaterializationPlan",
    "MaterializationPolicy",
    "MaterializationResult",
    "NoSQLAdapter",
    "NoSQLCapabilities",
    "NoSQLConnection",
    "NoSQLMaterializer",
    "NoSQLPolicy",
    "NoSQLQuery",
    "NoSQLResult",
    "NoSQLTarget",
    "PipelineClassification",
    "PipelineOperation",
    "RequiredFields",
    "classify_pipeline",
    "connect",
    "destination_exists",
    "document_count",
    "providers",
    "register",
    "register_provider",
    "required_fields",
]
