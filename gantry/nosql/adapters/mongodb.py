# SPDX-License-Identifier: Apache-2.0
"""The MongoDB adapter, using pymongo's native async client."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import uuid4

from gantry.context import Context
from gantry.execution import Execution, ExecutionResult, ExecutionState, ValidationResult
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.metrics import ExecutionMetrics
from gantry.nosql.capabilities import NoSQLCapabilities
from gantry.nosql.materialization import (
    _DESTINATION_METADATA,
    _OPERATION_METADATA,
    _PLAN_METADATA,
    MaterializationPlan,
)
from gantry.nosql.output import InlineDocuments
from gantry.nosql.pipeline import CollectionRef, Pipeline, normalize_pipeline
from gantry.nosql.policy import NoSQLPolicy
from gantry.nosql.target import NoSQLTarget
from gantry.nosql.verify import CollectionSnapshot
from gantry.output import OutputKind, OutputRef

_SAMPLE_SIZE = 100


class MongoAdapter:
    def __init__(self, target: NoSQLTarget) -> None:
        try:
            module = importlib.import_module("pymongo")
        except ImportError as error:
            raise ImportError(
                'MongoDB support requires `pip install "data-gantry[mongodb]"`'
            ) from error
        uri = target.config.get("uri")
        database = target.config.get("database")
        if not isinstance(uri, str) or not uri.strip():
            raise ValueError("MongoDB target requires a uri")
        if not isinstance(database, str) or not database.strip():
            raise ValueError("MongoDB target requires a database")
        self._target = target
        self._client = module.AsyncMongoClient(uri)
        self._database = self._client[database]
        self._jobs: dict[str, asyncio.Task[ExecutionResult]] = {}

    def capabilities(self) -> NoSQLCapabilities:
        return NoSQLCapabilities(
            cancellation=True,
            read_only_session=True,
            operation_timeout=True,
            document_limit=True,
            query_metrics=True,
            result_reference=True,
            out_merge_writes=True,
            destination_introspection=True,
            materialization_reference=True,
        )

    async def validate(
        self,
        pipeline: Pipeline,
        target: NoSQLTarget,
        context: Context,
        policy: NoSQLPolicy,
    ) -> ValidationResult:
        return ValidationResult.accepted()

    async def inspect_collection(
        self,
        reference: CollectionRef,
        target: NoSQLTarget,
        *,
        include_document_count: bool = False,
    ) -> CollectionSnapshot | None:
        names = await self._database.list_collection_names()
        if reference.name not in names:
            return None
        metadata: dict[str, object] = {}
        if include_document_count:
            metadata["document_count"] = await self._database[
                reference.name
            ].estimated_document_count()
        cursor = self._database[reference.name].aggregate([{"$sample": {"size": _SAMPLE_SIZE}}])
        sample = await cursor.to_list(length=_SAMPLE_SIZE)
        fields: set[str] = set()
        for document in sample:
            fields.update(document.keys())
        return CollectionSnapshot(reference.name, metadata, tuple(sorted(fields)))

    async def submit(
        self,
        pipeline: Pipeline,
        target: NoSQLTarget,
        context: Context,
    ) -> ExecutionHandle:
        policy = context.metadata.get("gantry.nosql.policy")
        if not isinstance(policy, NoSQLPolicy):
            raise ValueError("governed NoSQL policy is missing from execution context")
        collection = context.metadata.get("gantry.nosql.collection")
        if not isinstance(collection, str) or not collection.strip():
            raise ValueError("target collection is missing from execution context")
        gantry_id = f"run_{uuid4().hex}"
        metadata: dict[str, object] = {
            "database": target.config.get("database"),
            "collection": collection,
            "max_documents": policy.max_documents,
            "timeout_seconds": policy.timeout_seconds,
        }
        plan = context.metadata.get(_PLAN_METADATA)
        if isinstance(plan, MaterializationPlan):
            metadata.update(
                {
                    _DESTINATION_METADATA: plan.destination.name,
                    _OPERATION_METADATA: plan.operation.value,
                }
            )
        handle = ExecutionHandle(
            gantry_id=gantry_id,
            engine="nosql",
            target=target.provider,
            native_id=f"mongo_{uuid4().hex}",
            metadata=metadata,
        )
        self._jobs[gantry_id] = asyncio.create_task(
            self._execute(handle, collection, pipeline, policy)
        )
        return handle

    async def status(self, handle: ExecutionHandle) -> Execution:
        task = self._jobs.get(handle.gantry_id)
        if task is None:
            return Execution(handle, ExecutionState.UNKNOWN)
        if not task.done():
            return Execution(handle, ExecutionState.RUNNING)
        result = task.result()
        state = ExecutionState.SUCCEEDED if result.ok else ExecutionState.FAILED
        return Execution(handle, state, failure=result.failure)

    async def result(self, handle: ExecutionHandle) -> ExecutionResult:
        task = self._jobs.get(handle.gantry_id)
        if task is None:
            return ExecutionResult.failed(
                handle, Failure(FailureKind.UNKNOWN, False, "no MongoDB job found for handle")
            )
        return await task

    async def cancel(self, handle: ExecutionHandle, mode: str = "default") -> Execution:
        task = self._jobs.get(handle.gantry_id)
        if task is not None and not task.done():
            task.cancel()
        return Execution(
            handle,
            ExecutionState.CANCELLED,
            failure=Failure(FailureKind.CANCELLED, False, f"cancelled ({mode})"),
        )

    async def _execute(
        self,
        handle: ExecutionHandle,
        collection: str,
        pipeline: Pipeline,
        policy: NoSQLPolicy,
    ) -> ExecutionResult:
        started = asyncio.get_running_loop().time()
        stages = normalize_pipeline(pipeline)
        try:
            cursor = self._database[collection].aggregate(list(stages))
            documents = await asyncio.wait_for(
                cursor.to_list(length=policy.max_documents + 1),
                timeout=policy.timeout_seconds,
            )
        except TimeoutError:
            return ExecutionResult.failed(
                handle, Failure(FailureKind.TIMEOUT, True, "MongoDB aggregation timed out")
            )
        except asyncio.CancelledError:
            return ExecutionResult.failed(
                handle,
                Failure(FailureKind.CANCELLED, False, "MongoDB aggregation was cancelled"),
            )
        except Exception as error:
            return ExecutionResult.failed(
                handle,
                Failure(FailureKind.ENGINE_ERROR, False, str(error), native_message=str(error)),
            )
        runtime = asyncio.get_running_loop().time() - started
        truncated = len(documents) > policy.max_documents
        inline = InlineDocuments(
            tuple(_json_safe(doc) for doc in documents[: policy.max_documents]),
            truncated=truncated,
        )
        destination = handle.metadata.get(_DESTINATION_METADATA)
        if isinstance(destination, str):
            output = OutputRef(
                OutputKind.TABLE,
                f"mongodb://{destination}",
                metadata={"object_kind": "collection"},
            )
        else:
            output = OutputRef(
                OutputKind.INLINE,
                f"inline://{handle.gantry_id}",
                metadata={"inline": inline},
            )
        return ExecutionResult.succeeded(
            handle,
            outputs=(output,),
            metrics=ExecutionMetrics(rows_read=len(inline.documents), runtime_seconds=runtime),
        )


def _json_safe(value: object) -> Any:
    bson = importlib.import_module("bson")
    if isinstance(value, bson.ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bson.Decimal128):
        return str(value.to_decimal())
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
