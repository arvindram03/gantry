# SPDX-License-Identifier: Apache-2.0
"""Classify MongoDB filter dicts / aggregation pipelines for policy checks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

Pipeline = Mapping[str, object] | Sequence[Mapping[str, object]]

_READ_STAGES = frozenset({"$match", "$project", "$group", "$sort", "$limit", "$unwind", "$lookup"})
_WRITE_STAGES = frozenset({"$out", "$merge"})
_MERGE_WHEN_MATCHED = frozenset({"merge", "replace"})


class PipelineOperation(StrEnum):
    READ = "READ"
    WRITE = "WRITE"


@dataclass(frozen=True, slots=True)
class CollectionRef:
    name: str

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("collection name must not be empty")


@dataclass(frozen=True, slots=True)
class PipelineClassification:
    operation: PipelineOperation
    read_only: bool
    collections: tuple[CollectionRef, ...] = ()
    write_destination: CollectionRef | None = None
    write_stage: str | None = None
    stage_count: int = 0


def normalize_pipeline(pipeline: Pipeline) -> tuple[Mapping[str, object], ...]:
    if isinstance(pipeline, (str, bytes)):  # type: ignore[unreachable]
        raise TypeError("pipeline must be a mapping filter or a sequence of stage mappings")
    if isinstance(pipeline, Mapping):
        return ({"$match": dict(pipeline)},)
    stages = tuple(pipeline)
    if any(not isinstance(stage, Mapping) for stage in stages):
        raise TypeError("every pipeline stage must be a mapping")
    return stages


def classify_pipeline(
    collection: str, pipeline: Pipeline, *, database: str | None = None
) -> PipelineClassification:
    if not collection.strip():
        raise ValueError("collection name must not be empty")
    stages = normalize_pipeline(pipeline)
    collections: dict[str, CollectionRef] = {collection.lower(): CollectionRef(collection)}
    write_destination: CollectionRef | None = None
    write_stage: str | None = None

    for index, stage in enumerate(stages):
        if len(stage) != 1:
            raise ValueError(f"pipeline stage {index} must have exactly one operator")
        operator, body = next(iter(stage.items()))
        if operator in _WRITE_STAGES:
            if index != len(stages) - 1:
                raise ValueError(f"write stage {operator} must be the last stage in the pipeline")
            write_stage = operator
            write_destination = _write_destination(operator, body, database=database)
            collections[write_destination.name.lower()] = write_destination
        elif operator not in _READ_STAGES:
            raise ValueError(f"unsupported pipeline stage: {operator}")
        elif operator == "$lookup":
            for reference in _lookup_references(body):
                collections[reference.name.lower()] = reference

    operation = PipelineOperation.WRITE if write_stage is not None else PipelineOperation.READ
    return PipelineClassification(
        operation=operation,
        read_only=write_stage is None,
        collections=tuple(collections.values()),
        write_destination=write_destination,
        write_stage=write_stage,
        stage_count=len(stages),
    )


def _lookup_references(body: object) -> tuple[CollectionRef, ...]:
    if not isinstance(body, Mapping) or "from" not in body:
        raise ValueError("$lookup stage must be a mapping with a 'from' collection")
    from_value = body["from"]
    if not isinstance(from_value, str):
        raise TypeError("$lookup 'from' must be a collection name")
    references = (CollectionRef(from_value),)
    sub_pipeline = body.get("pipeline")
    if sub_pipeline is None:
        return references
    return references + _sub_pipeline_references(sub_pipeline)


def _sub_pipeline_references(pipeline: object) -> tuple[CollectionRef, ...]:
    if isinstance(pipeline, (str, bytes)) or not isinstance(pipeline, Sequence):
        raise TypeError("$lookup 'pipeline' must be a sequence of stage mappings")
    references: list[CollectionRef] = []
    for stage in pipeline:
        if not isinstance(stage, Mapping) or len(stage) != 1:
            raise ValueError("every $lookup sub-pipeline stage must have exactly one operator")
        operator, stage_body = next(iter(stage.items()))
        if operator not in _READ_STAGES:
            raise ValueError(f"unsupported $lookup sub-pipeline stage: {operator}")
        if operator == "$lookup":
            references.extend(_lookup_references(stage_body))
    return tuple(references)


def _write_destination(
    operator: str, body: object, *, database: str | None = None
) -> CollectionRef:
    if operator == "$out":
        if isinstance(body, str):
            return CollectionRef(body)
        if isinstance(body, Mapping) and isinstance(body.get("coll"), str):
            out_db = body.get("db")
            if out_db is not None and out_db != database:
                raise ValueError(
                    f"$out must target the connection's own database, got db={out_db!r}"
                )
            return CollectionRef(body["coll"])
        raise ValueError("$out must be a collection name or a mapping with 'coll'")
    if isinstance(body, str):
        return CollectionRef(body)
    if isinstance(body, Mapping) and isinstance(body.get("into"), str):
        when_matched = body.get("whenMatched", "merge")
        if when_matched not in _MERGE_WHEN_MATCHED:
            raise ValueError(
                f"$merge whenMatched={when_matched!r} is not allowed; "
                f"use one of {sorted(_MERGE_WHEN_MATCHED)}"
            )
        return CollectionRef(body["into"])
    raise ValueError("$merge must name a destination via a string or 'into'")
