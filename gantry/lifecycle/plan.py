# SPDX-License-Identifier: Apache-2.0
"""Immutable, content-addressed execution plans.

A plan is compiled from a domain Operation, never from a spec. That matters:
design document section 8.5 requires a replay to retain the same PlanVersion,
so a purely cosmetic change to YAML field names must not invalidate plans for
running operations. Hashing the domain model rather than the spec is what makes
that true.

Node identifiers are derived from what a node *is* - its operation, kind and
scope - not from its position in a list. Reordering or inserting nodes
therefore leaves existing node identities untouched, which is what allows a
replay to resume against the checkpoints it already has.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ContentHash, ResourceName
from gantry.core.operation import LifecycleStage, OperationType
from gantry.core.positions import CheckpointScope


class NodeKind(StrEnum):
    """Plan node kinds, from the design document's execution plan section."""

    DISCOVER = "discover"
    PROFILE = "profile"
    CREATE_SCHEMA = "create_schema"
    SNAPSHOT_PARTITION = "snapshot_partition"
    START_CDC = "start_cdc"
    APPLY_CDC = "apply_cdc"
    WAIT_FOR_LAG = "wait_for_lag"
    VERIFY_DATASET = "verify_dataset"
    RECONCILE = "reconcile"
    GENERATE_ARTIFACT = "generate_artifact"
    VALIDATE_ARTIFACT = "validate_artifact"
    EXECUTE_ARTIFACT = "execute_artifact"
    VERIFY_RESULT = "verify_result"
    FINALIZE = "finalize"


def node_id(operation: str, kind: NodeKind, scope: str | None = None) -> str:
    """Deterministic identifier for a plan node.

    Stable across runs, machines and Python versions: a truncated SHA-256 over
    the node's identity rather than `hash()`, which is randomised per process.
    """
    material = f"{operation}|{kind.value}|{scope or ''}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


class PlanNode(BaseModel):
    """One unit of planned work."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str
    kind: NodeKind
    stage: LifecycleStage
    depends_on: tuple[str, ...] = ()
    scope: str | None = None
    params: dict[str, str] = {}

    @model_validator(mode="after")
    def _check_self_dependency(self) -> PlanNode:
        if self.id in self.depends_on:
            raise ValueError(f"node {self.id} cannot depend on itself")
        return self


class PlanVersion(BaseModel):
    """An immutable compiled plan.

    `created_at` is deliberately outside the content hash: two compilations of
    the same Operation must produce the same hash regardless of when they ran.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: ResourceName
    operation_type: OperationType
    version: int = Field(ge=1)
    nodes: tuple[PlanNode, ...] = Field(min_length=1)
    guarantee_fingerprint: ContentHash
    created_at: datetime

    @model_validator(mode="after")
    def _check_graph(self) -> PlanVersion:
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

        ids = [node.id for node in self.nodes]
        if len(set(ids)) != len(ids):
            duplicates = sorted({i for i in ids if ids.count(i) > 1})
            raise ValueError(f"duplicate node ids: {duplicates}")

        known = set(ids)
        for node in self.nodes:
            unknown = [dep for dep in node.depends_on if dep not in known]
            if unknown:
                raise ValueError(f"node {node.id} depends on unknown nodes: {sorted(unknown)}")

        self._check_acyclic()
        return self

    def _check_acyclic(self) -> None:
        edges = {node.id: set(node.depends_on) for node in self.nodes}
        resolved: set[str] = set()
        while True:
            ready = {name for name, deps in edges.items() if deps <= resolved}
            if ready == resolved:
                break
            resolved = ready
        unresolved = sorted(set(edges) - resolved)
        if unresolved:
            raise ValueError(f"plan contains a dependency cycle: {unresolved}")

    def canonical_json(self) -> str:
        """Stable serialisation used for content addressing."""
        payload = {
            "operation": self.operation,
            "operation_type": self.operation_type.value,
            "guarantee_fingerprint": self.guarantee_fingerprint,
            "nodes": [
                {
                    "id": node.id,
                    "kind": node.kind.value,
                    "stage": node.stage.value,
                    "depends_on": sorted(node.depends_on),
                    "scope": node.scope,
                    "params": dict(sorted(node.params.items())),
                }
                for node in self.nodes
            ],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @property
    def content_hash(self) -> ContentHash:
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    def node(self, identifier: str) -> PlanNode | None:
        return next((node for node in self.nodes if node.id == identifier), None)

    def topological_order(self) -> tuple[PlanNode, ...]:
        """Nodes in a deterministic dependency-respecting order.

        Ties are broken by node id so the order is reproducible rather than
        merely valid.
        """
        by_id = {node.id: node for node in self.nodes}
        remaining = {node.id: set(node.depends_on) for node in self.nodes}
        ordered: list[PlanNode] = []
        while remaining:
            ready = sorted(name for name, deps in remaining.items() if not deps)
            if not ready:  # pragma: no cover - validation rejects cycles
                raise ValueError("plan contains a dependency cycle")
            for name in ready:
                ordered.append(by_id[name])
                del remaining[name]
            for deps in remaining.values():
                deps.difference_update(ready)
        return tuple(ordered)


class ReplanRequiredError(Exception):
    """Raised when a change alters guarantees that are immutable within a plan.

    Design document section 10: source and target identity, ordering guarantee,
    migration key and verification requirements cannot change inside a plan
    version. Concurrency and rate limits can.
    """

    def __init__(self, operation: str, previous: str, proposed: str) -> None:
        super().__init__(
            f"operation {operation!r} changed guarantees that are immutable within a "
            f"plan version ({previous[:19]}... -> {proposed[:19]}...); an explicit "
            f"replan is required"
        )
        self.operation = operation


def next_version(
    previous: PlanVersion | None,
    proposed_fingerprint: ContentHash,
    *,
    proposed_content: ContentHash | None = None,
    replan: bool = False,
) -> int:
    """Decide the version number for a newly compiled plan.

    Three cases, and the third is the one that matters in practice:

    - Nothing changed: reuse the version. Recompiling is free and idempotent.
    - Immutable guarantees changed: refuse unless the caller asked to replan.
      Source and target identity, ordering, key and verification requirements
      cannot change inside a version (design document section 10).
    - Only content changed - partition bounds moved because the data moved:
      allocate the next version. This is ordinary. Bounds are content, not a
      guarantee, and a plan that has been stored is immutable: workers
      reconstruct plans from the store rather than recompiling, so rewriting
      version 1 with new bounds would change what a running worker believes
      it is executing.

    Passing no `proposed_content` keeps the old behaviour of comparing
    guarantees alone.
    """
    if previous is None:
        return 1
    if previous.guarantee_fingerprint != proposed_fingerprint:
        if not replan:
            raise ReplanRequiredError(
                previous.operation, previous.guarantee_fingerprint, proposed_fingerprint
            )
        return previous.version + 1
    if proposed_content is not None and previous.content_hash != proposed_content:
        return previous.version + 1
    return previous.version


def checkpoint_scope_for(kind: NodeKind) -> CheckpointScope:
    """The scope a checkpoint over this node covers.

    Every node used to checkpoint as `partition`, which made a dataset-level
    verify and a single partition copy indistinguishable in the trail. A
    checkpoint is evidence, and evidence labelled with the wrong scope answers
    a question nobody asked.
    """
    if kind is NodeKind.SNAPSHOT_PARTITION:
        return CheckpointScope.PARTITION
    if kind in (NodeKind.START_CDC, NodeKind.APPLY_CDC, NodeKind.WAIT_FOR_LAG):
        return CheckpointScope.STREAM
    # Discovery and profiling cover the source, not any one Dataset - they are
    # the nodes that decide what the Datasets are.
    if kind in (NodeKind.DISCOVER, NodeKind.PROFILE):
        return CheckpointScope.OPERATION
    return CheckpointScope.DATASET
