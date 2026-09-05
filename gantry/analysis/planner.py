# SPDX-License-Identifier: Apache-2.0
"""Compile an Analysis into an execution plan.

The shape is the lifecycle itself: generate an engine artifact, validate it
before it runs, execute it, then verify the output. An Analysis that skipped
straight from generation to execution would sit outside the guarantee boundary.
"""

from __future__ import annotations

from datetime import datetime

from gantry.analysis.model import Analysis
from gantry.core.operation import LifecycleStage, OperationType
from gantry.lifecycle.plan import NodeKind, PlanNode, PlanVersion, node_id


def compile_analysis(analysis: Analysis, *, created_at: datetime, version: int = 1) -> PlanVersion:
    """Build the deterministic plan for an Analysis."""
    generate = PlanNode(
        id=node_id(analysis.name, NodeKind.GENERATE_ARTIFACT),
        kind=NodeKind.GENERATE_ARTIFACT,
        stage=LifecycleStage.GENERATE,
        params={"engine": analysis.engine.value, "inputs": ",".join(analysis.inputs)},
    )
    validate = PlanNode(
        id=node_id(analysis.name, NodeKind.VALIDATE_ARTIFACT),
        kind=NodeKind.VALIDATE_ARTIFACT,
        stage=LifecycleStage.VALIDATE,
        depends_on=(generate.id,),
    )
    execute = PlanNode(
        id=node_id(analysis.name, NodeKind.EXECUTE_ARTIFACT),
        kind=NodeKind.EXECUTE_ARTIFACT,
        stage=LifecycleStage.EXECUTE,
        depends_on=(validate.id,),
    )
    verify = PlanNode(
        id=node_id(analysis.name, NodeKind.VERIFY_RESULT),
        kind=NodeKind.VERIFY_RESULT,
        stage=LifecycleStage.VERIFY,
        depends_on=(execute.id,),
        params={"checks": ",".join(sorted(c.check.value for c in analysis.verification))},
    )
    finalize = PlanNode(
        id=node_id(analysis.name, NodeKind.FINALIZE),
        kind=NodeKind.FINALIZE,
        stage=LifecycleStage.RESULT,
        depends_on=(verify.id,),
    )

    return PlanVersion(
        operation=analysis.name,
        operation_type=OperationType.ANALYSIS,
        version=version,
        nodes=(generate, validate, execute, verify, finalize),
        guarantee_fingerprint=analysis.guarantee_fingerprint(),
        created_at=created_at,
    )
