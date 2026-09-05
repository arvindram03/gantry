"""A worker in its own process, so a test can kill it for real.

Invoked as a subprocess by the crash-replay test. It prints one line per
completed node to stdout, so the test can wait until real work is in flight
before sending SIGKILL - killing before the first COPY would prove nothing.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from build_plan import PLAN_AT, SOURCE_TABLE, TARGET_TABLE, build_movement
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.core.commit import CommitResult
from gantry.lifecycle.plan import PlanNode
from gantry.movement.executor import MovementExecutor
from gantry.movement.planner import compile_movement
from gantry.scheduler.postgres import PostgresWorkflowBackend
from gantry.scheduler.worker import Worker
from gantry.state.checkpoints import PostgresCheckpointStore
from gantry.state.database import create_engine


async def main(name: str, source_url: str, target_url: str, meta_url: str) -> None:
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    meta_engine = create_engine(meta_url)

    try:
        source = PostgresSourceAdapter(source_engine)
        manifests = {m.name: await source.profile(m) for m in await source.discover()}
        movement = build_movement()
        plan = compile_movement(movement, created_at=PLAN_AT, manifests=manifests)

        executor = MovementExecutor(
            source_engine=source_engine,
            target_engine=target_engine,
            manifests=manifests,
            targets={SOURCE_TABLE: TARGET_TABLE},
        )
        backend = PostgresWorkflowBackend(meta_engine, plan.operation)
        await backend.submit(plan)

        # A starting worker reclaims whatever an earlier one abandoned. Nobody
        # has to have noticed the crash - the lease simply ran out.
        reclaimed = await backend.reclaim_expired(now=datetime.now(UTC))
        print(f"RECLAIMED {reclaimed}", flush=True)

        worker = Worker(
            name,
            backend,
            PostgresCheckpointStore(meta_engine),
            _Reporting(executor),
            plan,
            clock=lambda: datetime.now(UTC),
            lease=timedelta(seconds=float(os.environ.get("GANTRY_LEASE_SECONDS", "30"))),
        )
        report = await worker.run()
        print(f"DONE completed={len(report.completed)} rows={report.rows_written}", flush=True)
    finally:
        await source_engine.dispose()
        await target_engine.dispose()
        await meta_engine.dispose()


class _Reporting:
    """Announces each node as it commits, so the test can time its kill."""

    def __init__(self, inner: MovementExecutor) -> None:
        self._inner = inner

    async def execute(self, node: PlanNode) -> CommitResult:
        result = await self._inner.execute(node)
        print(f"COMMITTED {node.id} rows={result.rows_changed}", flush=True)
        return result


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))
