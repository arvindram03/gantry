# SPDX-License-Identifier: Apache-2.0
"""The chaos suite's target, backed by a real job executor.

`FakeTarget` proves the *runtime's* guarantees — that a crash between commit and
checkpoint replays without duplicating — using a dict for idempotence. That is
the right shape for a fast unit run, but it means the guarantees are only ever
demonstrated against a fake.

This presents exactly the same surface, and implements it by compiling a
partition to a job, running it in a container, and letting PostgreSQL's merge
decide whether the effect was new. Same assertions, real executor. If the
generated job's idempotence is wrong, the existing chaos assertions fail — which
is the point of not writing new ones.

The job kind is a parameter, so the same scenarios run against `sql` and against
`beam` without either backend getting its own assertions.

The work runs on a private event loop in a background thread because
`FakeWorkload.execute` calls `apply` synchronously from inside the simulator's
loop; nothing here touches that loop.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, ClassVar, TypeVar

from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.jobs import JobState
from gantry.jobs.packaging import beam_packaging, sql_client_packaging
from gantry.jobs.runners import DockerRunner
from gantry.movement.beamjob import JDBC_SECRETS
from gantry.movement.beamjob import compile_snapshot_job as beam_compile_snapshot_job
from gantry.movement.partitioning import Partition, PartitionMethod
from gantry.movement.sqljob import SOURCE_DSN, TARGET_DSN, compile_snapshot_job
from gantry.state.database import create_engine, transaction
from sqlalchemy import text

T = TypeVar("T")

TABLE = "public.chaos_effects"
# One row per possible effect. The chaos plans have 11 nodes at most, and a
# node may be replayed but never produces a new key, so this is ample.
ROWS = 64


class JobBackedTarget:
    """An idempotent target whose idempotence is PostgreSQL's, not a dict's."""

    #: Filled in by the subclasses below.
    secrets: ClassVar[dict[str, str]] = {}

    def compile(self, manifest: Any, partition: Partition, *, network: str) -> Any:
        raise NotImplementedError

    def __init__(self, *, source_url: str, target_url: str, network: str) -> None:
        self.apply_calls = 0
        self.suppressed_duplicates = 0
        self.applied: dict[str, str] = {}
        self._ids: dict[str, int] = {}
        self._network = network
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._source = self._call(self._make(source_url))
        self._target = self._call(self._make(target_url))
        self._call(self._build())
        self._manifest = self._call(self._discover())

    # --- plumbing ----------------------------------------------------------

    def _call(self, coro: Coroutine[Any, Any, T]) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(180)

    async def _make(self, url: str) -> Any:
        return create_engine(url)

    async def _build(self) -> None:
        for engine in (self._source, self._target):
            async with transaction(engine) as connection:
                await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
                await connection.execute(
                    text(
                        f"CREATE TABLE {TABLE} (id bigint PRIMARY KEY, effect text NOT NULL,"
                        " source_lsn bigint NOT NULL DEFAULT 0)"
                    )
                )
        async with transaction(self._source) as connection:
            await connection.execute(
                text(
                    f"INSERT INTO {TABLE} SELECT g, 'effect-' || g, 0 "
                    f"FROM generate_series(1, {ROWS}) g"
                )
            )
            await connection.execute(text(f"ANALYZE {TABLE}"))

    async def _discover(self) -> Any:
        found = {m.name: m for m in await PostgresSourceAdapter(self._source).discover()}
        return found[TABLE]

    async def _count(self) -> int:
        async with transaction(self._target) as connection:
            return int(
                (await connection.execute(text(f"SELECT count(*) FROM {TABLE}"))).scalar_one()
            )

    async def _move(self, row_id: int) -> None:
        partition = Partition(
            dataset=TABLE,
            index=row_id,
            column="id",
            method=PartitionMethod.HISTOGRAM,
            lo=str(row_id),
            hi=str(row_id + 1),
        )
        job = self.compile(self._manifest, partition, network=self._network)
        runner = DockerRunner(secrets=self.secrets)
        handle = await runner.submit(job)
        try:
            deadline = asyncio.get_running_loop().time() + 300
            while asyncio.get_running_loop().time() < deadline:
                status = await runner.poll(handle)
                if status.state.terminal:
                    if status.state is JobState.FAILED:
                        raise AssertionError(f"the effect job failed: {status.detail}")
                    return
                await asyncio.sleep(0.05)
            raise AssertionError("the effect job never finished")
        finally:
            await runner._run(["docker", "rm", "-f", handle.id])

    # --- the FakeTarget surface --------------------------------------------

    def apply(self, key: str, value: str) -> bool:
        """Apply an effect. Returns True when it was new.

        The answer comes from the target's own row count either side of the
        job, so a replay is suppressed by the merge's ON CONFLICT rather than
        by bookkeeping here.
        """
        self.apply_calls += 1
        row_id = self._ids.setdefault(key, len(self._ids) + 1)
        before = self._call(self._count())
        self._call(self._move(row_id))
        after = self._call(self._count())
        if after == before:
            self.suppressed_duplicates += 1
            return False
        self.applied[key] = value
        return True

    @property
    def effect_count(self) -> int:
        return self._call(self._count())

    def close(self) -> None:
        async def shut() -> None:
            for engine in (self._source, self._target):
                async with transaction(engine) as connection:
                    await connection.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
                await engine.dispose()

        try:
            self._call(shut())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)


INSIDE_SOURCE = "postgresql://gantry:gantry@gantry-pg-source:5432/gantry"
INSIDE_TARGET = "postgresql://gantry:gantry@gantry-pg-target:5432/gantry"


class SqlJobTarget(JobBackedTarget):
    """The chaos scenarios against a generated SQL script."""

    secrets: ClassVar[dict[str, str]] = {SOURCE_DSN: INSIDE_SOURCE, TARGET_DSN: INSIDE_TARGET}

    def compile(self, manifest: Any, partition: Partition, *, network: str) -> Any:
        return compile_snapshot_job(
            "chaos",
            manifest,
            partition,
            target=TABLE,
            packaging=sql_client_packaging(secrets=(SOURCE_DSN, TARGET_DSN), network=network),
        )


class BeamJobTarget(JobBackedTarget):
    """The same scenarios against a generated Beam pipeline.

    One partition per job here, which is deliberately *not* how a real Beam
    Movement is executed — grouping is what makes it affordable. The chaos suite
    is asking whether a crash between commit and checkpoint duplicates an
    effect, and that question needs one effect per job to be legible. The cost
    of the answer is that this suite is slow; see docs/guarantees.md.
    """

    secrets: ClassVar[dict[str, str]] = {
        "GANTRY_SOURCE_JDBC": "jdbc:postgresql://gantry-pg-source:5432/gantry",
        "GANTRY_TARGET_JDBC": "jdbc:postgresql://gantry-pg-target:5432/gantry",
        "GANTRY_SOURCE_USER": "gantry",
        "GANTRY_SOURCE_PASSWORD": "gantry",
        "GANTRY_TARGET_USER": "gantry",
        "GANTRY_TARGET_PASSWORD": "gantry",
    }

    def compile(self, manifest: Any, partition: Partition, *, network: str) -> Any:
        return beam_compile_snapshot_job(
            "chaos",
            manifest,
            [partition],
            target=TABLE,
            packaging=beam_packaging(secrets=JDBC_SECRETS, network=network),
        )
