"""Verification against real databases.

Requires the local stack, a seeded source, and a completed snapshot:
    make dev-up && uv run gantry seed --rows 1000000
    uv run gantry start examples/postgres-to-postgres/movement.yaml

The exit criterion for the day: a deliberately corrupted target is caught,
attributed to the partition that holds the damage, and surfaced as evidence.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.core.dataset import DatasetManifest
from gantry.core.evidence import ScopeKind, VerificationStatus
from gantry.core.verification import (
    ForeignKeyIntegrityCheck,
    NullRateCheck,
    PrimaryKeyUniqueCheck,
    RowCountCheck,
    VerificationRequirement,
)
from gantry.movement.model import (
    Endpoint,
    Movement,
    MovementDataset,
    MovementMode,
    Ordering,
    OrderingScope,
    WriteMode,
)
from gantry.movement.partitioning import plan_partitions
from gantry.state.database import create_engine, transaction
from gantry.verification.runner import MovementVerificationRunner
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)
SOURCE_TABLE = "public.customers"
TARGET_TABLE = "public.customers_verify"


@pytest.fixture
async def engines() -> AsyncIterator[tuple[AsyncEngine, AsyncEngine]]:
    source = create_engine(SOURCE_URL)
    target = create_engine(TARGET_URL)
    try:
        # A fresh copy of the source, so a test can damage it freely.
        async with transaction(target) as connection:
            await connection.execute(text(f"DROP TABLE IF EXISTS {TARGET_TABLE}"))
        yield source, target
    finally:
        await source.dispose()
        await target.dispose()


async def manifest_of(source: AsyncEngine) -> DatasetManifest:
    adapter = PostgresSourceAdapter(source)
    manifests = {m.name: m for m in await adapter.discover()}
    assert SOURCE_TABLE in manifests, "seed the source first"
    return await adapter.profile(manifests[SOURCE_TABLE])


async def copy_source(engines: tuple[AsyncEngine, AsyncEngine]) -> DatasetManifest:
    """Materialise a target that matches the source exactly."""
    source, target = engines
    manifest = await manifest_of(source)
    async with transaction(target) as connection:
        await connection.execute(
            text(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM {SOURCE_TABLE}")
        )
        await connection.execute(text(f"ALTER TABLE {TARGET_TABLE} ADD PRIMARY KEY (customer_id)"))
    return manifest


def movement(*checks: VerificationRequirement) -> tuple[Movement, MovementDataset]:
    dataset = MovementDataset(
        name=SOURCE_TABLE,
        source=SOURCE_TABLE,
        target=TARGET_TABLE,
        key_columns=("customer_id",),
        ordering=Ordering(scope=OrderingScope.NONE),
        write_mode=WriteMode.UPSERT,
        verification=tuple(checks),
    )
    return (
        Movement(
            name="verification-test",
            source=Endpoint(adapter="postgres", connection_ref="source"),
            destination=Endpoint(adapter="postgres", connection_ref="target"),
            mode=MovementMode.SNAPSHOT,
            datasets=(dataset,),
        ),
        dataset,
    )


def runner(engines: tuple[AsyncEngine, AsyncEngine]) -> MovementVerificationRunner:
    source, target = engines
    return MovementVerificationRunner(source_engine=source, target_engine=target)


# --- an intact target passes ----------------------------------------------


async def test_an_intact_copy_passes(engines: tuple[AsyncEngine, AsyncEngine]) -> None:
    manifest = await copy_source(engines)
    spec, dataset = movement(RowCountCheck(), PrimaryKeyUniqueCheck())
    report = await runner(engines).verify_dataset(spec, dataset, manifest, plan_version=1)

    assert report.passed
    assert all(result.passed for result in report.results)


# --- the exit criterion ----------------------------------------------------


async def test_a_corrupted_target_is_caught_and_attributed(
    engines: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """Delete rows from one partition; verification names that partition."""
    _, target = engines
    manifest = await copy_source(engines)
    partitions = plan_partitions(manifest, target_partitions=8).partitions
    damaged = partitions[3]
    assert damaged.lo is not None and damaged.hi is not None

    async with transaction(target) as connection:
        deleted = (
            await connection.execute(
                text(
                    f"WITH doomed AS ("
                    f"  SELECT customer_id FROM {TARGET_TABLE} "
                    f"  WHERE customer_id >= :lo AND customer_id < :hi LIMIT 137"
                    f") DELETE FROM {TARGET_TABLE} t USING doomed d "
                    f"WHERE t.customer_id = d.customer_id RETURNING t.customer_id"
                ),
                {"lo": int(damaged.lo), "hi": int(damaged.hi)},
            )
        ).rowcount
    assert deleted == 137

    spec, dataset = movement(RowCountCheck())
    report = await runner(engines).verify_dataset(
        spec, dataset, manifest, plan_version=1, partitions=partitions
    )

    assert not report.passed, "a corrupted target must not verify"

    # The dataset-scope check says something is wrong.
    dataset_failures = [
        result for result in report.blocking if result.scope.kind is ScopeKind.DATASET
    ]
    assert len(dataset_failures) == 1
    assert "137 rows missing from target" in (dataset_failures[0].difference or "")

    # The partition-scope checks say where.
    partition_failures = [
        result for result in report.blocking if result.scope.kind is ScopeKind.PARTITION
    ]
    assert len(partition_failures) == 1, "exactly one partition should be implicated"
    assert partition_failures[0].scope.identifier == damaged.id
    assert str(partition_failures[0].scope) in report.failed_scopes
    # The scope reads cleanly rather than repeating the dataset name.
    assert str(partition_failures[0].scope) == f"partition/{SOURCE_TABLE}/00003"


async def test_evidence_records_what_each_side_reported(
    engines: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """A failure has to be actionable, not just true."""
    _, target = engines
    manifest = await copy_source(engines)
    async with transaction(target) as connection:
        await connection.execute(text(f"DELETE FROM {TARGET_TABLE} WHERE customer_id <= 50"))

    spec, dataset = movement(RowCountCheck())
    report = await runner(engines).verify_dataset(spec, dataset, manifest, plan_version=1)
    failure = report.blocking[0]

    assert failure.source_result == "1000000"
    assert failure.target_result == "999950"
    assert failure.difference == "50 rows missing from target"
    assert failure.evidence["predicate"] == "TRUE"
    assert failure.plan_version == 1
    assert failure.observed_at.tzinfo is not None


# --- the other verifiers ---------------------------------------------------


async def test_duplicate_keys_are_caught(engines: tuple[AsyncEngine, AsyncEngine]) -> None:
    """Duplicated keys mean the write path produced a second effect."""
    _, target = engines
    manifest = await copy_source(engines)
    async with transaction(target) as connection:
        await connection.execute(
            text(f"ALTER TABLE {TARGET_TABLE} DROP CONSTRAINT {TARGET_TABLE.split('.')[1]}_pkey")
        )
        await connection.execute(
            text(f"INSERT INTO {TARGET_TABLE} SELECT * FROM {TARGET_TABLE} WHERE customer_id <= 10")
        )

    spec, dataset = movement(PrimaryKeyUniqueCheck())
    report = await runner(engines).verify_dataset(spec, dataset, manifest, plan_version=1)

    assert not report.passed
    assert "10 duplicate keys" in (report.blocking[0].difference or "")


async def test_null_rate_within_bounds_passes(
    engines: tuple[AsyncEngine, AsyncEngine],
) -> None:
    manifest = await copy_source(engines)
    spec, dataset = movement(NullRateCheck(field="region", max=0.05))
    report = await runner(engines).verify_dataset(spec, dataset, manifest, plan_version=1)
    assert report.passed


async def test_null_rate_beyond_bounds_fails(
    engines: tuple[AsyncEngine, AsyncEngine],
) -> None:
    _, target = engines
    manifest = await copy_source(engines)
    async with transaction(target) as connection:
        await connection.execute(
            text(f"UPDATE {TARGET_TABLE} SET region = NULL WHERE customer_id <= 200000")
        )

    spec, dataset = movement(NullRateCheck(field="region", max=0.05))
    report = await runner(engines).verify_dataset(spec, dataset, manifest, plan_version=1)

    assert not report.passed
    assert "exceeds maximum 0.05" in (report.blocking[0].difference or "")


async def test_a_dataset_without_foreign_keys_skips_that_check(
    engines: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """A skip is recorded, not silently treated as a pass."""
    manifest = await copy_source(engines)
    spec, dataset = movement(ForeignKeyIntegrityCheck())
    report = await runner(engines).verify_dataset(spec, dataset, manifest, plan_version=1)

    assert report.passed
    assert report.results[0].status is VerificationStatus.SKIPPED


async def test_a_broken_check_errors_rather_than_passing(
    engines: tuple[AsyncEngine, AsyncEngine],
) -> None:
    """An unanswered question is not a satisfied one."""
    manifest = await copy_source(engines)
    spec, dataset = movement(NullRateCheck(field="nonexistent_column", max=0.1))
    report = await runner(engines).verify_dataset(spec, dataset, manifest, plan_version=1)

    assert not report.passed
    assert report.results[0].status is VerificationStatus.ERRORED
