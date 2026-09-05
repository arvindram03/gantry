# SPDX-License-Identifier: Apache-2.0
"""Durable Migration workflow state.

The store's job is to make a cutover accountable: every transition persisted,
every operator decision attributed, and no two actors advancing one workflow at
once. Those are the properties a post-mortem needs, so they are tested against
the real database rather than a fake.

Requires: make dev-up && uv run alembic upgrade head.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from gantry.lifecycle.migration import IllegalMigrationTransitionError, MigrationState
from gantry.lifecycle.states import ActorKind
from gantry.migration.model import Migration
from gantry.state.database import create_engine, transaction
from gantry.state.migrations import (
    ApprovalRequiredError,
    ConcurrentMigrationUpdateError,
    MigrationRecord,
    MigrationStore,
    UnattributedDecisionError,
    UnknownMigrationError,
)
from gantry.state.tables import migration_transitions, migrations
from sqlalchemy import delete

pytestmark = pytest.mark.integration

META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)
NAME = "store-test-migration"


@pytest.fixture
async def store() -> AsyncIterator[MigrationStore]:
    engine = create_engine(META_URL)

    async def clear() -> None:
        async with transaction(engine) as connection:
            await connection.execute(
                delete(migration_transitions).where(migration_transitions.c.migration == NAME)
            )
            await connection.execute(delete(migrations).where(migrations.c.name == NAME))

    try:
        await clear()
        yield MigrationStore(engine)
    finally:
        await clear()
        await engine.dispose()


def migration(**overrides: object) -> Migration:
    base: dict[str, object] = {"name": NAME, "movements": ("orders-snapshot",)}
    base.update(overrides)
    return Migration.model_validate(base)


async def walk_to(store: MigrationStore, target: MigrationState) -> None:
    path = [
        MigrationState.DISCOVERING,
        MigrationState.PLANNED,
        MigrationState.PREPARING,
        MigrationState.SNAPSHOTTING,
        MigrationState.CATCHING_UP,
        MigrationState.VERIFYING,
        MigrationState.READY_FOR_CUTOVER,
    ]
    for state in path:
        await store.transition(NAME, state, actor=ActorKind.RUNTIME, reason="test setup")
        if state is target:
            return


async def test_a_migration_starts_in_draft(store: MigrationStore) -> None:
    record = await store.ensure(migration())
    assert record.state is MigrationState.DRAFT
    assert record.movements == ("orders-snapshot",)


async def test_resubmitting_a_spec_does_not_reset_a_running_workflow(
    store: MigrationStore,
) -> None:
    """A cutover partway through must not be rewound by someone re-applying
    the same file."""
    await store.ensure(migration())
    await walk_to(store, MigrationState.SNAPSHOTTING)

    again = await store.ensure(migration())
    assert again.state is MigrationState.SNAPSHOTTING


async def test_the_spec_round_trips(store: MigrationStore) -> None:
    await store.ensure(migration(description="move orders"))
    assert (await store.spec(NAME)).description == "move orders"


async def test_an_unknown_migration_is_named(store: MigrationStore) -> None:
    with pytest.raises(UnknownMigrationError, match="no-such-migration"):
        await store.get("no-such-migration")


class TestTransitionsAreRecorded:
    async def test_every_transition_lands_in_the_history(self, store: MigrationStore) -> None:
        await store.ensure(migration())
        await walk_to(store, MigrationState.PLANNED)

        history = await store.history(NAME)
        assert [entry.to_state for entry in history] == [
            MigrationState.DISCOVERING,
            MigrationState.PLANNED,
        ]
        assert all(entry.actor is ActorKind.RUNTIME for entry in history)

    async def test_an_illegal_transition_never_reaches_the_trail(
        self, store: MigrationStore
    ) -> None:
        """Validation happens before the write, so the audit trail records
        what happened rather than what was attempted and rejected."""
        await store.ensure(migration())

        with pytest.raises(IllegalMigrationTransitionError):
            await store.transition(
                NAME, MigrationState.CUTTING_OVER, actor=ActorKind.OPERATOR, reason="no"
            )

        assert await store.history(NAME) == ()
        assert (await store.get(NAME)).state is MigrationState.DRAFT


class TestOperatorDecisions:
    async def test_an_agent_cannot_cut_over(self, store: MigrationStore) -> None:
        """Agents propose; they do not execute. The refusal is in the
        transition function, not in a policy that could be relaxed."""
        await store.ensure(migration())
        await walk_to(store, MigrationState.READY_FOR_CUTOVER)

        with pytest.raises(ApprovalRequiredError, match="operator decision"):
            await store.transition(
                NAME, MigrationState.CUTTING_OVER, actor=ActorKind.AGENT, reason="looks fine"
            )

    async def test_the_runtime_cannot_cut_over_either(self, store: MigrationStore) -> None:
        """Not only models. Moving production traffic is a human decision."""
        await store.ensure(migration())
        await walk_to(store, MigrationState.READY_FOR_CUTOVER)

        with pytest.raises(ApprovalRequiredError):
            await store.transition(
                NAME, MigrationState.CUTTING_OVER, actor=ActorKind.RUNTIME, reason="gates green"
            )

    async def test_an_operator_decision_must_name_who_decided(self, store: MigrationStore) -> None:
        """A cutover approved by 'operator' and nobody in particular is not an
        approval, it is a checkbox."""
        await store.ensure(migration())
        await walk_to(store, MigrationState.READY_FOR_CUTOVER)

        with pytest.raises(UnattributedDecisionError, match="actor_id"):
            await store.transition(
                NAME, MigrationState.CUTTING_OVER, actor=ActorKind.OPERATOR, reason="approved"
            )

    async def test_an_attributed_operator_decision_is_recorded_with_its_evidence(
        self, store: MigrationStore
    ) -> None:
        await store.ensure(migration())
        await walk_to(store, MigrationState.READY_FOR_CUTOVER)

        await store.transition(
            NAME,
            MigrationState.CUTTING_OVER,
            actor=ActorKind.OPERATOR,
            actor_id="arvind",
            reason="gates green, approved in #migrations",
            evidence={"maxCdcLag": "0.8s", "allPartitionsVerified": "true"},
        )

        entry = (await store.history(NAME))[-1]
        assert entry.actor is ActorKind.OPERATOR
        assert entry.actor_id == "arvind"
        assert entry.evidence == {"maxCdcLag": "0.8s", "allPartitionsVerified": "true"}


async def test_two_actors_cannot_both_advance_one_migration(
    store: MigrationStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lost update on a workflow that moves production traffic.

    Simulated by holding a read from before another actor's write, which is
    exactly what a second process would have.
    """
    await store.ensure(migration())
    stale = await store.get(NAME)

    await store.transition(
        NAME, MigrationState.DISCOVERING, actor=ActorKind.RUNTIME, reason="the other actor"
    )

    async def stale_read(name: str) -> MigrationRecord:
        return stale

    monkeypatch.setattr(store, "get", stale_read)
    with pytest.raises(ConcurrentMigrationUpdateError, match="re-read it and retry"):
        await store.transition(
            NAME, MigrationState.DISCOVERING, actor=ActorKind.RUNTIME, reason="this actor"
        )
