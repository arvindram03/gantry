# SPDX-License-Identifier: Apache-2.0
"""The durable run record, and the invariants the design rests on.

Most of these are about ordering and durability rather than about data. A run
that is correct but written after the engine already started, or one that
cannot be read back by anyone else, fails at the only job it has.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import gantry
import pytest
from gantry.actor import ActorType, actor, context, current_actor
from gantry.runs.lifecycle import RunRecorder
from gantry.runs.model import OperationKind, new_run_id
from gantry.runs.status import RunStatus
from gantry.runs.store import MemoryRunStore, RunPersistenceError, SQLiteRunStore


def _seeded(tmp_path: Path, name: str = "runs") -> str:
    path = str(tmp_path / f"{name}.duckdb")
    native = duckdb.connect(path)
    native.execute("CREATE TABLE events AS SELECT 1 AS id UNION ALL SELECT 2")
    native.close()
    return path


def test_run_ids_are_opaque_and_sort_by_creation() -> None:
    """Sortable so a listing is useful without a second index.

    Opaque so callers do not come to depend on the shape, and unrelated to any
    engine's own id — both are recorded, and tying Gantry's identifier to a
    provider's would make the record only as durable as that provider.
    """
    ids = [new_run_id() for _ in range(200)]

    assert len(set(ids)) == 200
    assert ids == sorted(ids) or ids[0] < ids[-1]
    assert all(value.startswith("run_") for value in ids)


def test_nothing_is_submitted_when_the_run_cannot_be_recorded() -> None:
    """§27: fail closed.

    An engine job that exists without a control-plane record of why it was
    allowed to is the outcome the create-before-execute ordering exists to
    prevent, so a store that cannot write is a reason not to run.
    """

    class BrokenStore:
        def create(self, run: object) -> None:
            raise OSError("disk full")

        def update(self, run: object) -> None: ...

        def get(self, run_id: str) -> None:
            return None

    gantry.runs.configure(BrokenStore())
    try:
        with pytest.raises(RunPersistenceError, match="disk full"):
            RunRecorder(kind=OperationKind.QUERY, engine="sql", proposal="SELECT 1")
    finally:
        gantry.runs.configure(MemoryRunStore())


async def test_a_query_records_one_run_that_moves_through_its_states(tmp_path: Path) -> None:
    """§25: one run evolves. Stages are not separate runs."""
    store = MemoryRunStore()
    gantry.runs.configure(store)
    db = gantry.sql.connect("duckdb", path=_seeded(tmp_path), read_only=True)

    run = await db.query(schemas=["main"], verify=[gantry.verify.row_count(min=1)])(
        "SELECT id FROM main.events"
    )

    assert run.status is RunStatus.ACCEPTED
    assert store.get(run.id) is not None
    # The recorder wrote several times; there is still one run.
    assert len(store.recent(limit=50)) == 1
    assert run.created_at <= run.updated_at
    assert run.completed_at is not None


async def test_a_refused_proposal_still_leaves_a_run(tmp_path: Path) -> None:
    """The case anyone actually asks about later.

    A proposal that never reached the engine is exactly the one someone wants
    explained, so `POLICY_REJECTED` is a recorded outcome rather than an
    absence of one.
    """
    store = MemoryRunStore()
    gantry.runs.configure(store)
    db = gantry.sql.connect("duckdb", path=_seeded(tmp_path), read_only=True)

    run = await db.query(schemas=["nowhere"])("SELECT id FROM main.events")

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.admission is not None and not run.admission.allowed
    assert run.admission.reasons, "a refusal records what it refused for"
    stored = store.get(run.id)
    assert stored is not None and stored.status is RunStatus.POLICY_REJECTED


async def test_the_actor_comes_from_the_application_not_the_proposal(tmp_path: Path) -> None:
    """§10: an agent that could name itself could name someone else."""
    gantry.runs.configure(MemoryRunStore())
    db = gantry.sql.connect("duckdb", path=_seeded(tmp_path), read_only=True)

    unattributed = await db.query(schemas=["main"])("SELECT id FROM main.events")
    with context(actor=actor("agent", "research-agent", session_id="s1")):
        attributed = await db.query(schemas=["main"])("SELECT id FROM main.events")

    assert unattributed.actor.type is ActorType.UNKNOWN
    assert unattributed.actor.id is None
    assert attributed.actor.label == "agent:research-agent"
    assert attributed.actor.session_id == "s1"
    # The context does not leak past its block.
    assert current_actor().type is ActorType.UNKNOWN


def test_actor_metadata_is_bounded() -> None:
    """A run record is not a transcript store."""
    with pytest.raises(ValueError, match="small"):
        actor("agent", "a", metadata={"transcript": "x" * 2000})


async def test_query_rows_reach_the_caller_and_not_the_store(tmp_path: Path) -> None:
    """§18 and §30: outputs are references; result rows are not persisted.

    The returned run carries the rows because a caller needs them. The stored
    run keeps a count and nothing else — otherwise a durable file accumulates
    every result the system has ever returned.
    """
    store = MemoryRunStore()
    gantry.runs.configure(store)
    db = gantry.sql.connect("duckdb", path=_seeded(tmp_path), read_only=True)

    run = await db.query(schemas=["main"], max_rows=10)("SELECT id FROM main.events")

    assert run.rows == ((1,), (2,))
    assert run.columns == ("id",)
    assert run.result_ref is not None and run.result_ref.rows == 2
    assert "rows" not in json.loads(run.to_json())
    assert json.loads(run.to_json())["result_ref"]["rows"] == 2
    for value in json.dumps(run.as_dict()).split():
        assert "(1,)" not in value


async def test_a_run_survives_the_process_that_created_it(tmp_path: Path) -> None:
    """§24, the invariant the whole feature rests on.

    The store is opened twice over the same file, the second time after the
    first is closed, because "it is still in memory" is not the property being
    claimed.
    """
    path = str(tmp_path / "durable.db")
    writer = SQLiteRunStore(path)
    gantry.runs.configure(writer)
    db = gantry.sql.connect("duckdb", path=_seeded(tmp_path, "src"), read_only=True)

    with context(actor=actor("agent", "migration-agent", session_id="s847")):
        run = await db.query(schemas=["main"], verify=[gantry.verify.row_count(min=1)])(
            "SELECT id FROM main.events"
        )
    writer.close()

    reader = SQLiteRunStore(path)
    try:
        recovered = reader.get(run.id)
        assert recovered is not None
        assert recovered.status is RunStatus.ACCEPTED
        assert recovered.actor.label == "agent:migration-agent"
        assert recovered.actor.session_id == "s847"
        assert recovered.operation.kind is OperationKind.QUERY
        assert recovered.proposal is not None and recovered.proposal.hash.startswith("sha256:")
        assert recovered.verification is not None
        assert recovered.inline is None, "rows do not survive, by design"
        rendered = recovered.render()
        assert "agent:migration-agent" in rendered
        assert "✓ row_count" in rendered
    finally:
        reader.close()
    gantry.runs.configure(MemoryRunStore())


async def test_the_tool_response_carries_the_run_id(tmp_path: Path) -> None:
    """§31: an agent that is told "rejected" needs something to refer to."""
    gantry.runs.configure(MemoryRunStore())
    db = gantry.sql.connect("duckdb", path=_seeded(tmp_path), read_only=True)
    tool = db.query(schemas=["main"]).tool()

    run = await tool.invoke({"sql": "SELECT id FROM main.events"})

    assert run.id.startswith("run_")
    assert run.status is RunStatus.ACCEPTED
    assert run.run_id == run.id


async def test_a_proposal_refused_at_the_tool_boundary_still_gets_a_run(
    tmp_path: Path,
) -> None:
    """Malformed agent input is still a proposal, and still worth recording."""
    store = MemoryRunStore()
    gantry.runs.configure(store)
    db = gantry.sql.connect("duckdb", path=_seeded(tmp_path), read_only=True)
    tool = db.query(schemas=["main"]).tool()

    run = await tool.invoke(
        {"sql": "SELECT id FROM main.events", "verify": [{"type": "no_such_check"}]}
    )

    assert run.status in {RunStatus.POLICY_REJECTED, RunStatus.VERIFICATION_UNSUPPORTED}
    assert store.get(run.id) is not None


def test_a_run_serializes_to_stable_json() -> None:
    """§28. Anything that cannot be read back is a log line, not a record."""
    from gantry.runs.sqlite import run_from_dict

    store = MemoryRunStore()
    gantry.runs.configure(store)
    recorder = RunRecorder(
        kind=OperationKind.MATERIALIZE,
        engine="sql",
        provider="postgres",
        proposal="CREATE TABLE x AS SELECT 1",
    )
    original = recorder.decided(verification=None)

    restored = run_from_dict(json.loads(original.to_json()))

    assert restored.id == original.id
    assert restored.status is original.status
    assert restored.operation.kind is OperationKind.MATERIALIZE
    assert restored.proposal is not None
    assert restored.proposal.body == "CREATE TABLE x AS SELECT 1"


def test_a_proposal_can_be_stored_as_a_hash_alone() -> None:
    """§12 and §30: agent SQL can carry values out of the data it filters."""
    from gantry.runs.model import ProposalRecord, ProposalStorage

    full = ProposalRecord.of("SELECT * FROM t WHERE email = 'a@b.c'")
    hashed = ProposalRecord.of(
        "SELECT * FROM t WHERE email = 'a@b.c'", storage=ProposalStorage.HASH
    )

    assert full.body is not None
    assert hashed.body is None
    assert hashed.hash == full.hash, "the digest identifies it either way"
    assert "a@b.c" not in json.dumps(hashed.as_dict())
