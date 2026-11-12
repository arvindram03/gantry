# SPDX-License-Identifier: Apache-2.0
"""What the library owes anyone embedding it.

Three gaps found by building a service on top of Gantry rather than by reading
it: a run could be written but not read back without reaching into the SQLite
store, a provider preset carried nothing provider-specific so every caller
re-derived it, and every integration wrote its own idea of what a model should
be told about a run.
"""

from __future__ import annotations

import json

import gantry
import pytest
from gantry.failure import Failure, FailureKind
from gantry.runs.model import ExecutionRecord, OperationKind
from gantry.runs.status import RunStatus
from gantry.sql.registry import resolve_provider

from _runs import make_run

# ------------------------------------------------- a run can be read back


def test_a_run_round_trips_without_importing_a_storage_backend() -> None:
    """The counterpart to `to_json`, reachable from the model itself.

    Anyone implementing the `RunStore` protocol over their own database needs
    this. Before it existed the only way back was a function inside the SQLite
    store, which told store implementers the library did not expect them.
    """
    original = make_run(
        id="run_surface",
        status=RunStatus.ACCEPTED,
        kind=OperationKind.MATERIALIZE,
        execution=ExecutionRecord(status="SUCCEEDED", native_id="pg_1"),
    )

    restored = gantry.Run.from_json(original.to_json())

    assert restored == original
    assert restored.execution is not None
    assert restored.execution.native_id == "pg_1"
    assert gantry.Run.from_dict(original.as_dict()) == original


def test_the_evidence_deserializer_is_importable() -> None:
    """`__all__` claimed this name for as long as the store has existed.

    Only a private `_evidence` was ever defined, so `from gantry.runs.sqlite
    import *` raised `AttributeError` for every caller who tried it.
    """
    from gantry.runs.serialization import evidence_from_dict

    assert evidence_from_dict(None) is None
    assert evidence_from_dict({"engine": "postgres", "run_id": "run_1"}) is not None

    from gantry.runs import sqlite

    assert [name for name in sqlite.__all__ if not hasattr(sqlite, name)] == []


# ------------------------------------------- a preset knows its own needs


def test_hosted_postgres_presets_carry_what_they_require() -> None:
    """`postgres`, `neon` and `supabase` used to be registered identically.

    The preset exists to hold provider-specific behaviour. While it held none,
    every caller had to know that two of the three terminate TLS and require
    it — so every example and every downstream service wrote it out again.
    """
    assert dict(resolve_provider("supabase").connect_defaults) == {
        "ssl": "require",
        "timeout": 30,
    }
    assert dict(resolve_provider("neon").connect_defaults) == {"ssl": "require", "timeout": 30}
    # Plain `postgres` may be a local instance with no TLS at all.
    assert dict(resolve_provider("postgres").connect_defaults) == {}


def test_the_caller_always_wins_over_a_preset_default() -> None:
    """Otherwise the defaults would make a local instance unreachable."""
    pytest.importorskip("asyncpg")
    db = gantry.sql.connect("supabase", url="postgresql://u:p@localhost:5432/d", ssl=None)

    assert db._target.config["ssl"] is None
    assert db._target.config["timeout"] == 30, "the default it did not override still applies"


# --------------------------------------- one rendering of a run for a model


def test_an_accepted_run_hands_back_what_the_model_asked_for() -> None:
    class _Inline:
        columns = ("region", "revenue")
        rows = (("eu", 10),)
        truncated = True

    run = make_run(status=RunStatus.ACCEPTED, inline=_Inline())
    result = run.as_tool_result()

    assert result["status"] == "ACCEPTED"
    assert result["columns"] == ["region", "revenue"]
    assert result["rows"] == [["eu", 10]]
    assert "truncated" in str(result["note"])
    assert result["run_id"] == run.id, "a model can quote something lookup-able"


def test_a_refusal_is_a_result_rather_than_an_error() -> None:
    """The distinction every integration has to get right.

    A refused run is something a model can act on — narrow the query, tell the
    user what it could not reach. Raising instead hands it a stack trace, which
    it cannot reason about.
    """
    run = make_run(
        status=RunStatus.POLICY_REJECTED,
        failure=Failure(FailureKind.POLICY_REJECTED, False, "schema is not allowed: finance"),
    )
    result = run.as_tool_result()

    assert result["status"] == "POLICY_REJECTED"
    assert result["refused_because"] == "schema is not allowed: finance"
    assert result["retry"] == "running this again will not help"
    assert "rows" not in result


def test_a_parked_run_tells_the_agent_to_ask_and_not_how_to_answer() -> None:
    from gantry.confirmation import ConfirmationReason, ConfirmationRecord, ConfirmationRequirement

    requirement = ConfirmationRequirement(
        required=True,
        reasons=(
            ConfirmationReason(
                gantry.ConfirmationReasonCode.PRODUCTION_WRITE,
                "This writes to production data.",
            ),
        ),
    )
    run = make_run(
        status=RunStatus.AWAITING_CONFIRMATION,
        confirmation=ConfirmationRecord.required("run_x", requirement),
    )
    result = run.as_tool_result()

    assert result["confirmation_required"] == "This writes to production data."
    assert result["reasons"] == ["PRODUCTION_WRITE"]
    # Nothing here is a way to satisfy the gate.
    assert "confirm" not in json.dumps(result).replace("confirmation_required", "")


@pytest.mark.parametrize("status", [RunStatus.ACCEPTED, RunStatus.POLICY_REJECTED])
def test_the_result_is_json_serializable_whatever_happened(status: RunStatus) -> None:
    """It is handed to a model through a protocol, so it has to survive JSON."""
    run = make_run(status=status, failure=Failure(FailureKind.TIMEOUT, True, "slow"))

    assert json.loads(json.dumps(run.as_tool_result(), default=str))["status"] == status.value


# ------------------------------------------ listing is a capability, not a guess


def test_a_store_that_cannot_list_says_so_instead_of_saying_nothing() -> None:
    """An empty answer and an unsupported one are different facts.

    `recent()` used to return `()` for any store without the method, so "this
    store has no index" was indistinguishable from "nothing has run" — the
    worse of the two, because it is the one a person believes.
    """
    from gantry.runs.store import ListableRunStore, RunPersistenceError

    class Minimal:
        """Everything `RunStore` requires, and nothing more."""

        def create(self, run: gantry.Run) -> None: ...
        def update(self, run: gantry.Run) -> None: ...
        def get(self, run_id: str) -> gantry.Run | None:
            return None

        def compare_and_set(self, run_id: str, expected: RunStatus, updated: gantry.Run) -> bool:
            return False

    assert isinstance(Minimal(), gantry.runs.RunStore), "it is a valid store"
    assert not isinstance(Minimal(), ListableRunStore), "it just cannot list"

    previous = gantry.runs.store()
    gantry.runs.configure(Minimal())
    try:
        with pytest.raises(RunPersistenceError, match="cannot list runs"):
            gantry.runs.recent()
    finally:
        gantry.runs.configure(previous)


def test_the_stores_that_ship_can_list(tmp_path: object) -> None:
    from gantry.runs.store import ListableRunStore

    assert isinstance(gantry.runs.MemoryRunStore(), ListableRunStore)
    assert isinstance(gantry.runs.SQLiteRunStore(f"{tmp_path}/runs.db"), ListableRunStore)


# --------------------------------------------- a policy can be stored and reloaded


def test_a_policy_round_trips_and_keeps_its_version() -> None:
    """The version has to survive, or the audit claim does not.

    A run records the policy version that admitted it. If reloading a stored
    policy produced a different version, every run recorded before the restart
    would appear to have been decided by a policy that no longer exists.
    """
    original = gantry.Policy(
        name="data-agents",
        rules=[
            gantry.allow.query(name="reads", actors=["research-agent"], sources=["analytics.*"]),
            gantry.allow.materialize(
                sources=["raw.*"],
                destinations=["prod.*"],
                require_confirmation=True,
                confirmation_code="PRODUCTION_WRITE",
                constraints={"max_rows": 1000},
            ),
            gantry.deny.materialize(destinations=["prod.secrets"]),
        ],
    )

    restored = gantry.Policy.from_dict(original.as_dict())

    assert restored.version == original.version
    assert [rule.as_dict() for rule in restored.rules] == [
        rule.as_dict() for rule in original.rules
    ]
    # And it still decides the same way, which is the point of keeping it.
    assert restored.rules[1].require_confirmation
    assert restored.rules[2].destinations == ("prod.secrets",)


def test_an_unconstrained_dimension_comes_back_unconstrained() -> None:
    """`as_dict` omits what was never set, so `from_dict` must not invent it.

    A rule that named no actors applies to everyone. One that named an empty
    list of actors applies to nobody. Losing that distinction on a reload would
    silently disable a rule.
    """
    rule = gantry.PolicyRule.from_dict({"effect": "allow", "sources": ["a.*"]})

    assert rule.actors is None
    assert rule.environments is None
    assert rule.sources == ("a.*",)


def test_a_stored_policy_that_is_not_a_policy_is_refused() -> None:
    from gantry.policy import PolicyConfigurationError

    with pytest.raises(PolicyConfigurationError):
        gantry.Policy.from_dict({"name": "p", "rules": "allow everything"})
    with pytest.raises(PolicyConfigurationError):
        gantry.Policy.from_dict({"name": "p", "rules": ["allow everything"]})
