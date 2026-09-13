# SPDX-License-Identifier: Apache-2.0
"""Confirmation: allowed, but ask the user first.

The claim under test is narrow and easy to get wrong in the dangerous direction:
while a run is parked, *nothing has happened*. So the assertions here are mostly
about the engine rather than about the run — a table that does not exist, a
cluster with no new job, a row count that did not change.
"""

from __future__ import annotations

import asyncio
import pathlib

import duckdb
import gantry
import pytest
from gantry.actor import actor, context
from gantry.confirmation import (
    ConfirmationReason,
    ConfirmationReasonCode,
    ConfirmationRequirement,
    ConfirmationStatus,
)
from gantry.policy import Policy, PolicyConfigurationError, allow, deny
from gantry.runs.service import ConfirmationError
from gantry.runs.status import RunStatus


def _database(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "confirmation.duckdb"
    native = duckdb.connect(str(path))
    native.execute("CREATE SCHEMA raw")
    native.execute("CREATE SCHEMA prod")
    native.execute("CREATE SCHEMA analytics")
    native.execute("CREATE TABLE raw.invoices(customer_id INTEGER, balance INTEGER)")
    native.execute("INSERT INTO raw.invoices VALUES (1, 10), (2, 20)")
    native.execute("CREATE TABLE analytics.customers(customer_id INTEGER, revenue INTEGER)")
    native.execute("INSERT INTO analytics.customers VALUES (1, 10), (2, 20)")
    native.close()
    return path


def _tables(path: pathlib.Path, schema: str) -> set[str]:
    """What DuckDB itself holds. The only convincing form of "nothing ran"."""
    native = duckdb.connect(str(path))
    try:
        rows = native.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]
        ).fetchall()
    finally:
        native.close()
    return {str(row[0]) for row in rows}


def _prod_policy(**extra: object) -> Policy:
    return Policy(
        name="prod-data-agents",
        rules=[
            allow.materialize(
                name="prod-writes",
                sources=["raw.*"],
                destinations=["prod.*"],
                require_confirmation=True,
                confirmation_code="PRODUCTION_WRITE",
                confirmation_message="This will write to production data.",
                **extra,  # type: ignore[arg-type]
            ),
        ],
    )


def _sql(destination: str = "prod.totals") -> str:
    return (
        f"CREATE TABLE {destination} AS "
        "SELECT customer_id, SUM(balance) AS balance FROM raw.invoices GROUP BY customer_id"
    )


# --------------------------------------------------------------------------
# Construction
# --------------------------------------------------------------------------


def test_a_confirmation_prompt_nobody_will_see_is_refused_at_construction() -> None:
    """A message on a rule that never asks is a mistake, not a preference."""
    with pytest.raises(PolicyConfigurationError, match="require_confirmation=True"):
        allow.query(confirmation_message="are you sure?")
    with pytest.raises(PolicyConfigurationError, match="require_confirmation=True"):
        allow.query(confirmation_code="HIGH_COST")
    with pytest.raises(PolicyConfigurationError, match="unknown confirmation code"):
        allow.query(require_confirmation=True, confirmation_code="ARE_YOU_SURE")
    with pytest.raises(PolicyConfigurationError, match="must not be empty"):
        allow.query(require_confirmation=True, confirmation_message="   ")


def test_a_deny_rule_cannot_ask_for_confirmation() -> None:
    """There is nothing to confirm about work that will not run."""
    with pytest.raises(PolicyConfigurationError, match="deny rule cannot require confirmation"):
        gantry.PolicyRule(effect="deny", require_confirmation=True)  # type: ignore[arg-type]


def test_a_required_confirmation_must_say_why() -> None:
    """An empty prompt is worse than none: the user cannot tell what they agreed to."""
    with pytest.raises(ValueError, match="at least one reason"):
        ConfirmationRequirement(required=True)
    with pytest.raises(ValueError, match="say something"):
        ConfirmationReason(code=ConfirmationReasonCode.HIGH_COST, message="  ")


def test_asking_for_confirmation_changes_the_policy_version() -> None:
    """The prompt is part of the configuration a run is explained by."""
    quiet = Policy("p", [allow.materialize(destinations=["prod.*"])])
    asking = Policy("p", [allow.materialize(destinations=["prod.*"], require_confirmation=True)])
    assert quiet.version != asking.version


# --------------------------------------------------------------------------
# Parking: allowed, and nothing has happened
# --------------------------------------------------------------------------


async def test_a_parked_run_is_allowed_and_has_not_touched_the_engine(
    tmp_path: pathlib.Path,
) -> None:
    """`AWAITING_CONFIRMATION` is not a refusal and not an execution.

    Policy said yes — `admission.allowed` is true — and the table still does not
    exist. Both halves matter: a confirmation that read as a denial would send
    the agent looking for a policy problem it does not have.
    """
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

    with context(actor=actor("agent", "migration-agent")):
        run = await materialize(_sql())

    assert run.status is RunStatus.AWAITING_CONFIRMATION
    assert not run.status.terminal
    assert run.status.awaiting
    assert run.admission is not None and run.admission.allowed
    assert run.execution is None
    assert _tables(path, "prod") == set(), "nothing may exist while confirmation is pending"

    assert run.confirmation is not None
    assert run.confirmation.status is ConfirmationStatus.REQUIRED
    assert run.confirmation.codes == ("PRODUCTION_WRITE",)
    assert run.confirmation.message == "This will write to production data."
    assert run.confirmation.reasons[0].rule == "prod-writes"


async def test_a_confirmed_run_resumes_the_same_run_and_verifies_normally(
    tmp_path: pathlib.Path,
) -> None:
    """The same immutable run executes, and acceptance is still earned."""
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    materialize = db.materialize(
        sources=["raw.*"],
        destinations=["prod.*"],
        checks=[gantry.verify.destination_exists(), gantry.verify.row_count(min=1)],
    )

    with context(actor=actor("agent", "migration-agent")):
        parked = await materialize(_sql())
    done = await gantry.runs.confirm(parked.id, metadata={"channel": "cli"})

    assert done.id == parked.id, "confirmation resumes the run, it does not start a new one"
    assert done.status is RunStatus.ACCEPTED
    assert _tables(path, "prod") == {"totals"}
    assert done.confirmation is not None
    assert done.confirmation.status is ConfirmationStatus.CONFIRMED
    assert done.confirmation.metadata["channel"] == "cli"
    # Confirmation is not acceptance: the checks still ran and still decided.
    assert done.verification is not None
    assert [check.name for check in done.verification.checks]
    assert done.verification.ok


async def test_a_declined_run_is_terminal_and_leaves_nothing_behind(
    tmp_path: pathlib.Path,
) -> None:
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

    with context(actor=actor("agent", "migration-agent")):
        parked = await materialize(_sql())
    declined = await gantry.runs.decline(parked.id, metadata={"channel": "cli"})

    assert declined.status is RunStatus.CONFIRMATION_DECLINED
    assert declined.status.terminal
    assert declined.execution is None
    assert _tables(path, "prod") == set(), "a declined run must not have run"
    assert declined.confirmation is not None
    assert declined.confirmation.status is ConfirmationStatus.DECLINED
    # And it cannot be revived by asking the other way afterwards.
    with pytest.raises(ConfirmationError, match="was declined"):
        await gantry.runs.confirm(parked.id)
    assert _tables(path, "prod") == set()


# --------------------------------------------------------------------------
# Idempotency and concurrency
# --------------------------------------------------------------------------


async def test_confirming_twice_executes_once(tmp_path: pathlib.Path) -> None:
    """A host that retries must not run the work twice.

    `CREATE TABLE` is the right proposal for this test: a second execution would
    fail against the destination it already made, so "executed once" is visible
    in the run rather than only in a counter.
    """
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

    with context(actor=actor("agent", "migration-agent")):
        parked = await materialize(_sql())
    first = await gantry.runs.confirm(parked.id)
    second = await gantry.runs.confirm(parked.id)
    third = await gantry.runs.confirm(parked.id)

    assert first.status is RunStatus.ACCEPTED
    assert second.status is RunStatus.ACCEPTED
    assert third.status is RunStatus.ACCEPTED
    assert first.id == second.id == third.id
    assert _tables(path, "prod") == {"totals"}


async def test_two_concurrent_confirmations_release_the_work_once(
    tmp_path: pathlib.Path,
) -> None:
    """Only one caller may be the reason work starts.

    Three confirmations land at once against the durable store. In one process
    the single-shot registry is what stops the second from executing; the store's
    compare-and-set is what stops a second *process*, and is tested directly
    below. Both have to hold, so both are checked.
    """
    store_path = str(tmp_path / "runs.db")
    writer = gantry.runs.SQLiteRunStore(store_path)
    gantry.runs.configure(writer)
    try:
        path = _database(tmp_path)
        db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
        materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

        with context(actor=actor("agent", "migration-agent")):
            parked = await materialize(_sql())
        outcomes = await asyncio.gather(
            gantry.runs.confirm(parked.id),
            gantry.runs.confirm(parked.id),
            gantry.runs.confirm(parked.id),
            return_exceptions=True,
        )
    finally:
        writer.close()
        gantry.runs.configure(gantry.runs.MemoryRunStore())

    assert not [item for item in outcomes if isinstance(item, BaseException)], outcomes
    assert _tables(path, "prod") == {"totals"}, "exactly one execution may have happened"
    statuses = {run.status for run in outcomes if isinstance(run, gantry.Run)}
    assert statuses <= {RunStatus.ACCEPTED, RunStatus.RUNNING}


async def test_declining_something_already_confirmed_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

    with context(actor=actor("agent", "migration-agent")):
        parked = await materialize(_sql())
    await gantry.runs.confirm(parked.id)
    with pytest.raises(ConfirmationError, match="already confirmed"):
        await gantry.runs.decline(parked.id)


async def test_confirming_a_run_that_never_needed_it_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """Nothing to confirm, so saying "confirmed" would record something untrue."""
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path))
    run = await db.query(schemas=["analytics"])("SELECT revenue FROM analytics.customers")
    with pytest.raises(ConfirmationError, match="never required confirmation"):
        await gantry.runs.confirm(run.id)
    with pytest.raises(ConfirmationError, match="no such run"):
        await gantry.runs.confirm("run_does_not_exist")


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_only_one_caller_wins_a_transition(tmp_path: pathlib.Path, kind: str) -> None:
    """The store guarantee confirmation rests on, tested on its own.

    Every store that can back confirmation has to answer "was I the one that
    moved this run?" — because the caller who gets `True` is the only one
    allowed to start work. A store that said yes twice would let two processes
    each submit the same job.
    """
    store: gantry.runs.RunStore = (
        gantry.runs.MemoryRunStore()
        if kind == "memory"
        else gantry.runs.SQLiteRunStore(str(tmp_path / f"{kind}.db"))
    )
    parked = _awaiting_run()
    store.create(parked)

    confirmed = parked.advanced(RunStatus.RUNNING)
    wins = [
        store.compare_and_set(parked.id, RunStatus.AWAITING_CONFIRMATION, confirmed)
        for _ in range(5)
    ]
    assert wins == [True, False, False, False, False]
    # And a transition from a state the run was never in changes nothing.
    assert not store.compare_and_set(parked.id, RunStatus.PENDING, confirmed)
    current = store.get(parked.id)
    assert current is not None and current.status is RunStatus.RUNNING
    if isinstance(store, gantry.runs.SQLiteRunStore):
        store.close()


def _awaiting_run() -> gantry.Run:
    from gantry.confirmation import ConfirmationRecord
    from gantry.runs.model import OperationKind, OperationRef, Run, new_run_id

    requirement = ConfirmationRequirement(
        required=True,
        reasons=(
            ConfirmationReason(
                ConfirmationReasonCode.PRODUCTION_WRITE, "This writes to production."
            ),
        ),
    )
    run_id = new_run_id()
    return Run(
        id=run_id,
        status=RunStatus.AWAITING_CONFIRMATION,
        actor=actor("agent", "migration-agent"),
        operation=OperationRef(kind=OperationKind.MATERIALIZE, engine="sql"),
        confirmation=ConfirmationRecord.required(run_id, requirement),
    )


# --------------------------------------------------------------------------
# Ordering: policy first, verification after
# --------------------------------------------------------------------------


async def test_a_policy_denial_is_never_offered_for_confirmation(
    tmp_path: pathlib.Path,
) -> None:
    """Confirmation cannot override policy, so a denial never parks.

    The deny rule here overlaps the allow rule that asks for confirmation. The
    run must come back refused, with nothing to confirm — otherwise a host could
    turn a refusal into an execution by answering yes.
    """
    path = _database(tmp_path)
    policy = Policy(
        name="prod-frozen",
        rules=[
            allow.materialize(
                sources=["raw.*"],
                destinations=["prod.*"],
                require_confirmation=True,
                confirmation_code="PRODUCTION_WRITE",
            ),
            deny.materialize(destinations=["prod.*"]),
        ],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

    with context(actor=actor("agent", "migration-agent")):
        run = await materialize(_sql())

    assert run.status is RunStatus.POLICY_REJECTED
    assert run.confirmation is None
    assert _tables(path, "prod") == set()
    with pytest.raises(ConfirmationError, match="never required confirmation"):
        await gantry.runs.confirm(run.id)
    assert _tables(path, "prod") == set()


async def test_confirmation_does_not_make_a_bad_result_acceptable(
    tmp_path: pathlib.Path,
) -> None:
    """A user saying yes is not a verification result.

    The row count check cannot pass here, so the confirmed run executes and is
    then rejected on its merits.
    """
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    materialize = db.materialize(
        sources=["raw.*"],
        destinations=["prod.*"],
        checks=[gantry.verify.row_count(min=500)],
    )

    with context(actor=actor("agent", "migration-agent")):
        parked = await materialize(_sql())
    done = await gantry.runs.confirm(parked.id)

    assert done.status is RunStatus.REJECTED
    assert done.confirmation is not None
    assert done.confirmation.status is ConfirmationStatus.CONFIRMED
    assert _tables(path, "prod") == {"totals"}, "it ran; it was the result that was rejected"


# --------------------------------------------------------------------------
# Several reasons, one question
# --------------------------------------------------------------------------


async def test_several_rules_asking_collapse_into_one_question(
    tmp_path: pathlib.Path,
) -> None:
    """Being asked twice about one operation teaches a user to click through."""
    path = _database(tmp_path)
    policy = Policy(
        name="two-reasons",
        rules=[
            allow.materialize(
                name="prod",
                sources=["raw.*"],
                destinations=["prod.*"],
                require_confirmation=True,
                confirmation_code="PRODUCTION_WRITE",
            ),
            allow.materialize(
                name="expensive",
                sources=["*"],
                destinations=["*"],
                require_confirmation=True,
                confirmation_code="HIGH_COST",
                confirmation_message="This may be expensive.",
            ),
        ],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

    with context(actor=actor("agent", "migration-agent")):
        run = await materialize(_sql())

    assert run.confirmation is not None
    assert run.confirmation.codes == ("PRODUCTION_WRITE", "HIGH_COST")
    assert len(run.confirmation.reasons) == 2
    # One record, one question — not two prompts to answer separately.
    assert await gantry.runs.confirm(run.id) is not None


# --------------------------------------------------------------------------
# The agent side
# --------------------------------------------------------------------------


async def test_the_agent_learns_it_must_ask_but_cannot_answer(
    tmp_path: pathlib.Path,
) -> None:
    """§27's tool contract: enough to ask the user, no capability to confirm."""
    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    tool = db.materialize(sources=["raw.*"], destinations=["prod.*"]).tool()

    with context(actor=actor("agent", "migration-agent")):
        run = await tool.invoke({"sql": _sql()})
    payload = run.as_dict()

    properties = tool.input_schema["properties"]
    assert isinstance(properties, dict)
    assert sorted(properties) == ["sql", "verify"]
    assert payload["status"] == "AWAITING_CONFIRMATION"
    confirmation = payload["confirmation"]
    assert isinstance(confirmation, dict)
    assert confirmation["status"] == "required"
    assert confirmation["reasons"] == [
        {
            "code": "PRODUCTION_WRITE",
            "message": "This will write to production data.",
            "rule": "prod-writes",
        }
    ]
    # The tool surface has no way to satisfy the gate it just reported.
    assert "confirm" not in str(tool.input_schema)
    for argument in ("confirm", "confirmed", "confirmation", "run_id"):
        with pytest.raises(ValueError, match="unexpected"):
            await tool.invoke({"sql": _sql(), argument: True})
    assert _tables(path, "prod") == set()


# --------------------------------------------------------------------------
# Proposal binding and persistence
# --------------------------------------------------------------------------


async def test_confirmation_is_bound_to_the_proposal_it_was_asked_about(
    tmp_path: pathlib.Path,
) -> None:
    """Confirming statement A must never license running statement B.

    The proposal travels with the parked work, so the store cannot be used to
    swap it. This tampers with the recorded hash and checks the mismatch is
    refused rather than executed under a disagreement about what was asked.
    """
    from dataclasses import replace

    path = _database(tmp_path)
    db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
    materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])

    with context(actor=actor("agent", "migration-agent")):
        parked = await materialize(_sql())
    assert parked.confirmation is not None
    assert parked.proposal is not None
    assert parked.confirmation.proposal_hash == parked.proposal.hash

    tampered = replace(
        parked,
        confirmation=replace(parked.confirmation, proposal_hash="sha256:something-else"),
    )
    gantry.runs.store().update(tampered)

    with pytest.raises(ConfirmationError, match="different proposal"):
        await gantry.runs.confirm(parked.id)
    assert _tables(path, "prod") == set()


async def test_the_confirmation_record_survives_the_process(tmp_path: pathlib.Path) -> None:
    """A run read back later still says it was asked about, and answered."""
    store_path = str(tmp_path / "runs.db")
    writer = gantry.runs.SQLiteRunStore(store_path)
    gantry.runs.configure(writer)
    try:
        path = _database(tmp_path)
        db = gantry.sql.connect("duckdb", path=str(path), policy=_prod_policy())
        materialize = db.materialize(sources=["raw.*"], destinations=["prod.*"])
        with context(actor=actor("agent", "migration-agent")):
            parked = await materialize(_sql())
        done = await gantry.runs.confirm(parked.id, metadata={"channel": "chat"})
    finally:
        writer.close()
        gantry.runs.configure(gantry.runs.MemoryRunStore())

    reader = gantry.runs.SQLiteRunStore(store_path)
    try:
        recovered = reader.get(done.id)
    finally:
        reader.close()

    assert recovered is not None and recovered.confirmation is not None
    assert recovered.status is RunStatus.ACCEPTED
    assert recovered.confirmation.status is ConfirmationStatus.CONFIRMED
    assert recovered.confirmation.codes == ("PRODUCTION_WRITE",)
    assert recovered.confirmation.reasons[0].message == "This will write to production data."
    assert recovered.confirmation.proposal_hash == recovered.proposal.hash  # type: ignore[union-attr]
    assert recovered.confirmation.confirmed_at is not None
    assert dict(recovered.confirmation.metadata) == {"channel": "chat"}


def test_the_run_view_never_claims_a_human_approved_anything(
    tmp_path: pathlib.Path,
) -> None:
    """v0 records that the host confirmed. It cannot name a person, so it does not.

    The rendered view is where someone reads a run months later, so the wording
    is part of the contract: "approved by" would be a claim Gantry has no
    evidence for.
    """
    from gantry.confirmation import ConfirmationRecord
    from gantry.runs.model import OperationKind, OperationRef, Run

    record = ConfirmationRecord.required(
        "run_1",
        ConfirmationRequirement(
            required=True,
            reasons=(
                ConfirmationReason(
                    ConfirmationReasonCode.PRODUCTION_WRITE, "This writes to production."
                ),
            ),
        ),
    )
    run = Run(
        id="run_1",
        status=RunStatus.AWAITING_CONFIRMATION,
        actor=actor("agent", "migration-agent"),
        operation=OperationRef(kind=OperationKind.MATERIALIZE, engine="sql"),
        confirmation=record,
    )

    waiting = run.render()
    assert "awaiting user confirmation" in waiting
    assert "PRODUCTION_WRITE: This writes to production." in waiting

    confirmed = run.advanced(RunStatus.ACCEPTED, confirmation=record.confirmed()).render()
    assert "confirmed by the host" in confirmed
    for claim in ("approved by", "approved_by", "authenticated"):
        assert claim not in confirmed.lower()


async def test_a_streaming_job_waits_for_confirmation_before_the_cluster_sees_it() -> None:
    """The engine where parking matters most, checked at the transport.

    A submitted Flink job is already running somewhere, so "nothing happened
    while we waited" has to mean the gateway was never called — not that the run
    says it was not.
    """
    from test_flink import FakeFlinkTransport

    transport = FakeFlinkTransport()
    policy = Policy(
        name="stream-confirmations",
        rules=[
            allow.stream(
                sources=["hive.events.*"],
                destinations=["hive.events.clean_events"],
                require_confirmation=True,
                confirmation_code="LONG_RUNNING_JOB",
                confirmation_message="This starts a job that keeps running.",
            )
        ],
    )
    stream = gantry.stream.connect(
        "flink",
        endpoint="https://gateway.example/flink",
        policy=policy,
        config={
            "jobmanager_endpoint": "https://jobmanager.example",
            "transport": transport,
            "default_catalog": "hive",
            "default_database": "events",
        },
    )
    job = stream.job(
        inputs=["raw_events"],
        outputs=["clean_events"],
        checks=[gantry.verify.running()],
        poll_interval=0,
    )

    with context(actor=actor("agent", "etl-agent")):
        parked = await job("INSERT INTO clean_events SELECT * FROM raw_events")

    assert parked.status is RunStatus.AWAITING_CONFIRMATION
    assert transport.calls == [], "nothing may reach the gateway while confirmation is pending"
    assert parked.confirmation is not None
    assert parked.confirmation.codes == ("LONG_RUNNING_JOB",)

    resumed = await gantry.runs.confirm(parked.id)
    assert resumed.id == parked.id
    assert resumed.status is RunStatus.ACCEPTED
    assert transport.calls, "a confirmed job must actually be submitted"


async def test_a_declined_streaming_job_never_reaches_the_cluster() -> None:
    from test_flink import FakeFlinkTransport

    transport = FakeFlinkTransport()
    policy = Policy(
        name="stream-confirmations",
        rules=[
            allow.stream(
                sources=["hive.events.*"],
                destinations=["hive.events.clean_events"],
                require_confirmation=True,
                confirmation_code="LONG_RUNNING_JOB",
            )
        ],
    )
    stream = gantry.stream.connect(
        "flink",
        endpoint="https://gateway.example/flink",
        policy=policy,
        config={
            "jobmanager_endpoint": "https://jobmanager.example",
            "transport": transport,
            "default_catalog": "hive",
            "default_database": "events",
        },
    )
    job = stream.job(inputs=["raw_events"], outputs=["clean_events"], poll_interval=0)

    with context(actor=actor("agent", "etl-agent")):
        parked = await job("INSERT INTO clean_events SELECT * FROM raw_events")
    declined = await gantry.runs.decline(parked.id)

    assert declined.status is RunStatus.CONFIRMATION_DECLINED
    assert transport.calls == []
