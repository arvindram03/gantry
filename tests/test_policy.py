# SPDX-License-Identifier: Apache-2.0
"""Policy: may this actor perform this proposed operation on these resources?

The end-to-end cases run against DuckDB rather than a fake adapter. A policy
that refuses is only worth something if the engine never saw the work, and the
only convincing evidence of that is asking the engine afterwards whether the
table exists.
"""

from __future__ import annotations

import pathlib
from collections.abc import Sequence

import duckdb
import gantry
import pytest
from gantry.actor import actor, context
from gantry.failure import FailureKind
from gantry.policy import (
    Policy,
    PolicyConfigurationError,
    PolicyRequest,
    PolicyRule,
    allow,
    deny,
    evaluate,
)
from gantry.policy.patterns import matches, normalize_pattern
from gantry.runs.model import OperationKind, ResourceRef
from gantry.runs.status import RunStatus


def _request(
    operation: OperationKind = OperationKind.QUERY,
    *,
    inputs: Sequence[str] = (),
    outputs: Sequence[str] = (),
    who: str = "research-agent",
    engine: str = "postgres",
    environment: str | None = None,
    unresolved: Sequence[str] = (),
    **constraints: float,
) -> PolicyRequest:
    return PolicyRequest(
        actor=actor("agent", who),
        operation=operation,
        engine=engine,
        inputs=tuple(ResourceRef(system=engine, resource=name) for name in inputs),
        outputs=tuple(ResourceRef(system=engine, resource=name) for name in outputs),
        environment=environment,
        constraints=constraints,
        unresolved=tuple(unresolved),
    )


def _codes(policy: Policy, request: PolicyRequest) -> tuple[str, ...]:
    return evaluate(policy, request).codes


# --------------------------------------------------------------------------
# Construction: a policy that cannot mean anything fails where it is written
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pattern",
    ["ana*lytics", "*.orders", "analytics.*.customers", "analytics..customers", "", "   ", "a**"],
)
def test_malformed_resource_patterns_are_refused_at_construction(pattern: str) -> None:
    """A pattern language is a place to be wrong quietly.

    `prod*` looking like it covers `production` while matching nothing, or a
    mid-pattern `*` read as a regex, are mistakes that surface in an audit
    rather than at the keyboard. Every one of these is a typo, not a rule.
    """
    with pytest.raises(PolicyConfigurationError):
        allow.query(sources=[pattern])


def test_a_string_where_a_list_belongs_is_not_a_one_element_policy() -> None:
    """`sources="analytics.*"` would otherwise iterate into single characters."""
    with pytest.raises(PolicyConfigurationError):
        allow.query(sources="analytics.*")  # type: ignore[arg-type]


def test_unknown_operations_effects_and_constraints_are_refused() -> None:
    with pytest.raises(PolicyConfigurationError, match="unknown operation"):
        PolicyRule(effect="allow", operations=["transmogrify"])  # type: ignore[list-item]
    with pytest.raises(PolicyConfigurationError, match="unknown policy effect"):
        PolicyRule(effect="maybe")  # type: ignore[arg-type]
    with pytest.raises(PolicyConfigurationError, match="unknown constraint"):
        allow.query(constraints={"max_joins": 3})
    with pytest.raises(PolicyConfigurationError, match="must be positive"):
        allow.query(constraints={"max_rows": 0})
    with pytest.raises(PolicyConfigurationError, match="must be numeric"):
        allow.query(constraints={"max_rows": "many"})  # type: ignore[dict-item]


def test_an_allow_that_could_never_authorize_a_write_is_refused() -> None:
    """`allow.materialize(sources=["raw.*"])` authorizes nothing at all.

    Every materialization has a destination and an allow rule grants no write
    authority by omission, so the rule as written can only ever deny. Saying so
    at construction is the difference between a typo and a policy that
    mysteriously refuses everything.
    """
    # Omitting them is a signature error, so it cannot even be written.
    with pytest.raises(TypeError, match="destinations"):
        allow.materialize(sources=["raw.*"])  # type: ignore[call-arg]
    # Passing an empty list is the same mistake spelled differently.
    with pytest.raises(PolicyConfigurationError, match="must name destinations"):
        allow.stream(destinations=[])
    with pytest.raises(PolicyConfigurationError, match="must name destinations"):
        allow.batch(destinations=[])


def test_duplicate_rule_names_are_refused_because_a_decision_names_its_rule() -> None:
    with pytest.raises(PolicyConfigurationError, match="duplicate rule name"):
        Policy(
            name="p",
            rules=[
                allow.query(name="reads", sources=["a.*"]),
                allow.query(name="reads", sources=["b.*"]),
            ],
        )


def test_a_policy_needs_a_name_and_a_list_of_rules() -> None:
    with pytest.raises(PolicyConfigurationError):
        Policy(name="  ", rules=[])
    with pytest.raises(PolicyConfigurationError, match="not one rule"):
        Policy(name="p", rules=allow.query())  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# Identity: the version is derived from the rules, not declared beside them
# --------------------------------------------------------------------------


def test_policy_version_is_derived_from_the_rules_and_changes_when_they_do() -> None:
    """A run records `sha256:…` so the decision stays explainable.

    If the version were declared by hand it could disagree with the rules it
    names, which is worse than no version: an audit would trust it.
    """
    first = Policy(name="data-agents", rules=[allow.query(sources=["analytics.*"])])
    same = Policy(name="data-agents", rules=[allow.query(sources=["analytics.*"])])
    widened = Policy(name="data-agents", rules=[allow.query(sources=["analytics.*", "raw.*"])])
    renamed = Policy(name="other-agents", rules=[allow.query(sources=["analytics.*"])])

    assert first.version == same.version
    assert first.version.startswith("sha256:")
    assert first.version != widened.version
    assert first.version != renamed.version


def test_rule_order_is_part_of_the_policy_identity() -> None:
    forwards = Policy(
        name="p", rules=[allow.query(sources=["a.*"]), deny.query(sources=["a.secret"])]
    )
    backwards = Policy(
        name="p", rules=[deny.query(sources=["a.secret"]), allow.query(sources=["a.*"])]
    )
    assert forwards.version != backwards.version
    # Order does not change the outcome, though: deny wins from either position.
    request = _request(inputs=["a.secret"])
    assert _codes(forwards, request) == _codes(backwards, request) == ("SOURCE_DENIED",)


# --------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------


def test_a_trailing_wildcard_names_things_in_a_namespace_not_the_namespace() -> None:
    pattern = normalize_pattern("analytics.*")
    assert matches(pattern, "analytics.customers")
    assert matches(pattern, "analytics.public.customers")
    assert matches(pattern, "ANALYTICS.Customers")
    assert not matches(pattern, "analytics")
    assert not matches(pattern, "analytics_staging.customers")
    assert not matches(pattern, "raw.analytics.customers")
    assert matches(normalize_pattern("*"), "anything.at.all")
    assert matches(normalize_pattern("events"), "events")
    assert not matches(normalize_pattern("events"), "events.archive")


def test_a_name_prefix_matches_within_one_segment_only() -> None:
    """MongoDB has `database.collection` and nothing between.

    Without a prefix form the only Mongo policy wider than one collection is
    "the whole database", so `derived_*` — the shape the operation layer already
    takes — has to mean something here too. It stops at the dot: a prefix names
    a resource, not a namespace.
    """
    pattern = normalize_pattern("gantry_test.scratch_*")
    assert matches(pattern, "gantry_test.scratch_rollup")
    assert matches(pattern, "GANTRY_TEST.SCRATCH_rollup")
    assert not matches(pattern, "gantry_test.prod_rollup")
    assert not matches(pattern, "gantry_test.scratch_rollup.v2")
    assert not matches(pattern, "other.scratch_rollup")


# --------------------------------------------------------------------------
# Semantics
# --------------------------------------------------------------------------


def test_explicit_deny_wins_over_an_allow_that_covers_the_same_resource() -> None:
    policy = Policy(
        name="agent-materialization",
        rules=[
            allow.materialize(sources=["raw.*"], destinations=["*"]),
            deny.materialize(destinations=["prod.*"]),
        ],
    )
    scratch = _request(OperationKind.MATERIALIZE, inputs=["raw.orders"], outputs=["scratch.o"])
    production = _request(OperationKind.MATERIALIZE, inputs=["raw.orders"], outputs=["prod.orders"])

    assert evaluate(policy, scratch).allowed
    refused = evaluate(policy, production)
    assert not refused.allowed
    assert refused.codes == ("DESTINATION_DENIED",)
    assert refused.matched_rules == ("deny-materialize-1",)
    assert refused.reasons[0].resource == "prod.orders"


def test_an_explicit_policy_denies_what_it_does_not_mention() -> None:
    """Default deny, so a policy is a list of what may happen.

    The opposite default would make every new operation kind and every new
    table authorized until someone remembered to write a rule about it.
    """
    empty = Policy(name="nothing", rules=[])
    assert _codes(empty, _request(inputs=["analytics.customers"])) == ("NO_MATCHING_ALLOW",)

    reads_only = Policy(name="reads", rules=[allow.query(sources=["analytics.*"])])
    assert _codes(
        reads_only, _request(OperationKind.MATERIALIZE, inputs=["analytics.c"], outputs=["s.t"])
    ) == ("OPERATION_DENIED",)


def test_every_touched_resource_needs_authority_of_its_own() -> None:
    """Allowing `analytics.*` does not authorize the `finance` table joined to it."""
    policy = Policy(name="reads", rules=[allow.query(sources=["analytics.*"])])
    decision = evaluate(policy, _request(inputs=["analytics.orders", "finance.customers"]))
    assert not decision.allowed
    assert [reason.resource for reason in decision.reasons] == ["finance.customers"]


def test_separate_rules_compose_to_authorize_the_union() -> None:
    policy = Policy(
        name="reads",
        rules=[allow.query(sources=["analytics.*"]), allow.query(sources=["finance.*"])],
    )
    decision = evaluate(policy, _request(inputs=["analytics.orders", "finance.customers"]))
    assert decision.allowed
    assert len(decision.matched_rules) == 2


def test_an_allow_rule_that_names_no_destinations_authorizes_no_write() -> None:
    """Write authority is granted explicitly or not at all.

    Reading the wrong table leaks; writing the wrong table destroys. A rule
    that forgot to mention destinations should not turn out to have granted
    every destination.
    """
    policy = Policy(name="reads", rules=[allow.query(sources=["*"])])
    decision = evaluate(policy, _request(inputs=["analytics.c"], outputs=["analytics.c_copy"]))
    assert not decision.allowed
    assert "no rule allows writing analytics.c_copy" in decision.messages()


def test_a_rule_constraint_is_a_ceiling_the_operation_must_already_be_under() -> None:
    """Trusted constraints compose toward less authority, never more.

    A policy that could raise the operation's own limit would let the reusable
    layer weaken what the call site configured, which is the one direction
    composition must not go.
    """
    policy = Policy(name="bounded", rules=[allow.query(constraints={"max_rows": 1_000})])
    assert evaluate(policy, _request(inputs=["a.b"], max_rows=100)).allowed
    assert evaluate(policy, _request(inputs=["a.b"], max_rows=1_000)).allowed
    exceeded = evaluate(policy, _request(inputs=["a.b"], max_rows=10_000))
    assert not exceeded.allowed
    assert exceeded.codes == ("CONSTRAINT_EXCEEDED",)
    # A rule that bounds rows cannot apply where nothing bounds them: no limit
    # is configured, so nothing would hold the ceiling.
    assert _codes(policy, _request(inputs=["a.b"])) == ("CONSTRAINT_EXCEEDED",)


def test_rules_scope_to_actor_engine_and_environment() -> None:
    policy = Policy(
        name="scoped",
        rules=[
            allow.materialize(
                actors=["etl-agent"],
                sources=["raw.*"],
                destinations=["scratch.*"],
                engines=["snowflake"],
                environments=["dev", "staging"],
            )
        ],
    )

    def request(**changes: object) -> PolicyRequest:
        defaults: dict[str, object] = {
            "operation": OperationKind.MATERIALIZE,
            "inputs": ["raw.orders"],
            "outputs": ["scratch.orders"],
            "who": "etl-agent",
            "engine": "snowflake",
            "environment": "dev",
        }
        return _request(**{**defaults, **changes})  # type: ignore[arg-type]

    assert evaluate(policy, request()).allowed
    assert _codes(policy, request(who="rogue-agent")) == ("ACTOR_DENIED",)
    assert _codes(policy, request(engine="postgres")) == ("NO_MATCHING_ALLOW",)
    assert _codes(policy, request(environment="prod")) == ("ENVIRONMENT_DENIED",)
    # No environment is not `dev`. Guessing a default would let a rule written
    # for one environment quietly apply everywhere.
    assert _codes(policy, request(environment=None)) == ("ENVIRONMENT_DENIED",)


def test_an_actor_matches_by_id_or_by_its_full_label() -> None:
    by_id = Policy(name="p", rules=[allow.query(actors=["research-agent"], sources=["a.*"])])
    by_label = Policy(
        name="p", rules=[allow.query(actors=["agent:research-agent"], sources=["a.*"])]
    )
    request = _request(inputs=["a.b"], who="research-agent")
    assert evaluate(by_id, request).allowed
    assert evaluate(by_label, request).allowed


def test_an_effect_that_cannot_be_determined_fails_closed() -> None:
    policy = Policy(name="p", rules=[allow.query(sources=["*"])])
    decision = evaluate(policy, _request(unresolved=("the statement could not be classified",)))
    assert not decision.allowed
    assert decision.codes == ("RESOURCE_UNRESOLVED",)


def test_a_deny_rule_that_names_a_pair_denies_that_pair_only() -> None:
    policy = Policy(
        name="p",
        rules=[
            allow.materialize(sources=["*"], destinations=["*"]),
            deny.materialize(sources=["raw.*"], destinations=["prod.*"]),
        ],
    )
    pair = _request(OperationKind.MATERIALIZE, inputs=["raw.orders"], outputs=["prod.orders"])
    half = _request(OperationKind.MATERIALIZE, inputs=["raw.orders"], outputs=["scratch.orders"])
    assert not evaluate(policy, pair).allowed
    assert evaluate(policy, half).allowed


def test_deny_anything_covers_every_operation() -> None:
    policy = Policy(
        name="frozen",
        rules=[
            allow.query(sources=["*"]),
            allow.materialize(sources=["*"], destinations=["*"]),
            deny.anything(environments=["prod"]),
        ],
    )
    for operation in (OperationKind.QUERY, OperationKind.MATERIALIZE):
        decision = evaluate(
            policy,
            _request(operation, inputs=["a.b"], outputs=["c.d"], environment="prod"),
        )
        assert decision.codes == ("ENVIRONMENT_DENIED",)


# --------------------------------------------------------------------------
# End to end, against DuckDB. A refusal is only worth something if the engine
# never saw the work, so these ask the engine afterwards.
# --------------------------------------------------------------------------


def _database(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "policy.duckdb"
    native = duckdb.connect(str(path))
    native.execute("CREATE SCHEMA analytics")
    native.execute("CREATE SCHEMA finance")
    native.execute("CREATE SCHEMA raw")
    native.execute("CREATE SCHEMA agent_scratch")
    native.execute("CREATE SCHEMA prod")
    native.execute("CREATE TABLE analytics.customers(customer_id INTEGER, revenue INTEGER)")
    native.execute("INSERT INTO analytics.customers VALUES (1, 10), (2, 20)")
    native.execute("CREATE TABLE finance.payroll(employee_id INTEGER, salary INTEGER)")
    native.execute("INSERT INTO finance.payroll VALUES (1, 100)")
    native.execute("CREATE TABLE raw.invoices(customer_id INTEGER, balance INTEGER)")
    native.execute("INSERT INTO raw.invoices VALUES (1, 10), (2, 20)")
    native.close()
    return path


def _tables(path: pathlib.Path) -> set[str]:
    """Ask DuckDB itself what exists. The only convincing check."""
    native = duckdb.connect(str(path))
    try:
        rows = native.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables"
        ).fetchall()
    finally:
        native.close()
    return {str(row[0]) for row in rows}


async def test_a_denied_query_never_reaches_the_engine(tmp_path: pathlib.Path) -> None:
    """The proposal is refused before execution, and the run says so.

    A run with no execution record is the observable form of "nothing ran": the
    record is created before admission, so a refusal leaves a run that exists,
    names what was asked, and has no engine job attached to it.
    """
    path = _database(tmp_path)
    policy = Policy(
        name="analytics-read",
        rules=[allow.query(actors=["research-agent"], sources=["analytics.*"])],
    )
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True, policy=policy)
    query = db.query(schemas=["analytics", "finance"])

    with context(actor=actor("agent", "research-agent")):
        allowed = await query("SELECT customer_id, revenue FROM analytics.customers")
        refused = await query("SELECT salary FROM finance.payroll")

    assert allowed.status is RunStatus.ACCEPTED
    assert allowed.rows == ((1, 10), (2, 20))
    assert allowed.admission is not None
    assert allowed.admission.policy == "analytics-read"
    assert allowed.admission.policy_hash == policy.version
    assert allowed.admission.matched_rules == ("allow-query-0",)

    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.execution is None, "a refused proposal must not have reached the engine"
    assert refused.rows == ()
    assert refused.failure is not None
    assert refused.failure.kind is FailureKind.POLICY_REJECTED
    assert refused.admission is not None
    assert refused.admission.codes == ("NO_MATCHING_ALLOW",)
    assert refused.admission.request is not None
    assert refused.admission.request["inputs"] == ["finance.payroll"]


async def test_a_denied_materialization_leaves_no_table_behind(tmp_path: pathlib.Path) -> None:
    """The strongest form of "policy ran first": ask DuckDB what exists.

    `run.status` could be wrong about this in a way no assertion on the run
    would catch. The table either exists in the database or it does not.
    """
    path = _database(tmp_path)
    policy = Policy(
        name="agent-materialization",
        rules=[
            allow.materialize(sources=["raw.*"], destinations=["agent_scratch.*"]),
            deny.materialize(destinations=["prod.*"]),
        ],
    )
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    materialize = db.materialize(sources=["raw.*"], destinations=["agent_scratch.*", "prod.*"])

    before = _tables(path)
    allowed = await materialize(
        "CREATE TABLE agent_scratch.totals AS SELECT customer_id, SUM(balance) AS b "
        "FROM raw.invoices GROUP BY customer_id"
    )
    refused = await materialize(
        "CREATE TABLE prod.totals AS SELECT customer_id, SUM(balance) AS b "
        "FROM raw.invoices GROUP BY customer_id"
    )
    after = _tables(path)

    assert allowed.status is RunStatus.ACCEPTED
    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.admission is not None
    assert refused.admission.codes == ("DESTINATION_DENIED",)
    assert refused.execution is None

    assert "agent_scratch.totals" in after
    assert "prod.totals" not in after
    assert after - before == {"agent_scratch.totals"}
    # The destination it wanted is recorded as a proposal, not as an output: an
    # authorized destination is not a table anyone can go and read.
    assert refused.outputs == ()
    assert refused.admission.request is not None
    assert refused.admission.request["outputs"] == ["prod.totals"]


async def test_operation_constraints_still_apply_under_a_permissive_policy(
    tmp_path: pathlib.Path,
) -> None:
    """Composition only ever narrows authority.

    The reusable policy here allows every source. The operation was configured
    for `analytics` alone, and that is the effective authority — otherwise
    attaching a policy would quietly widen what a call site had bounded.
    """
    path = _database(tmp_path)
    policy = Policy(name="permissive", rules=[allow.query(sources=["*"])])
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True, policy=policy)
    query = db.query(schemas=["analytics"], max_rows=10)

    with context(actor=actor("agent", "research-agent")):
        refused = await query("SELECT salary FROM finance.payroll")
        allowed = await query("SELECT revenue FROM analytics.customers")

    assert allowed.status is RunStatus.ACCEPTED
    assert refused.status is RunStatus.POLICY_REJECTED
    # Refused by the operation's own allow-list, which the policy cannot relax.
    assert refused.admission is not None
    assert refused.admission.codes == ()
    assert any("schema is not allowed" in reason for reason in refused.admission.reasons)


async def test_a_write_dressed_as_a_query_needs_write_authority(
    tmp_path: pathlib.Path,
) -> None:
    """A read-only policy's authority does not extend to a statement that writes.

    The operation kind is how the caller configured it, not what the SQL turned
    out to do, so an `INSERT` submitted through a query operation still has to
    find a rule that grants the write.
    """
    path = _database(tmp_path)
    policy = Policy(name="reads", rules=[allow.query(sources=["*"])])
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    query = db.query(read_only=False, schemas=["analytics", "finance"])

    with context(actor=actor("agent", "research-agent")):
        refused = await query("INSERT INTO finance.payroll VALUES (2, 200)")

    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.execution is None
    assert refused.admission is not None
    assert refused.admission.request is not None
    assert refused.admission.request["outputs"] == ["finance.payroll"]
    native = duckdb.connect(str(path))
    try:
        assert native.execute("SELECT COUNT(*) FROM finance.payroll").fetchone() == (1,)
    finally:
        native.close()


async def test_a_multi_statement_submission_has_no_separable_effects(
    tmp_path: pathlib.Path,
) -> None:
    """Two statements share one classification, so neither can be authorized.

    Authorizing the union as reads would authorize the write hidden among them,
    which is exactly the case `RESOURCE_UNRESOLVED` exists for.
    """
    path = _database(tmp_path)
    policy = Policy(name="permissive", rules=[allow.query(sources=["*"])])
    db = gantry.sql.connect("duckdb", path=str(path), policy=policy)
    query = db.query(read_only=False, allow_multiple_statements=True, schemas=["analytics"])

    with context(actor=actor("agent", "research-agent")):
        refused = await query(
            "SELECT 1; INSERT INTO analytics.customers VALUES (3, 30)",
        )

    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.admission is not None
    assert refused.admission.codes == ("RESOURCE_UNRESOLVED",)
    native = duckdb.connect(str(path))
    try:
        assert native.execute("SELECT COUNT(*) FROM analytics.customers").fetchone() == (2,)
    finally:
        native.close()


async def test_the_admission_decision_survives_the_process(tmp_path: pathlib.Path) -> None:
    """A run read back months later still says which policy decided it.

    The decision is the part of a run an auditor asks about, so it has to
    survive storage in full: the name, the version that was in force, the rules
    that matched, the codes, and the normalized request they were applied to.
    """
    store_path = str(tmp_path / "runs.db")
    writer = gantry.runs.SQLiteRunStore(store_path)
    gantry.runs.configure(writer)
    path = _database(tmp_path)
    policy = Policy(
        name="data-agents",
        rules=[allow.query(name="analytics-reads", sources=["analytics.*"])],
    )
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True, policy=policy)
    query = db.query(schemas=["analytics", "finance"])

    with context(actor=actor("agent", "research-agent")):
        allowed = await query("SELECT revenue FROM analytics.customers")
        refused = await query("SELECT salary FROM finance.payroll")
    writer.close()

    reader = gantry.runs.SQLiteRunStore(store_path)
    try:
        recovered_allowed = reader.get(allowed.id)
        recovered_refused = reader.get(refused.id)
    finally:
        reader.close()
        gantry.runs.configure(gantry.runs.MemoryRunStore())

    assert recovered_allowed is not None and recovered_allowed.admission is not None
    assert recovered_allowed.admission.allowed
    assert recovered_allowed.admission.policy == "data-agents"
    assert recovered_allowed.admission.policy_hash == policy.version
    assert recovered_allowed.admission.matched_rules == ("analytics-reads",)

    assert recovered_refused is not None and recovered_refused.admission is not None
    assert not recovered_refused.admission.allowed
    assert recovered_refused.admission.codes == ("NO_MATCHING_ALLOW",)
    assert recovered_refused.admission.request is not None
    assert recovered_refused.admission.request["actor"] == "agent:research-agent"
    assert recovered_refused.admission.request["inputs"] == ["finance.payroll"]
    assert recovered_refused.status is RunStatus.POLICY_REJECTED


async def test_a_run_keeps_the_policy_version_that_decided_it(tmp_path: pathlib.Path) -> None:
    """v0 never re-evaluates admitted work, so the snapshot has to be real.

    Editing the rules produces a different version. The run that was already
    decided keeps pointing at the one that decided it, which is what makes an
    old decision explainable rather than merely plausible.
    """
    path = _database(tmp_path)
    original = Policy(name="data-agents", rules=[allow.query(sources=["analytics.*"])])
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True, policy=original)

    with context(actor=actor("agent", "research-agent")):
        run = await db.query(schemas=["analytics"])("SELECT revenue FROM analytics.customers")

    widened = Policy(name="data-agents", rules=[allow.query(sources=["analytics.*", "finance.*"])])
    assert widened.version != original.version
    assert run.admission is not None
    assert run.admission.policy_hash == original.version


async def test_policy_is_not_something_a_tool_call_can_carry(tmp_path: pathlib.Path) -> None:
    """The tool is bound to trusted policy; policy is not an argument.

    An agent that could pass `policy` could pass `{"allow": ["*"]}`. Every tool
    surface refuses arguments it does not know, so this is a refusal rather than
    a silently ignored field — an ignored one would look to the agent like it
    had worked.
    """
    path = _database(tmp_path)
    policy = Policy(name="reads", rules=[allow.query(sources=["analytics.*"])])
    db = gantry.sql.connect("duckdb", path=str(path), read_only=True, policy=policy)
    query_tool = db.query(schemas=["analytics"]).tool()
    materialize_tool = db.materialize(sources=["raw.*"], destinations=["agent_scratch.*"]).tool()

    assert "policy" not in str(query_tool.input_schema)
    assert "policy" not in str(materialize_tool.input_schema)
    for tool in (query_tool, materialize_tool):
        with pytest.raises(ValueError, match="unexpected"):
            await tool.invoke(
                {"sql": "SELECT revenue FROM analytics.customers", "policy": {"allow": ["*"]}}
            )


async def test_a_streaming_job_is_refused_before_it_is_submitted() -> None:
    """Flink is the case where submission is hardest to undo.

    A submitted streaming job is already running on a cluster, so the refusal
    has to land before the gateway is called at all — the transport must show no
    submission, not a submission that was later cancelled.
    """
    from tests.test_flink import FakeFlinkTransport

    transport = FakeFlinkTransport()
    policy = Policy(
        name="stream-authority",
        # Patterns are written against the qualified names, which is what the
        # connection's defaults make of the table names in the SQL.
        rules=[allow.stream(sources=["hive.events.*"], destinations=["hive.events.clean_events"])],
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
        outputs=["clean_events", "secret_events"],
        checks=[gantry.verify.running()],
        poll_interval=0,
    )

    with context(actor=actor("agent", "etl-agent")):
        refused = await job("INSERT INTO secret_events SELECT * FROM raw_events")

    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.execution is None
    assert transport.calls == [], "nothing may reach the gateway after a policy refusal"
    assert refused.admission is not None
    assert refused.admission.codes == ("NO_MATCHING_ALLOW",)
    # Names are qualified from the connection's own defaults, so the spelling
    # the SQL happened to use cannot decide whether a rule matched.
    assert refused.admission.request is not None
    assert refused.admission.request["outputs"] == ["hive.events.secret_events"]
    assert refused.admission.request["inputs"] == ["hive.events.raw_events"]

    with context(actor=actor("agent", "etl-agent")):
        allowed = await job("INSERT INTO clean_events SELECT * FROM raw_events")
    assert allowed.status is RunStatus.ACCEPTED
    assert allowed.admission is not None
    assert allowed.admission.policy == "stream-authority"
    assert transport.calls, "an authorized job must still be submitted"


async def test_a_batch_rule_does_not_authorize_a_stream_or_the_reverse() -> None:
    """The operation kind is part of the authority, not a label on it.

    A batch job that runs once and a streaming job that runs until someone stops
    it are different grants over the same two tables, so a policy written for one
    must not quietly cover the other.
    """
    from tests.test_flink import FakeFlinkTransport

    stream_only = Policy(
        name="stream-only",
        rules=[allow.stream(sources=["hive.events.*"], destinations=["hive.events.clean_events"])],
    )
    config = {
        "jobmanager_endpoint": "https://jobmanager.example",
        "default_catalog": "hive",
        "default_database": "events",
    }
    transport = FakeFlinkTransport()
    batch = gantry.batch.connect(
        "flink",
        endpoint="https://gateway.example/flink",
        policy=stream_only,
        config={**config, "transport": transport},
    )
    job = batch.job(inputs=["raw_events"], outputs=["clean_events"], poll_interval=0)

    with context(actor=actor("agent", "etl-agent")):
        refused = await job("INSERT INTO clean_events SELECT * FROM raw_events")

    assert refused.status is RunStatus.POLICY_REJECTED
    assert transport.calls == []
    assert refused.admission is not None
    assert refused.admission.codes == ("OPERATION_DENIED",)


def test_two_rules_do_not_combine_into_permission_neither_one_granted() -> None:
    """Reads compose across rules. Writes must not.

    Taken resource by resource, `raw.orders -> scratch_b.orders` looks authorized
    here: one rule allows reading `raw.*`, another allows writing `scratch_b.*`.
    Neither rule allows moving `raw` data into `scratch_b`, and a policy that
    granted it by cross-product would be widening authority, which is the one
    direction composition must never go.
    """
    policy = Policy(
        name="two-pipelines",
        rules=[
            allow.materialize(sources=["raw.*"], destinations=["scratch_a.*"]),
            allow.materialize(sources=["other.*"], destinations=["scratch_b.*"]),
        ],
    )
    crossed = _request(OperationKind.MATERIALIZE, inputs=["raw.orders"], outputs=["scratch_b.o"])
    intended = _request(OperationKind.MATERIALIZE, inputs=["raw.orders"], outputs=["scratch_a.o"])

    assert evaluate(policy, intended).allowed
    decision = evaluate(policy, crossed)
    assert not decision.allowed
    assert "no rule allows writing scratch_b.o" in decision.messages()


def test_one_rule_still_authorizes_every_source_it_names() -> None:
    """The narrowing above must not break the ordinary many-sources case."""
    policy = Policy(
        name="one-pipeline",
        rules=[allow.materialize(sources=["raw.*", "ref.*"], destinations=["scratch.*"])],
    )
    decision = evaluate(
        policy,
        _request(
            OperationKind.MATERIALIZE,
            inputs=["raw.orders", "ref.regions"],
            outputs=["scratch.joined"],
        ),
    )
    assert decision.allowed
