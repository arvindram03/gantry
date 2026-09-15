# SPDX-License-Identifier: Apache-2.0
"""What `retryable` means, and what it does not.

The flag was set on every failure in the library and read by nothing, with each
adapter carrying its own idea of which kinds deserved it. These tests pin the
rule in one place, prove the library obeys it, and cover the half the flag never
answered: whether running the same proposal again is safe.
"""

from __future__ import annotations

import ast
import pathlib

import duckdb
import gantry
import pytest
from gantry.failure import Failure, FailureKind
from gantry.runs.model import ExecutionRecord, OperationKind
from gantry.runs.status import RunStatus

from _runs import make_run

ROOT = pathlib.Path(__file__).resolve().parents[1] / "gantry"


# --------------------------------------------------------------------------
# One rule, and the whole library obeys it
# --------------------------------------------------------------------------


def test_transient_is_a_short_conservative_list() -> None:
    """Only conditions that genuinely pass on their own.

    Pinned as a literal so widening it is a deliberate edit with a test diff,
    not something that happens while adding an adapter. The cost of the two
    mistakes is not symmetric: calling a permanent failure transient invites a
    caller to retry forever.
    """
    transient = {kind for kind in FailureKind if kind.transient}

    assert transient == {
        FailureKind.TIMEOUT,
        FailureKind.RESOURCE_ERROR,
        FailureKind.RESOURCE_EXHAUSTED,
    }
    # The three deliberately left out, each because Gantry cannot tell.
    assert not FailureKind.CONNECTOR_ERROR.transient
    assert not FailureKind.ENGINE_ERROR.transient
    assert not FailureKind.UNKNOWN.transient
    # And the refusals, which no amount of waiting changes.
    assert not FailureKind.POLICY_REJECTED.transient
    assert not FailureKind.VERIFICATION_FAILED.transient


def _literal_failures() -> list[tuple[str, str, bool]]:
    """Every `Failure(...)` in the library built from a literal kind and flag."""

    def kind_of(node: ast.expr) -> str | None:
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "FailureKind"
        ):
            return node.attr
        return None

    def flag_of(node: ast.expr) -> bool | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, bool):
            return node.value
        return None

    found: list[tuple[str, str, bool]] = []
    for path in sorted(ROOT.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "Failure"
            ):
                continue
            kind = kind_of(node.args[0]) if node.args else None
            flag = flag_of(node.args[1]) if len(node.args) > 1 else None
            for keyword in node.keywords:
                if keyword.arg == "kind":
                    kind = kind_of(keyword.value)
                if keyword.arg == "retryable":
                    flag = flag_of(keyword.value)
            if kind is not None and flag is not None:
                found.append((f"{path.name}:{node.lineno}", kind, flag))
    return found


def test_no_failure_in_the_library_disagrees_with_its_kind() -> None:
    """The drift guard.

    A new adapter that hardcodes `retryable=False` on a timeout makes the flag
    mean something different on that backend than everywhere else, and nothing
    but this notices — the value is plausible, the tests pass, and a caller
    acting on it silently stops retrying the one failure that deserves it.
    """
    sites = _literal_failures()
    assert len(sites) > 40, "the audit found almost nothing; it has probably stopped parsing"

    disagreements = [
        f"{where}: {kind} says retryable={flag}, kind.transient is {FailureKind[kind].transient}"
        for where, kind, flag in sites
        if flag is not FailureKind[kind].transient
    ]
    assert not disagreements, "\n".join(disagreements)


def test_each_adapter_classifier_agrees_with_the_shared_rule() -> None:
    """The dynamic classifiers, which the source audit above cannot see.

    Each of these computes a kind at runtime and then decides the flag from it,
    independently. They are the places the rule was duplicated, so they are the
    places it can drift.
    """
    from gantry.flink.adapter import _classify_failure
    from gantry.sql.adapters.mysql import _failure as mysql_failure

    flink_resources = _classify_failure("java.lang.OutOfMemoryError: Java heap space")
    assert flink_resources.kind is FailureKind.RESOURCE_ERROR
    assert flink_resources.retryable is FailureKind.RESOURCE_ERROR.transient

    flink_syntax = _classify_failure("SQL parse failed. Encountered 'FROM'")
    assert flink_syntax.kind is FailureKind.SYNTAX_ERROR
    assert flink_syntax.retryable is FailureKind.SYNTAX_ERROR.transient

    # The ambiguous one, and the reason it is not transient: this same kind is
    # a broker that is down and a sink table that does not exist.
    flink_connector = _classify_failure("Kafka connector failed to reach the broker")
    assert flink_connector.kind is FailureKind.CONNECTOR_ERROR
    assert flink_connector.retryable is FailureKind.CONNECTOR_ERROR.transient

    timeout = mysql_failure(_MySQLError(3024, "Query execution was interrupted"))
    assert timeout.kind is FailureKind.TIMEOUT
    assert timeout.retryable is FailureKind.TIMEOUT.transient

    syntax = mysql_failure(_MySQLError(1064, "You have an error in your SQL syntax"))
    assert syntax.kind is FailureKind.SYNTAX_ERROR
    assert syntax.retryable is FailureKind.SYNTAX_ERROR.transient


class _MySQLError(Exception):
    """Shaped like the driver's error: the code is `args[0]`."""


# --------------------------------------------------------------------------
# The half the flag never answered
# --------------------------------------------------------------------------


def _failed(kind: FailureKind, *, operation: OperationKind, native_id: str | None) -> gantry.Run:
    execution = None if native_id is None else ExecutionRecord(status="FAILED", native_id=native_id)
    return make_run(
        status=RunStatus.EXECUTION_FAILED,
        kind=operation,
        execution=execution,
        failure=Failure(kind, kind.transient, "engine said so"),
    )


def test_a_run_with_nothing_wrong_is_not_something_to_retry() -> None:
    assert not make_run(status=RunStatus.ACCEPTED).safe_to_retry
    parked = make_run(status=RunStatus.AWAITING_CONFIRMATION)
    assert not parked.safe_to_retry, "a parked run is confirmed, not retried"


def test_a_permanent_failure_is_never_retry_safe() -> None:
    for kind in (FailureKind.POLICY_REJECTED, FailureKind.SYNTAX_ERROR, FailureKind.UNKNOWN):
        run = _failed(kind, operation=OperationKind.QUERY, native_id=None)
        assert not run.safe_to_retry, kind


def test_a_timed_out_query_is_retry_safe_however_far_it_got() -> None:
    """Reads are idempotent. That is the whole reason this case is simple."""
    for native_id in (None, "pg_1234"):
        run = _failed(FailureKind.TIMEOUT, operation=OperationKind.QUERY, native_id=native_id)
        assert run.safe_to_retry


@pytest.mark.parametrize(
    "operation", [OperationKind.MATERIALIZE, OperationKind.BATCH, OperationKind.STREAM]
)
def test_a_write_is_retry_safe_only_if_nothing_reached_the_engine(
    operation: OperationKind,
) -> None:
    """The distinction the flag alone cannot make.

    Same failure kind, same transient condition — and one of these may be run
    again while the other may have already left something behind.
    """
    never_submitted = _failed(FailureKind.TIMEOUT, operation=operation, native_id=None)
    reached_the_engine = _failed(FailureKind.TIMEOUT, operation=operation, native_id="job_1")

    assert never_submitted.safe_to_retry
    assert not reached_the_engine.safe_to_retry


# --------------------------------------------------------------------------
# Why the write rule is what it is, against a real engine
# --------------------------------------------------------------------------


async def test_rerunning_a_materialization_really_does_fail(tmp_path: pathlib.Path) -> None:
    """The claim `safe_to_retry` is protecting callers from, demonstrated.

    Materialization is create-only by design. Once the destination exists the
    same proposal no longer produces the same outcome — it produces
    `DESTINATION_EXISTS`. A caller who saw `retryable=True` and simply ran it
    again would turn a transient failure into one that looks permanent, which is
    why the flag alone was never enough to act on.
    """
    path = tmp_path / "retry.duckdb"
    native = duckdb.connect(str(path))
    native.execute("CREATE SCHEMA raw")
    native.execute("CREATE SCHEMA scratch")
    native.execute("CREATE TABLE raw.invoices(customer_id INTEGER, balance INTEGER)")
    native.execute("INSERT INTO raw.invoices VALUES (1, 10), (2, 20)")
    native.close()

    db = gantry.sql.connect("duckdb", path=str(path))
    materialize = db.materialize(sources=["raw.*"], destinations=["scratch.*"])
    sql = (
        "CREATE TABLE scratch.totals AS "
        "SELECT customer_id, SUM(balance) AS balance FROM raw.invoices GROUP BY customer_id"
    )

    first = await materialize(sql)
    again = await materialize(sql)

    assert first.status is RunStatus.ACCEPTED
    assert again.status is RunStatus.POLICY_REJECTED
    assert again.failure is not None
    assert again.failure.kind is FailureKind.DESTINATION_EXISTS
    # And the refusal is permanent, so nobody is invited to keep trying.
    assert not again.failure.retryable
    assert not again.safe_to_retry
