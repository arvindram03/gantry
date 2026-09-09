# SPDX-License-Identifier: Apache-2.0
"""The parsing that stands between a policy and the engine.

Every bug these cover shipped, and every one was invisible to a fake transport:
the fake answered whatever was asked of it, so a mangled identifier or an
unread result page looked exactly like a correct one.

None of this needs a cluster, which is the point — a test that skips when
infrastructure is missing proves nothing on the day it is missing.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from gantry.execution import Execution, ExecutionState
from gantry.flink.adapter import _root_cause, _row_count
from gantry.flink.api import _terminal_result
from gantry.flink.metrics import FlinkMetrics
from gantry.flink.operation import _allowed, _normalize_identifier, _parse_job
from gantry.handle import ExecutionHandle


class TestIdentifierNormalisation:
    """A JDBC catalog exposes a PostgreSQL table as *one* identifier that
    contains a dot. Splitting on the dot produces two halves that name nothing,
    and if a same-named table exists in `public` it silently resolves to the
    wrong one."""

    @pytest.mark.parametrize(
        ("written", "expected"),
        [
            ("`analytics.orders`", "analytics.orders"),
            ("`pg`.`gantry`.`analytics.orders`", "pg.gantry.analytics.orders"),
            ('"analytics.orders"', "analytics.orders"),
            ("analytics.orders", "analytics.orders"),
            ("`orders`", "orders"),
            ("orders", "orders"),
            ("  `analytics` . `orders` ", "analytics.orders"),
        ],
    )
    def test_quoting_is_stripped_without_splitting_inside_it(
        self, written: str, expected: str
    ) -> None:
        assert _normalize_identifier(written) == expected

    def test_a_dot_inside_quotes_is_part_of_the_name(self) -> None:
        """The case that made every non-public schema unusable."""
        assert _normalize_identifier("`reporting.orders_replica`") == ("reporting.orders_replica")
        # And not two identifiers.
        assert _normalize_identifier("`reporting`.`orders_replica`") == ("reporting.orders_replica")


class TestRootCause:
    """Flink returns the whole server-side stack. The one sentence that says
    what is wrong is buried in it, and the thing most likely to read the
    message is a model deciding how to correct its own SQL."""

    def test_the_innermost_cause_wins(self) -> None:
        message = (
            "Internal server error.; <Exception on server side:\n"
            "org.apache.flink.table.api.ValidationException: SQL validation failed.\n"
            "\tat org.apache.flink.table.SomeClass.method(SomeClass.java:1)\n"
            "Caused by: org.apache.flink.table.api.ValidationException: outer\n"
            "\tat org.apache.calcite.Frame.method(Frame.java:2)\n"
            "Caused by: org.apache.calcite.sql.validate.SqlValidatorException: "
            "Column 'no_such_column' not found in any table\n"
        )
        assert _root_cause(message) == "Column 'no_such_column' not found in any table"

    def test_a_message_with_no_cause_is_left_alone(self) -> None:
        assert _root_cause("Connection refused") == "Connection refused"

    def test_only_the_first_line_survives_a_bare_stack(self) -> None:
        assert _root_cause("Boom\n\tat Frame.method(Frame.java:1)") == "Boom"


def payload(*rows: tuple[object, str]) -> dict[str, object]:
    """A gateway result payload, in its changelog form."""
    return {
        "results": {
            "columns": [{"name": "EXPR$0"}],
            "data": [{"kind": kind, "fields": [value]} for value, kind in rows],
        }
    }


class TestRowCount:
    """Flink computes COUNT(*) incrementally and returns the working as a
    changelog. Reading the first row gives 1 for any non-empty table — a
    plausible number, and always wrong."""

    def test_the_last_retained_value_wins(self) -> None:
        assert (
            _row_count(
                payload(
                    (1, "INSERT"),
                    (1, "UPDATE_BEFORE"),
                    (2, "UPDATE_AFTER"),
                    (2, "UPDATE_BEFORE"),
                    (200, "UPDATE_AFTER"),
                )
            )
            == 200
        )

    def test_a_retraction_is_never_the_answer(self) -> None:
        assert _row_count(payload((37, "UPDATE_AFTER"), (37, "UPDATE_BEFORE"))) == 37

    def test_an_empty_table_counts_zero(self) -> None:
        assert _row_count(payload((0, "INSERT"))) == 0

    def test_no_rows_at_all_is_unknown_rather_than_zero(self) -> None:
        """A count that never arrived and a count of zero are different facts."""
        assert _row_count(payload()) is None

    def test_a_string_value_is_read_as_a_number(self) -> None:
        assert _row_count(payload(("195715", "INSERT"))) == 195715

    def test_something_that_is_not_a_count_is_refused(self) -> None:
        assert _row_count(payload((True, "INSERT"))) is None
        assert _row_count(payload(("not a number", "INSERT"))) is None


class TestJobPlan:
    """What the policy check is handed.

    The inputs and the output are extracted by different code paths, and only
    the output was ever normalised — so a quoted input reached the allow-list
    still wearing its backticks and matched nothing. Every table outside
    `public` is quoted, so every one of them was unusable.
    """

    def test_inputs_and_output_are_normalised_alike(self) -> None:
        plan = _parse_job(
            "INSERT INTO `reporting.orders_replica` SELECT order_id FROM `analytics.orders`"
        )
        assert plan.output == "reporting.orders_replica"
        assert plan.inputs == ("analytics.orders",)

    def test_an_unquoted_statement_is_unchanged(self) -> None:
        plan = _parse_job("INSERT INTO reporting.replica SELECT id FROM analytics.orders")
        assert plan.output == "reporting.replica"
        assert plan.inputs == ("analytics.orders",)

    def test_the_destination_is_not_also_reported_as_an_input(self) -> None:
        """Writing to a table does not make it something the job reads, and a
        policy that had to allow its own destination as an input would be
        stating the opposite of what the operator meant."""
        plan = _parse_job(
            "INSERT INTO `analytics.orders` SELECT order_id FROM `analytics.orders_backup`"
        )
        assert plan.output == "analytics.orders"
        assert "analytics.orders" not in plan.inputs

    def test_a_job_admits_a_schema_qualified_name(self) -> None:
        """The end-to-end shape of the bug: a policy naming real tables, and a
        statement written the way the catalog requires."""
        plan = _parse_job(
            "INSERT INTO `reporting.orders_replica` "
            "SELECT order_id, amount FROM `analytics.orders` WHERE status = 'paid'"
        )
        assert _allowed(plan.output, ("reporting.orders_replica",))
        assert all(_allowed(name, ("analytics.orders",)) for name in plan.inputs)


class TestTerminalStates:
    """Which states end a wait.

    SUCCEEDED was not one of them, which is only visible for a *streaming*
    job: a pipeline over a bounded source completes instead of settling into
    RUNNING, and a wait that recognised only failure states spun until its
    timeout waiting for a steady state that had already come and gone.
    """

    @staticmethod
    def _execution(state: ExecutionState) -> Execution:
        return Execution(
            ExecutionHandle(gantry_id="run_1", engine="flink", target="flink", native_id="job-1"),
            state,
            updated_at=datetime.now(UTC),
        )

    @pytest.mark.parametrize(
        ("state", "status"),
        [
            (ExecutionState.SUCCEEDED, "ACCEPTED"),
            (ExecutionState.FAILED, "FAILED"),
            (ExecutionState.CANCELLED, "CANCELLED"),
            (ExecutionState.UNKNOWN, "UNKNOWN"),
        ],
    )
    def test_a_finished_job_ends_the_wait(self, state: ExecutionState, status: str) -> None:
        result = _terminal_result(self._execution(state), FlinkMetrics())
        assert result is not None, f"{state.value} must end the wait, not spin"
        assert result.status.value == status

    @pytest.mark.parametrize("state", [ExecutionState.RUNNING, ExecutionState.PENDING])
    def test_a_job_still_going_does_not(self, state: ExecutionState) -> None:
        assert _terminal_result(self._execution(state), FlinkMetrics()) is None
