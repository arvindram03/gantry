# SPDX-License-Identifier: Apache-2.0
"""The Beam job generator.

What is checked here is the *shape* of the pipeline. That it moves rows, and
that a replay changes nothing, is proven against real databases in the
integration suite — the same division as the SQL job, and for the same reason:
an assertion on generated text checks what was written, never what was omitted.
"""

from __future__ import annotations

import ast

import pytest
from gantry.core import DatasetManifest, DatasetSchema, FieldSchema, PhysicalRef
from gantry.jobs.model import JobKind
from gantry.jobs.packaging import beam_packaging
from gantry.movement.beamjob import (
    JDBC_SECRETS,
    JdbcSink,
    compile_snapshot_job,
    snapshot_pipeline,
    unit_of,
)
from gantry.movement.partitioning import Partition, PartitionMethod


def constant(body: str, name: str) -> str:
    """Read a top-level string constant out of the generated program.

    Parsed rather than matched, because the pipeline embeds its SQL with repr()
    and asserting on the escaped bytes would test the quoting instead of the
    query. It also proves the generated body parses as Python at all.
    """
    tree = ast.parse(body)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            assert isinstance(node.value, ast.Constant)
            return str(node.value.value)
    raise AssertionError(f"no {name} in the generated pipeline")


FIELDS = (
    FieldSchema(name="order_id", type="bigint", nullable=False),
    FieldSchema(name="amount", type="numeric(12,2)"),
    FieldSchema(name="source_lsn", type="bigint"),
)


def manifest() -> DatasetManifest:
    return DatasetManifest.model_validate(
        {
            "name": "public.orders",
            "physical": PhysicalRef(adapter="postgres", reference="public.orders"),
            "dataset_schema": DatasetSchema(keys=("order_id",), fields=FIELDS),
        }
    )


def partition(index: int = 0, lo: str | None = "1000", hi: str | None = "2000") -> Partition:
    return Partition.model_validate(
        {
            "dataset": "public.orders",
            "index": index,
            "column": "order_id",
            "method": PartitionMethod.HISTOGRAM,
            "lo": lo,
            "hi": hi,
        }
    )


def packaging() -> object:
    return beam_packaging(secrets=JDBC_SECRETS)


class TestThePipeline:
    def test_bounds_come_from_the_plan(self) -> None:
        """Never recomputed: recomputing lets a partition move under a replay."""
        query = constant(
            snapshot_pipeline(manifest(), [partition()], sink=JdbcSink("public.orders_copy")),
            "QUERY",
        )
        assert "CAST('1000' AS bigint)" in query
        assert "CAST('2000' AS bigint)" in query

    def test_several_partitions_become_one_disjunction(self) -> None:
        """Beam's startup cost makes one job per partition unaffordable, so a
        job may cover several — but only the rows those partitions name."""
        body = snapshot_pipeline(
            manifest(),
            [partition(0, "1", "100"), partition(1, "500", "600")],
            sink=JdbcSink("public.orders_copy"),
        )
        assert body.count('order_id" >=') == 2
        assert " OR " in body

    def test_the_write_is_an_upsert_not_an_insert(self) -> None:
        """Beam's own JDBC write is a plain INSERT, which a replay duplicates.
        Idempotence is Gantry's guarantee and is passed explicitly."""
        body = snapshot_pipeline(manifest(), [partition()], sink=JdbcSink("public.orders_copy"))
        upsert = constant(body, "UPSERT")
        assert "ON CONFLICT" in upsert
        assert "DO UPDATE SET" in upsert
        assert "statement=UPSERT" in body

    def test_a_placeholder_for_every_column_in_schema_order(self) -> None:
        """JdbcIO binds positionally, so a missing placeholder is a silent
        column shift rather than an error."""
        upsert = constant(
            snapshot_pipeline(manifest(), [partition()], sink=JdbcSink("public.orders_copy")),
            "UPSERT",
        )
        assert upsert.count("?") == 3

    def test_a_snapshot_position_is_stamped_into_the_read(self) -> None:
        """Stamped in the source query: Beam moves rows opaquely, so there is no
        later point that still knows what the position was."""
        query = constant(
            snapshot_pipeline(
                manifest(), [partition()], sink=JdbcSink("public.orders_copy"), snapshot_lsn=99
            ),
            "QUERY",
        )
        assert 'CAST(99 AS bigint) AS "source_lsn"' in query

    def test_no_credential_appears_in_the_body(self) -> None:
        """The body is retained as provenance and is meant to be read."""
        body = snapshot_pipeline(manifest(), [partition()], sink=JdbcSink("public.orders_copy"))
        for secret in JDBC_SECRETS:
            assert f"os.environ[{secret!r}]" in body
        assert "password='" not in body
        assert "jdbc:postgresql" not in body, "a connection string is a secret"

    def test_a_job_with_no_partitions_is_refused(self) -> None:
        with pytest.raises(ValueError, match="at least one partition"):
            snapshot_pipeline(manifest(), [], sink=JdbcSink("public.orders_copy"))


class TestTheJob:
    def test_the_unit_is_stable_however_the_plan_ordered_it(self) -> None:
        """A replay must compile to the same job, or the runner will not
        recognise the work already in flight."""
        one = [partition(0), partition(1), partition(2)]
        assert unit_of(one) == unit_of(list(reversed(one)))

    def test_the_job_is_content_addressed_and_kind_beam(self) -> None:
        first = compile_snapshot_job(
            "orders-replication",
            manifest(),
            [partition()],
            sink=JdbcSink("public.orders_copy"),
            packaging=packaging(),  # type: ignore[arg-type]
        )
        second = compile_snapshot_job(
            "orders-replication",
            manifest(),
            [partition()],
            sink=JdbcSink("public.orders_copy"),
            packaging=packaging(),  # type: ignore[arg-type]
        )
        assert first.kind is JobKind.BEAM
        assert first.content_hash == second.content_hash, "generation time must not enter the hash"

    def test_a_different_partition_set_is_a_different_job(self) -> None:
        one = compile_snapshot_job(
            "orders-replication",
            manifest(),
            [partition(0)],
            sink=JdbcSink("public.orders_copy"),
            packaging=packaging(),  # type: ignore[arg-type]
        )
        two = compile_snapshot_job(
            "orders-replication",
            manifest(),
            [partition(0), partition(1, "9000", "9999")],
            sink=JdbcSink("public.orders_copy"),
            packaging=packaging(),  # type: ignore[arg-type]
        )
        assert one.content_hash != two.content_hash

    def test_the_body_is_handed_to_python_not_a_shell(self) -> None:
        """A generated pipeline is a Python program. Which interpreter an image
        can run is a fact about the image, so it travels with the packaging."""
        job = compile_snapshot_job(
            "orders-replication",
            manifest(),
            [partition()],
            sink=JdbcSink("public.orders_copy"),
            packaging=packaging(),  # type: ignore[arg-type]
        )
        assert job.packaging.interpreter == ("python", "-c")
