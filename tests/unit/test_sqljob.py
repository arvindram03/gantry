# SPDX-License-Identifier: Apache-2.0
"""The generated SQL job.

Three of these tests pin Day 0 findings that are **silent when wrong** — the
script still runs, and still appears to work, while a guarantee has quietly
gone. Those are the ones worth reading.

The rest pin the two nested quoting contexts. A partition bound is a value out
of the customer's data, and it is written into SQL that is written into a shell
script, so it is escaped twice and both layers have to hold.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from gantry.core import DatasetManifest, DatasetSchema, FieldSchema, PhysicalRef
from gantry.jobs import JobKind
from gantry.movement.partitioning import Partition, PartitionMethod
from gantry.movement.predicate import sql_literal
from gantry.movement.sqljob import (
    SOURCE_DSN,
    TARGET_DSN,
    compile_snapshot_job,
    shell_literal,
    snapshot_script,
)

AT = datetime(2026, 9, 5, tzinfo=UTC)

FIELDS = (
    FieldSchema(name="order_id", type="bigint", nullable=False),
    FieldSchema(name="amount", type="numeric(12,2)"),
    FieldSchema(name="source_lsn", type="bigint"),
)


def manifest(**overrides: object) -> DatasetManifest:
    base: dict[str, object] = {
        "name": "public.orders",
        "physical": PhysicalRef(adapter="postgres", reference="public.orders"),
        "dataset_schema": DatasetSchema(keys=("order_id",), fields=FIELDS),
    }
    base.update(overrides)
    return DatasetManifest.model_validate(base)


def partition(**overrides: object) -> Partition:
    base: dict[str, object] = {
        "dataset": "public.orders",
        "index": 4,
        "column": "order_id",
        "method": PartitionMethod.HISTOGRAM,
        "lo": "1000",
        "hi": "2000",
    }
    base.update(overrides)
    return Partition.model_validate(base)


def script(**overrides: object) -> str:
    return snapshot_script(manifest(), partition(), target="public.orders", **overrides)  # type: ignore[arg-type]


class TestTheDayZeroFindings:
    """Each of these is silent when wrong. That is why they are tested."""

    def test_copy_from_stdin_is_passed_with_dash_c(self) -> None:
        """In a `-f` script psql reads COPY data from the file itself, and the
        obvious formulation fails with "COPY file signature not recognized"."""
        text = script()
        assert "COPY gantry_staging" in text
        assert " -f " not in text, "COPY FROM STDIN cannot be read from a script file"

    def test_pipefail_is_set(self) -> None:
        """Without it a source-side failure is hidden by a target stage that
        exited cleanly. The binary COPY trailer catches it today, but that is a
        property of the wire format rather than of this script."""
        assert "set -o pipefail" in script()

    def test_the_target_runs_in_one_transaction_that_stops_on_error(self) -> None:
        """Together these are what make exit 0 mean committed."""
        text = script()
        assert "--single-transaction" in text
        assert text.count("ON_ERROR_STOP=1") == 2, "both sides, not just the target"


class TestQuoting:
    def test_a_bound_containing_a_quote_survives_both_layers(self) -> None:
        """Asserted on structure, not on exact bytes.

        The SQL doubles the quote and the shell then escapes both, so matching
        the result as a substring would test my arithmetic rather than the
        escaping. That the escaping actually works is proven by running a
        hostile bound against a real database — see the integration suite.
        """
        text = snapshot_script(manifest(), partition(lo="O'Brien", hi=None), target="public.orders")
        assert "Brien" in text
        assert text.count("'") % 2 == 0, "an odd number of quotes cannot be balanced"

    def test_a_bound_cannot_close_the_shell_quote(self) -> None:
        """The SQL sits inside a single-quoted shell word, so a quote in the
        data has to survive two layers."""
        text = snapshot_script(
            manifest(),
            partition(lo="'; DROP TABLE orders; --", hi=None),
            target="public.orders",
        )
        assert "DROP TABLE orders" in text, "the value is present…"
        assert "'\\''" in text, "…but neutralised by shell escaping"

    @pytest.mark.parametrize("bad", ["a\nb", "a\x00b", "a\tb"])
    def test_control_characters_in_a_bound_are_refused(self, bad: str) -> None:
        """A bound needing this much escaping is a bug upstream, and letting it
        through would let a value break out of the line it is written on."""
        with pytest.raises(ValueError, match="control characters"):
            sql_literal(bad)

    def test_an_unsafe_identifier_is_refused(self) -> None:
        broken = manifest(physical=PhysicalRef(adapter="postgres", reference='public.orders"; --'))
        with pytest.raises(ValueError, match="unsafe identifier"):
            snapshot_script(broken, partition(), target="public.orders")

    def test_shell_escaping_closes_and_reopens_the_quote(self) -> None:
        assert shell_literal("it's") == "'it'\\''s'"


class TestTheScript:
    def test_bounds_are_typed_from_the_schema(self) -> None:
        """Bounds travel as text so the runtime need not know the key's type;
        the cast puts it back at the point of use."""
        text = script()
        assert "AS bigint)" in text, "the bound is cast back to the column's type"
        assert "1000" in text and "2000" in text
        assert ">=" in text and "<" in text

    def test_an_unbounded_partition_still_produces_valid_sql(self) -> None:
        text = snapshot_script(manifest(), partition(lo=None, hi=None), target="public.orders")
        assert "WHERE TRUE" in text

    def test_the_merge_upserts_on_the_key(self) -> None:
        text = script()
        assert 'ON CONFLICT ("order_id") DO UPDATE' in text
        assert '"amount" = EXCLUDED."amount"' in text
        assert '"order_id" = EXCLUDED' not in text, "the key is not updated"

    def test_a_snapshot_position_refuses_to_overwrite_anything_newer(self) -> None:
        """What makes a snapshot and a change stream safe to run at once.

        Two halves, and checking only the guard is how the missing half got
        shipped: rows must also be *stamped* with the position, or the target
        understates how current it is and the stream redoes work the snapshot
        already holds.
        """
        text = script(snapshot_lsn=4242)
        assert "CAST(4242 AS bigint) AS " in text, "the snapshot position is not stamped"
        assert "source_lsn" in text and "EXCLUDED." in text, "no guard against a newer row"

    def test_no_snapshot_position_means_no_stamp(self) -> None:
        """Without a position there is nothing to stamp, and the source's own
        value must survive untouched."""
        assert "AS bigint) AS " not in script()

    def test_an_all_key_table_does_nothing_on_conflict(self) -> None:
        keys_only = manifest(dataset_schema=DatasetSchema(keys=("order_id",), fields=(FIELDS[0],)))
        text = snapshot_script(keys_only, partition(), target="public.orders")
        assert "DO NOTHING" in text

    def test_a_dataset_without_a_key_is_refused(self) -> None:
        """Idempotent writes detect a conflict on the key; without one a
        replayed job duplicates instead of upserting."""
        keyless = manifest(dataset_schema=DatasetSchema(keys=(), fields=FIELDS))
        with pytest.raises(ValueError, match="no key"):
            snapshot_script(keyless, partition(), target="public.orders")


class TestTheJob:
    def test_credentials_are_named_not_valued(self) -> None:
        """The body is retained as provenance and meant to be read."""
        job = compile_snapshot_job(
            "orders-snapshot",
            manifest(),
            partition(),
            target="public.orders",
            generated_at=AT,
        )
        assert job.packaging.secrets == (SOURCE_DSN, TARGET_DSN)
        assert f"${SOURCE_DSN}" in job.body
        assert "password" not in job.body.lower()

    def test_the_unit_is_the_partition(self) -> None:
        job = compile_snapshot_job(
            "orders-snapshot",
            manifest(),
            partition(),
            target="public.orders",
            generated_at=AT,
        )
        assert job.unit == partition().id
        assert job.kind is JobKind.SQL

    def test_two_partitions_are_two_jobs(self) -> None:
        one = compile_snapshot_job(
            "orders-snapshot",
            manifest(),
            partition(index=4, lo="1000", hi="2000"),
            target="public.orders",
            generated_at=AT,
        )
        two = compile_snapshot_job(
            "orders-snapshot",
            manifest(),
            partition(index=5, lo="2000", hi="3000"),
            target="public.orders",
            generated_at=AT,
        )
        assert one.content_hash != two.content_hash

    def test_recompiling_the_same_partition_is_the_same_job(self) -> None:
        """Idempotent submission depends on this: the container name derives
        from the hash, so an unstable hash would start a second copy."""
        made = [
            compile_snapshot_job(
                "orders-snapshot",
                manifest(),
                partition(),
                target="public.orders",
                generated_at=stamp,
            )
            for stamp in (AT, datetime(2027, 3, 3, tzinfo=UTC))
        ]
        assert made[0].content_hash == made[1].content_hash
