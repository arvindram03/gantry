# SPDX-License-Identifier: Apache-2.0
"""Parsing `kind: Migration`, and the defaults it refuses to relax."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from gantry.migration.model import CutoverGates, Migration, RollbackPolicy
from gantry.spec import load_migration_spec, load_spec
from gantry.spec.errors import SpecValidationError
from gantry.spec.migration import MigrationSpec
from pydantic import ValidationError

EXAMPLE = Path(__file__).resolve().parents[2] / "spec/examples/migration-orders-to-warehouse.yaml"

MINIMAL = """
apiVersion: gantry.dev/v1alpha1
kind: Migration
metadata:
  name: orders-to-warehouse
movements:
  - orders-snapshot
"""


def parse(text: str) -> MigrationSpec:
    return load_migration_spec("<test>", text)


class TestParsing:
    def test_the_shipped_example_parses(self) -> None:
        spec = load_migration_spec(EXAMPLE)
        migration = spec.to_migration()
        assert migration.name == "orders-to-warehouse"
        assert migration.movements == ("orders-snapshot",)
        assert migration.cutover.max_cdc_lag == timedelta(seconds=2)
        assert migration.rollback.window == timedelta(hours=24)

    def test_the_generic_loader_dispatches_on_kind(self) -> None:
        assert isinstance(load_spec(EXAMPLE), MigrationSpec)

    def test_a_movement_may_be_named_bare_or_tagged(self) -> None:
        """Both read naturally; the tagged form leaves room for per-Movement
        options later without a breaking change."""
        bare = parse(MINIMAL).to_migration()
        tagged = parse(
            MINIMAL.replace("  - orders-snapshot", "  - movement: orders-snapshot")
        ).to_migration()
        assert bare.movements == tagged.movements == ("orders-snapshot",)

    def test_at_least_one_movement_is_required(self) -> None:
        """A Migration with nothing to run is a cutover of nothing."""
        with pytest.raises(SpecValidationError):
            parse(MINIMAL.replace("  - orders-snapshot", "  []"))

    def test_duplicate_movements_are_refused(self) -> None:
        with pytest.raises(SpecValidationError, match="duplicate"):
            parse(MINIMAL + "  - orders-snapshot\n")

    def test_an_unknown_field_is_refused_rather_than_ignored(self) -> None:
        with pytest.raises(SpecValidationError):
            parse(MINIMAL + "cutovr:\n  gates: {}\n")

    def test_durations_accept_the_spec_spelling(self) -> None:
        spec = parse(
            MINIMAL + "cutover:\n  gates:\n    maxCdcLag: 500ms\nrollback:\n  window: 7d\n"
        ).to_migration()
        assert spec.cutover.max_cdc_lag == timedelta(milliseconds=500)
        assert spec.rollback.window == timedelta(days=7)


class TestDefaultsAreTheStrictAnswer:
    """An omitted gate must not be a way to disable it, or the spec becomes a
    place to quietly turn checks off."""

    def test_a_spec_declaring_no_gates_still_enforces_all_of_them(self) -> None:
        gates = parse(MINIMAL).to_migration().cutover
        assert gates == CutoverGates()
        assert gates.all_partitions_verified
        assert gates.require_approval
        assert gates.critical_verification_failures == 0
        assert gates.target_healthy
        assert gates.schema_compatible

    def test_approval_is_required_unless_someone_writes_otherwise(self) -> None:
        relaxed = parse(MINIMAL + "cutover:\n  gates:\n    requireApproval: false\n").to_migration()
        assert not relaxed.cutover.require_approval, "relaxing must be possible, but explicit"

    def test_the_source_stays_authoritative_by_default(self) -> None:
        assert parse(MINIMAL).to_migration().rollback.source_remains_authoritative


class TestModelInvariants:
    def test_a_negative_lag_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="negative"):
            CutoverGates(max_cdc_lag=timedelta(seconds=-1))

    def test_a_negative_rollback_window_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="negative"):
            RollbackPolicy(window=timedelta(hours=-1))

    def test_a_window_with_nothing_to_roll_back_to_is_refused(self) -> None:
        """Keeping a window open while releasing the source promises a
        rollback that cannot happen."""
        with pytest.raises(ValidationError, match="nothing to roll back to"):
            RollbackPolicy(window=timedelta(hours=24), source_remains_authoritative=False)

    def test_releasing_the_source_is_allowed_with_no_window(self) -> None:
        RollbackPolicy(window=timedelta(0), source_remains_authoritative=False)

    def test_duplicate_movements_are_refused_in_the_model_too(self) -> None:
        """The spec is one door to the model, not the only one."""
        with pytest.raises(ValidationError, match="duplicate"):
            Migration(name="m", movements=("a", "a"))
