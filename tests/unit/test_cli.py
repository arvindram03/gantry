"""CLI behaviour."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from gantry import __version__
from gantry.cli import main
from gantry.cli.main import app
from typer.testing import CliRunner

runner = CliRunner()

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"
ORDERS = EXAMPLES / "dataset-orders.yaml"


def test_version_matches_package() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_bare_invocation_shows_help() -> None:
    result = runner.invoke(app, [])
    assert "Reliability and execution layer" in result.stdout


def test_no_command_is_still_a_placeholder() -> None:
    """This asserted that `results get` exited 2 as unbuilt. It is built, and
    it was the last one: nothing in the CLI now exits 2 to mean "planned"."""
    assert not hasattr(main, "_pending")


def test_results_commands_are_wired() -> None:
    result = runner.invoke(app, ["results", "--help"])
    assert result.exit_code == 0
    for command in ("get", "explain", "provenance", "refresh"):
        assert command in result.stdout


def test_the_access_ladder_is_reachable_from_the_cli() -> None:
    for group, commands in (
        ("policy", ("show",)),
        ("dataset", ("access", "query", "sample")),
    ):
        result = runner.invoke(app, [group, "--help"])
        assert result.exit_code == 0
        for command in commands:
            assert command in result.stdout, f"{group} {command}"


def test_policy_show_prints_the_shipped_defaults() -> None:
    """The defaults are the RFC's; printing them is how an operator checks."""
    result = runner.invoke(app, ["policy", "show"])
    assert result.exit_code == 0
    assert "rows deny" in result.stdout
    assert "aggregates allow" in result.stdout


def test_an_unreadable_policy_file_fails_rather_than_falling_back() -> None:
    result = runner.invoke(app, ["policy", "show", "--policy", "/nonexistent/policy.yaml"])
    assert result.exit_code == 1
    assert "rows deny" not in result.stdout


def test_validate_accepts_an_example(tmp_path: Path) -> None:
    result = runner.invoke(app, ["validate", str(ORDERS)])
    assert result.exit_code == 0
    assert "ok" in result.stdout


def test_validate_reports_a_bad_spec(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: gantry.dev/v1alpha1\nkind: Dataset\nmetadata:\n  name: x\n")
    result = runner.invoke(app, ["validate", str(bad)])
    assert result.exit_code == 1
    assert "physical" in result.stderr


def test_register_describe_and_list_round_trip(tmp_path: Path) -> None:
    registry = tmp_path / "registry.json"
    args = ["--registry", str(registry)]

    registered = runner.invoke(app, ["dataset", "register", str(ORDERS), *args])
    assert registered.exit_code == 0
    assert "orders@1" in registered.stdout

    listed = runner.invoke(app, ["dataset", "ls", *args])
    assert listed.exit_code == 0
    assert "orders" in listed.stdout
    assert "postgres" in listed.stdout

    described = runner.invoke(app, ["dataset", "describe", "orders", *args])
    assert described.exit_code == 0
    assert "public.orders" in described.stdout
    assert "order_id" in described.stdout
    assert "aggregate_or_masked" in described.stdout


def test_describe_reads_no_data(tmp_path: Path) -> None:
    """A manifest for an unreachable system still describes.

    Nothing in describe may open a connection: the adapter here does not exist.
    """
    registry = tmp_path / "registry.json"
    spec = tmp_path / "ghost.yaml"
    spec.write_text(
        "apiVersion: gantry.dev/v1alpha1\n"
        "kind: Dataset\n"
        "metadata:\n  name: ghost\n"
        "physical:\n"
        "  adapter: nonexistent\n"
        "  reference: nowhere.at.all\n"
        "  estimatedBytes: 14.2TB\n"
    )
    args = ["--registry", str(registry)]
    assert runner.invoke(app, ["dataset", "register", str(spec), *args]).exit_code == 0

    described = runner.invoke(app, ["dataset", "describe", "ghost", *args])
    assert described.exit_code == 0
    assert "14.2TB" in described.stdout


def test_re_registering_unchanged_spec_reports_unchanged(tmp_path: Path) -> None:
    args = ["--registry", str(tmp_path / "registry.json")]
    runner.invoke(app, ["dataset", "register", str(ORDERS), *args])
    again = runner.invoke(app, ["dataset", "register", str(ORDERS), *args])
    assert again.exit_code == 0
    assert "unchanged" in again.stdout
    assert "orders@1" in again.stdout


def test_describe_accepts_a_pinned_version(tmp_path: Path) -> None:
    registry = tmp_path / "registry.json"
    args = ["--registry", str(registry)]
    runner.invoke(app, ["dataset", "register", str(ORDERS), *args])

    changed = tmp_path / "orders-v2.yaml"
    changed.write_text(ORDERS.read_text().replace("public.orders", "public.orders_v2"))
    runner.invoke(app, ["dataset", "register", str(changed), *args])

    assert "orders_v2" in runner.invoke(app, ["dataset", "describe", "orders", *args]).stdout
    pinned = runner.invoke(app, ["dataset", "describe", "orders@1", *args])
    assert "public.orders" in pinned.stdout
    assert "orders_v2" not in pinned.stdout


def test_describe_unknown_dataset_exits_one(tmp_path: Path) -> None:
    args = ["--registry", str(tmp_path / "registry.json")]
    result = runner.invoke(app, ["dataset", "describe", "ghost", *args])
    assert result.exit_code == 1
    assert "not registered" in result.stderr


def test_ls_on_empty_registry_is_not_an_error(tmp_path: Path) -> None:
    result = runner.invoke(app, ["dataset", "ls", "--registry", str(tmp_path / "r.json")])
    assert result.exit_code == 0
    assert "no datasets" in result.stdout


def test_registry_path_from_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry = tmp_path / "env-registry.json"
    monkeypatch.setenv("GANTRY_REGISTRY", str(registry))
    assert runner.invoke(app, ["dataset", "register", str(ORDERS)]).exit_code == 0
    assert registry.is_file()


@pytest.mark.parametrize("kind", ["Dataset", "Movement", "Analysis"])
def test_schema_command_emits_generated_json(tmp_path: Path, kind: str) -> None:
    out = tmp_path / f"{kind}.json"
    result = runner.invoke(app, ["schema", "show", kind, "--out", str(out)])
    assert result.exit_code == 0
    assert json.loads(out.read_text())["title"] == f"Gantry {kind}"


def test_schema_command_rejects_unknown_kind() -> None:
    result = runner.invoke(app, ["schema", "show", "Nonsense"])
    assert result.exit_code == 1


def test_validate_warns_about_migration_blocks_on_a_movement() -> None:
    example = EXAMPLES / "movement-orders-replication.yaml"
    result = runner.invoke(app, ["validate", str(example)])
    assert result.exit_code == 0
    assert "Migration workflow" in result.stderr


def test_validate_accepts_every_example() -> None:
    for path in sorted(EXAMPLES.glob("*.yaml")):
        assert runner.invoke(app, ["validate", str(path)]).exit_code == 0, path
