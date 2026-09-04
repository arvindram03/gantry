"""Smoke tests for the CLI scaffold."""

from __future__ import annotations

from gantry import __version__
from gantry.cli.main import app
from typer.testing import CliRunner

runner = CliRunner()


def test_version_matches_package() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_bare_invocation_shows_help() -> None:
    result = runner.invoke(app, [])
    assert "Reliability and execution layer" in result.stdout


def test_unimplemented_command_exits_two() -> None:
    """Planned-but-unbuilt commands must fail loudly, never silently succeed."""
    result = runner.invoke(app, ["dataset", "ls"])
    assert result.exit_code == 2
