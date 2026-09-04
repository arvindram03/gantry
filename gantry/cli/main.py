"""Gantry command line interface.

Day 1 scaffold: the command surface is fixed so the shape of the tool is visible,
but only `version` is implemented. Every other command exits 2 and names the day
it lands, so the CLI doubles as a build checklist rather than pretending to work.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from gantry import __version__

app = typer.Typer(
    name="gantry",
    help="Reliability and execution layer for data movement and analysis.",
    no_args_is_help=True,
)

dataset_app = typer.Typer(
    name="dataset",
    help="Register and inspect Datasets.",
    no_args_is_help=True,
)
results_app = typer.Typer(
    name="results",
    help="Read Results, findings and provenance.",
    no_args_is_help=True,
)
app.add_typer(dataset_app)
app.add_typer(results_app)


def _pending(feature: str, day: str) -> None:
    """Exit with a clear signal that a planned command has not been built yet."""
    msg = f"{feature}: not implemented yet (planned {day})."
    typer.secho(msg, fg=typer.colors.YELLOW, err=True)
    raise typer.Exit(code=2)


SpecArg = Annotated[Path, typer.Argument(help="Path to a Movement or Analysis spec.")]
NameArg = Annotated[str, typer.Argument(help="Resource name.")]


@app.command()
def version() -> None:
    """Print the Gantry version."""
    typer.echo(__version__)


@app.command()
def validate(spec: SpecArg) -> None:
    """Validate a spec against its JSON Schema."""
    _pending("validate", "Day 3")


@app.command()
def plan(spec: SpecArg) -> None:
    """Compile a spec into an immutable PlanVersion."""
    _pending("plan", "Day 4")


@app.command()
def start(name: NameArg) -> None:
    """Start an Operation."""
    _pending("start", "Day 10")


@app.command()
def status(name: NameArg) -> None:
    """Show Operation progress and state."""
    _pending("status", "Day 10")


@app.command()
def seed(
    rows: Annotated[int, typer.Option(help="Rows to generate.")] = 1_000_000,
) -> None:
    """Seed the source database with synthetic orders."""
    _pending("seed", "Day 6")


@dataset_app.command("ls")
def dataset_ls() -> None:
    """List registered Datasets."""
    _pending("dataset ls", "Day 2")


@dataset_app.command("describe")
def dataset_describe(name: NameArg) -> None:
    """Show a Dataset manifest without reading its data."""
    _pending("dataset describe", "Day 2")


@dataset_app.command("register")
def dataset_register(spec: SpecArg) -> None:
    """Register a Dataset from a spec file."""
    _pending("dataset register", "Day 2")


@results_app.command("get")
def results_get(name: NameArg) -> None:
    """Fetch a Result."""
    _pending("results get", "Day 18")


@results_app.command("provenance")
def results_provenance(name: NameArg) -> None:
    """Trace a Result back through artifacts, manifests and checkpoints."""
    _pending("results provenance", "Day 18")


if __name__ == "__main__":
    app()
