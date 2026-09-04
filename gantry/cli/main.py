"""Gantry command line interface.

Commands that are not built yet exit 2 and name the day they land, so the CLI
reads as a build checklist rather than pretending to work.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from gantry import __version__
from gantry.core.dataset import DatasetRef, DatasetVersion
from gantry.core.sizes import format_byte_size
from gantry.registry.base import DatasetRegistry
from gantry.registry.errors import RegistryError
from gantry.registry.jsonfile import DEFAULT_REGISTRY_PATH, JsonFileDatasetRegistry
from gantry.spec.apiversion import CANONICAL_API_VERSION, is_deprecated_api_version
from gantry.spec.errors import SpecError
from gantry.spec.loader import dataset_json_schema, load_dataset_spec

app = typer.Typer(
    name="gantry",
    help="Reliability and execution layer for data movement and analysis.",
    no_args_is_help=True,
)

dataset_app = typer.Typer(
    name="dataset", help="Register and inspect Datasets.", no_args_is_help=True
)
results_app = typer.Typer(
    name="results", help="Read Results, findings and provenance.", no_args_is_help=True
)
schema_app = typer.Typer(name="schema", help="Emit JSON Schema for specs.", no_args_is_help=True)
app.add_typer(dataset_app)
app.add_typer(results_app)
app.add_typer(schema_app)

console = Console()
err_console = Console(stderr=True)

REGISTRY_ENV_VAR = "GANTRY_REGISTRY"

SpecArg = Annotated[Path, typer.Argument(help="Path to a spec file.")]
RegistryOpt = Annotated[
    Path | None,
    typer.Option("--registry", help=f"Registry file (env {REGISTRY_ENV_VAR})."),
]


def _pending(feature: str, day: str) -> None:
    """Exit with a clear signal that a planned command has not been built yet."""
    msg = f"{feature}: not implemented yet (planned {day})."
    typer.secho(msg, fg=typer.colors.YELLOW, err=True)
    raise typer.Exit(code=2)


def _fail(message: str) -> None:
    err_console.print(f"[red]error:[/red] {message}")
    raise typer.Exit(code=1)


def _registry(path: Path | None) -> DatasetRegistry:
    resolved = path or Path(os.environ.get(REGISTRY_ENV_VAR, DEFAULT_REGISTRY_PATH))
    return JsonFileDatasetRegistry(resolved)


def _parse_ref(value: str) -> DatasetRef:
    """Parse `name` or `name@version`."""
    if "@" not in value:
        return DatasetRef(name=value)
    name, _, raw_version = value.partition("@")
    if not raw_version.isdigit():
        _fail(f"invalid version in {value!r}: expected an integer after '@'")
    return DatasetRef(name=name, version=int(raw_version))


@app.command()
def version() -> None:
    """Print the Gantry version."""
    typer.echo(__version__)


@app.command()
def validate(spec: SpecArg) -> None:
    """Validate a spec file."""
    try:
        parsed = load_dataset_spec(spec)
    except SpecError as exc:
        _fail(str(exc))
        return

    if is_deprecated_api_version(parsed.api_version):
        err_console.print(
            f"[yellow]warning:[/yellow] apiVersion {parsed.api_version!r} is deprecated; "
            f"use {CANONICAL_API_VERSION!r}"
        )
    console.print(f"[green]ok[/green] {spec}: {parsed.kind} {parsed.metadata.name}")


@app.command()
def plan(spec: SpecArg) -> None:
    """Compile a spec into an immutable PlanVersion."""
    _pending("plan", "Day 4")


@app.command()
def start(name: Annotated[str, typer.Argument(help="Operation name.")]) -> None:
    """Start an Operation."""
    _pending("start", "Day 10")


@app.command()
def status(name: Annotated[str, typer.Argument(help="Operation name.")]) -> None:
    """Show Operation progress and state."""
    _pending("status", "Day 10")


@app.command()
def seed(rows: Annotated[int, typer.Option(help="Rows to generate.")] = 1_000_000) -> None:
    """Seed the source database with synthetic orders."""
    _pending("seed", "Day 6")


@dataset_app.command("register")
def dataset_register(spec: SpecArg, registry: RegistryOpt = None) -> None:
    """Register a Dataset from a spec file.

    Registration is content-addressed: registering an unchanged manifest returns
    the existing version instead of creating a new one.
    """
    try:
        manifest = load_dataset_spec(spec).to_manifest()
    except SpecError as exc:
        _fail(str(exc))
        return

    store = _registry(registry)
    try:
        before = len(store.versions(manifest.name))
    except RegistryError:
        before = 0

    registered = store.register(manifest)
    verb = "unchanged" if registered.version == before else "registered"
    console.print(
        f"[green]{verb}[/green] {registered.name}@{registered.version} "
        f"[dim]{registered.content_hash}[/dim]"
    )


@dataset_app.command("ls")
def dataset_ls(registry: RegistryOpt = None) -> None:
    """List registered Datasets, showing the latest version of each."""
    entries = _registry(registry).list()
    if not entries:
        console.print("[dim]no datasets registered[/dim]")
        return

    table = Table(box=None, pad_edge=False)
    for column in ("NAME", "VERSION", "ADAPTER", "REFERENCE", "ROWS", "SIZE"):
        table.add_column(column, overflow="fold")
    for entry in entries:
        physical = entry.manifest.physical
        table.add_row(
            entry.name,
            str(entry.version),
            physical.adapter,
            physical.reference,
            "-" if physical.estimated_rows is None else f"{physical.estimated_rows:,}",
            "-" if physical.estimated_bytes is None else format_byte_size(physical.estimated_bytes),
        )
    console.print(table)


@dataset_app.command("describe")
def dataset_describe(
    name: Annotated[str, typer.Argument(help="Dataset name, optionally name@version.")],
    registry: RegistryOpt = None,
) -> None:
    """Show a Dataset manifest.

    Reads the manifest only. No connection is made to the underlying system and
    no data is read.
    """
    try:
        entry = _registry(registry).get(_parse_ref(name))
    except RegistryError as exc:
        _fail(str(exc))
        return
    _print_manifest(entry)


@dataset_app.command("versions")
def dataset_versions(
    name: Annotated[str, typer.Argument(help="Dataset name.")],
    registry: RegistryOpt = None,
) -> None:
    """List every version of one Dataset."""
    try:
        history = _registry(registry).versions(name)
    except RegistryError as exc:
        _fail(str(exc))
        return

    table = Table(box=None, pad_edge=False)
    for column in ("VERSION", "REGISTERED", "CONTENT HASH"):
        table.add_column(column, overflow="fold")
    for entry in history:
        table.add_row(
            str(entry.version),
            entry.registered_at.isoformat(timespec="seconds"),
            entry.content_hash,
        )
    console.print(table)


def _print_manifest(entry: DatasetVersion) -> None:
    manifest = entry.manifest
    physical = manifest.physical
    schema = manifest.dataset_schema

    console.print(f"[bold]{entry.name}[/bold]@{entry.version}  [dim]{entry.content_hash}[/dim]")
    console.print(f"  registered   {entry.registered_at.isoformat(timespec='seconds')}")
    console.print(f"  adapter      {physical.adapter}")
    console.print(f"  reference    {physical.reference}")
    if physical.estimated_rows is not None:
        console.print(f"  est. rows    {physical.estimated_rows:,}")
    if physical.estimated_bytes is not None:
        console.print(f"  est. size    {format_byte_size(physical.estimated_bytes)}")
    console.print(f"  keys         {', '.join(schema.keys) if schema.keys else '-'}")
    console.print(f"  time field   {schema.time_field or '-'}")
    console.print(f"  fields       {len(schema.fields) if schema.fields else '- (not discovered)'}")
    console.print(f"  agent access {manifest.access.agent_policy.value}")
    if manifest.sensitive_fields:
        console.print(f"  sensitive    {', '.join(manifest.sensitive_fields)}")


@schema_app.command("dataset")
def schema_dataset(
    out: Annotated[Path | None, typer.Option("--out", help="Write to a file.")] = None,
) -> None:
    """Emit the Dataset JSON Schema, generated from the models."""
    rendered = dataset_json_schema()
    if out is None:
        typer.echo(rendered, nl=False)
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rendered, encoding="utf-8")
    console.print(f"[green]wrote[/green] {out}")


@results_app.command("get")
def results_get(name: Annotated[str, typer.Argument(help="Result name.")]) -> None:
    """Fetch a Result."""
    _pending("results get", "Day 18")


@results_app.command("provenance")
def results_provenance(name: Annotated[str, typer.Argument(help="Result name.")]) -> None:
    """Trace a Result back through artifacts, manifests and checkpoints."""
    _pending("results provenance", "Day 18")


if __name__ == "__main__":
    app()
