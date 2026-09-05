"""Gantry command line interface.

Commands that are not built yet exit 2 and name the day they land, so the CLI
reads as a build checklist rather than pretending to work.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry import __version__
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.adapters.source.seed import seed as seed_source
from gantry.core.dataset import DatasetRef, DatasetVersion
from gantry.core.sizes import format_byte_size
from gantry.lifecycle.plan import PlanVersion
from gantry.lifecycle.states import IllegalTransitionError, StateTransition
from gantry.movement.result import MovementResult
from gantry.movement.service import MovementService, Progress
from gantry.registry.base import DatasetRegistry
from gantry.registry.discovery import DiscoveryReport, discover_into_registry
from gantry.registry.errors import RegistryError
from gantry.registry.jsonfile import DEFAULT_REGISTRY_PATH, JsonFileDatasetRegistry
from gantry.results.store import PostgresResultStore
from gantry.scheduler.postgres import PostgresWorkflowBackend
from gantry.spec.apiversion import CANONICAL_API_VERSION, is_deprecated_api_version
from gantry.spec.errors import SpecError
from gantry.spec.loader import (
    SUPPORTED_KINDS,
    load_dataset_spec,
    load_movement_spec,
    load_spec,
    spec_json_schema,
)
from gantry.spec.movement import MovementSpec
from gantry.state.checkpoints import PostgresCheckpointStore
from gantry.state.database import DATABASE_URL_ENV, create_engine, database_url
from gantry.state.operations import OperationStore, UnknownOperationError
from gantry.state.plans import PostgresPlanStore
from gantry.state.registry import PostgresDatasetRegistry

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


class ExecutionBackend(StrEnum):
    """Which scheduler executes a Movement.

    Temporal is the default. The leased queue remains available: it is tested,
    needs no extra infrastructure, and is the fallback while Temporal proves
    itself on real runs.
    """

    TEMPORAL = "temporal"
    QUEUE = "queue"


REGISTRY_ENV_VAR = "GANTRY_REGISTRY"
SOURCE_URL_ENV = "GANTRY_SOURCE_URL"
DEFAULT_SOURCE_URL = "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
TARGET_URL_ENV = "GANTRY_TARGET_URL"
DEFAULT_TARGET_URL = "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"


def _source_url() -> str:
    return os.environ.get(SOURCE_URL_ENV, DEFAULT_SOURCE_URL)


def _target_url() -> str:
    return os.environ.get(TARGET_URL_ENV, DEFAULT_TARGET_URL)


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
        parsed = load_spec(spec)
    except SpecError as exc:
        _fail(str(exc))
        return

    if is_deprecated_api_version(parsed.api_version):
        err_console.print(
            f"[yellow]warning:[/yellow] apiVersion {parsed.api_version!r} is deprecated; "
            f"use {CANONICAL_API_VERSION!r}"
        )
    if isinstance(parsed, MovementSpec) and parsed.has_migration_blocks:
        blocks = [
            name
            for name, present in (("cutover", parsed.cutover), ("rollback", parsed.rollback))
            if present is not None
        ]
        err_console.print(
            f"[yellow]warning:[/yellow] {', '.join(blocks)} on a Movement is deprecated; "
            f"these belong to the Migration workflow. A Movement does not imply a cutover."
        )
    console.print(f"[green]ok[/green] {spec}: {parsed.kind} {parsed.metadata.name}")


TargetUrlOpt = Annotated[
    str | None,
    typer.Option("--target-url", help=f"Target database (env {TARGET_URL_ENV})."),
]
MetaUrlOpt = Annotated[
    str | None,
    typer.Option("--meta-url", help=f"Metadata store (env {DATABASE_URL_ENV})."),
]


def _service(
    source_url: str | None, target_url: str | None, meta_url: str | None
) -> tuple[MovementService, tuple[AsyncEngine, ...]]:
    """Wire a service and hand back the engines to dispose."""
    source = create_engine(source_url or _source_url())
    target = create_engine(target_url or _target_url())
    meta = create_engine(meta_url or database_url())

    service = MovementService(
        source_engine=source,
        target_engine=target,
        operations=OperationStore(meta),
        backend_factory=lambda name: PostgresWorkflowBackend(meta, name),
        checkpoints=PostgresCheckpointStore(meta),
        registry=PostgresDatasetRegistry(meta),
        results=PostgresResultStore(meta),
        plans=PostgresPlanStore(meta),
    )
    return service, (source, target, meta)


async def _dispose(engines: tuple[AsyncEngine, ...]) -> None:
    for engine in engines:
        await engine.dispose()


@app.command()
def plan(
    spec: SpecArg,
    source_url: SourceUrlOpt = None,
    target_url: TargetUrlOpt = None,
    meta_url: MetaUrlOpt = None,
) -> None:
    """Discover the source and compile a spec into an immutable PlanVersion."""
    try:
        movement = load_movement_spec(spec).to_movement()
    except SpecError as exc:
        _fail(str(exc))
        return

    service, engines = _service(source_url, target_url, meta_url)

    async def run() -> PlanVersion:
        try:
            return await service.plan(movement)
        finally:
            await _dispose(engines)

    compiled = asyncio.run(run())
    partitions = sum(1 for node in compiled.nodes if node.kind.value == "snapshot_partition")
    console.print(
        f"[green]planned[/green] {compiled.operation} v{compiled.version}  "
        f"[dim]{len(compiled.nodes)} nodes, {partitions} partitions[/dim]"
    )
    console.print(f"  [dim]{compiled.content_hash}[/dim]")


@app.command()
def start(
    spec: SpecArg,
    backend: Annotated[
        ExecutionBackend,
        typer.Option("--backend", help="Who owns dispatch, retries and timeouts."),
    ] = ExecutionBackend.TEMPORAL,
    source_url: SourceUrlOpt = None,
    target_url: TargetUrlOpt = None,
    meta_url: MetaUrlOpt = None,
) -> None:
    """Run a Movement to completion, emitting a Result."""
    try:
        movement = load_movement_spec(spec).to_movement()
    except SpecError as exc:
        _fail(str(exc))
        return

    service, engines = _service(source_url, target_url, meta_url)
    targets = {dataset.name: dataset.target for dataset in movement.datasets}

    async def run() -> MovementResult:
        try:
            compiled = await service.plan(movement)
            if backend is ExecutionBackend.TEMPORAL:
                return await service.run_on_temporal(movement, compiled, targets=targets)
            return await service.run(movement, compiled, targets=targets)
        finally:
            await _dispose(engines)

    result = asyncio.run(run())
    rate = result.rows_per_second
    console.print(
        f"[green]{result.status.value}[/green] {result.name}  "
        f"{result.rows_moved:,} rows in {result.duration.total_seconds():.1f}s"
        + (f" ({rate:,.0f} rows/sec)" if rate else "")
    )
    console.print(
        f"  partitions {result.partitions_complete}/{result.partitions_total}"
        f"   checkpoints {len(result.provenance.checkpoints)}"
        f"   inputs pinned {len(result.provenance.lineage.inputs)}"
    )


@app.command()
def status(
    name: Annotated[str, typer.Argument(help="Operation name.")],
    meta_url: MetaUrlOpt = None,
) -> None:
    """Show Operation state and progress."""
    service, engines = _service(None, None, meta_url)

    async def run() -> tuple[Progress, Sequence[StateTransition]]:
        try:
            return (
                await service.progress(name),
                await OperationStore(create_engine(meta_url or database_url())).history(name),
            )
        finally:
            await _dispose(engines)

    try:
        progress, history = asyncio.run(run())
    except UnknownOperationError as exc:
        _fail(str(exc))
        return

    console.print(f"[bold]{name}[/bold]  state=[cyan]{progress.state.value}[/cyan]")
    console.print(f"  plan version   {progress.plan_version or '-'}")
    console.print(
        f"  partitions     {progress.tasks_done}/{progress.tasks_total} "
        f"({progress.percent_complete:.1f}%)"
    )
    console.print(f"  in flight      {progress.tasks_leased}")
    console.print(f"  quarantined    {progress.tasks_quarantined}")
    console.print(f"  checkpoints    {progress.checkpoints}")
    if history:
        console.print("  [dim]recent transitions[/dim]")
        for step in list(history)[-5:]:
            console.print(
                f"    [dim]{step.occurred_at.isoformat(timespec='seconds')}[/dim] "
                f"{step.from_state.value} -> {step.to_state.value}  [dim]{step.reason}[/dim]"
            )


def _control(name: str, meta_url: str | None, action: str, reason: str) -> None:
    service, engines = _service(None, None, meta_url)

    async def run() -> None:
        try:
            await getattr(service, action)(name, reason=reason)
        finally:
            await _dispose(engines)

    try:
        asyncio.run(run())
    except (UnknownOperationError, IllegalTransitionError) as exc:
        _fail(str(exc))
        return
    console.print(f"[green]{action}d[/green] {name}")


@app.command()
def pause(
    name: Annotated[str, typer.Argument(help="Operation name.")],
    reason: Annotated[str, typer.Option(help="Why, for the audit log.")] = "operator paused",
    meta_url: MetaUrlOpt = None,
) -> None:
    """Stop handing out work. In-flight partitions run to their checkpoint."""
    _control(name, meta_url, "pause", reason)


@app.command()
def resume(
    name: Annotated[str, typer.Argument(help="Operation name.")],
    reason: Annotated[str, typer.Option(help="Why, for the audit log.")] = "operator resumed",
    meta_url: MetaUrlOpt = None,
) -> None:
    """Resume a paused Operation."""
    _control(name, meta_url, "resume", reason)


@app.command()
def abort(
    name: Annotated[str, typer.Argument(help="Operation name.")],
    reason: Annotated[str, typer.Option(help="Why, for the audit log.")] = "operator aborted",
    meta_url: MetaUrlOpt = None,
) -> None:
    """Stop an Operation permanently."""
    _control(name, meta_url, "abort", reason)


SourceUrlOpt = Annotated[
    str | None,
    typer.Option("--source-url", help=f"Source database (env {SOURCE_URL_ENV})."),
]


@app.command()
def seed(
    rows: Annotated[int, typer.Option(help="Orders to generate.")] = 1_000_000,
    source_url: SourceUrlOpt = None,
) -> None:
    """Seed the source database with synthetic orders."""
    engine = create_engine(source_url or _source_url())

    # One event loop for the whole command: a second asyncio.run() would try to
    # dispose connections created in a loop that no longer exists.
    async def run() -> int:
        try:
            return await seed_source(engine, orders=rows)
        finally:
            await engine.dispose()

    console.print(f"[green]seeded[/green] {asyncio.run(run()):,} orders")


@app.command()
def discover(
    source_url: SourceUrlOpt = None,
    registry: RegistryOpt = None,
    schema: Annotated[list[str] | None, typer.Option("--schema", help="Schemas to scan.")] = None,
    profile: Annotated[bool, typer.Option(help="Also profile discovered datasets.")] = True,
) -> None:
    """Discover a source and register what it holds as Datasets."""
    engine = create_engine(source_url or _source_url())
    adapter = PostgresSourceAdapter(engine)
    store = _registry(registry)

    async def run() -> DiscoveryReport:
        try:
            return await discover_into_registry(
                adapter, store, schemas=tuple(schema or ["public"]), profile=profile
            )
        finally:
            await engine.dispose()

    report = asyncio.run(run())
    for version in report.registered:
        manifest = version.manifest
        rows = manifest.physical.estimated_rows
        console.print(
            f"[green]registered[/green] {version.name}@{version.version}  "
            f"[dim]{len(manifest.dataset_schema.fields)} fields, "
            f"key={'/'.join(manifest.dataset_schema.keys) or '-'}, "
            f"~{rows:,} rows[/dim]"
            if rows is not None
            else f"[green]registered[/green] {version.name}@{version.version}"
        )
    if report.profiled:
        console.print(f"[dim]profiled {len(report.registered)} datasets[/dim]")


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
        before = len(asyncio.run(store.versions(manifest.name)))
    except RegistryError:
        before = 0

    registered = asyncio.run(store.register(manifest))
    verb = "unchanged" if registered.version == before else "registered"
    console.print(
        f"[green]{verb}[/green] {registered.name}@{registered.version} "
        f"[dim]{registered.content_hash}[/dim]"
    )


@dataset_app.command("ls")
def dataset_ls(registry: RegistryOpt = None) -> None:
    """List registered Datasets, showing the latest version of each."""
    entries = asyncio.run(_registry(registry).list())
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
        entry = asyncio.run(_registry(registry).get(_parse_ref(name)))
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
        history = asyncio.run(_registry(registry).versions(name))
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


@schema_app.command("show")
def schema_show(
    kind: Annotated[str, typer.Argument(help=f"One of: {', '.join(SUPPORTED_KINDS)}.")],
    out: Annotated[Path | None, typer.Option("--out", help="Write to a file.")] = None,
) -> None:
    """Emit a spec JSON Schema, generated from the models."""
    try:
        rendered = spec_json_schema(kind)
    except SpecError as exc:
        _fail(str(exc))
        return
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
