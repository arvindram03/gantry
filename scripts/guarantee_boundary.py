"""The engine-success / Gantry-failure case, as a runnable demo.

The clearest single expression of what Gantry is for. The same Analysis is run
twice against the same data on the same engine. The only difference is whether
the join carries its temporal qualifier, and PostgreSQL executes both without
complaint — it did exactly what it was asked. Whether the numbers mean anything
is a different question, and this is where it is answered.

    uv run python scripts/guarantee_boundary.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from gantry.adapters.engine.postgres import PostgresEngineAdapter
from gantry.adapters.source.postgres import PostgresSourceAdapter
from gantry.analysis.model import Analysis
from gantry.analysis.service import AnalysisRun, AnalysisService, pins_for
from gantry.core.dataset import DatasetManifest
from gantry.core.evidence import VerificationStatus
from gantry.core.operation import OperationType
from gantry.core.results import ResultStatus
from gantry.results.store import PostgresResultStore
from gantry.spec import load_analysis_spec, load_dataset_spec
from gantry.state.artifacts import PostgresArtifactStore
from gantry.state.database import create_engine
from gantry.state.operations import OperationStore
from gantry.state.registry import PostgresDatasetRegistry
from sqlalchemy.ext.asyncio import AsyncEngine

ROOT = Path(__file__).resolve().parents[1]
ANALYSIS = ROOT / "spec/examples/analysis-checkout-regression.yaml"
DATASETS = (
    ROOT / "spec/examples/dataset-request-logs.yaml",
    ROOT / "spec/examples/dataset-deploy-events.yaml",
)

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)

BOLD, GREEN, RED, DIM, OFF = "\033[1m", "\033[32m", "\033[31m", "\033[2m", "\033[0m"


async def manifests(source: AsyncEngine) -> dict[str, DatasetManifest]:
    """Catalog facts, with the time semantics the Dataset specs declare.

    Discovery cannot know which column orders the data; a temporal join has to
    be told. That declaration lives in the Dataset spec and is merged onto what
    the catalog reports.
    """
    adapter = PostgresSourceAdapter(source)
    discovered = {m.name: await adapter.profile(m) for m in await adapter.discover()}

    resolved: dict[str, DatasetManifest] = {}
    for path in DATASETS:
        declared = load_dataset_spec(path).to_manifest()
        catalog = discovered[declared.name]
        resolved[declared.name] = catalog.model_copy(
            update={
                "dataset_schema": catalog.dataset_schema.model_copy(
                    update={"time_field": declared.dataset_schema.time_field}
                )
            }
        )
    return resolved


def without_temporal_join(analysis: Analysis) -> Analysis:
    """The same Analysis, with the temporal qualifier removed.

    Every request then matches every deploy for its service instead of the one
    that was live when it happened, so the join multiplies rows. The engine is
    perfectly happy with it. That is the point.
    """
    return analysis.model_copy(
        update={
            "name": "checkout-regression-expanding",
            "joins": tuple(join.model_copy(update={"temporal": None}) for join in analysis.joins),
        }
    )


def report(label: str, run: AnalysisRun) -> None:
    result = run.result
    assert result is not None

    engine = f"SUCCESS ({result.rows_returned} rows returned)"
    print(f"\n{BOLD}{label}{OFF}")
    print(f"  ENGINE  {GREEN}{engine}{OFF}")

    for check in result.verification:
        passed = check.status is VerificationStatus.PASSED
        colour = GREEN if passed else RED
        # The two sides a check compared. An Analysis verifier records what
        # the spec declared as the `source_result` and what it measured as the
        # `target_result` - the same shape a Movement check uses for source
        # against target.
        allowed = check.source_result or "n/a"
        measured = check.target_result or ""
        print(
            f"  GANTRY  {colour}{check.status.value.upper()}{OFF}  "
            f"{check.check.value} {measured} {DIM}(allowed {allowed}){OFF}"
        )
        if check.difference:
            print(f"          {DIM}{check.difference}{OFF}")

    if result.status is ResultStatus.OK:
        print(f"  RESULT  {GREEN}{len(result.findings)} findings published{OFF}")
    else:
        print(f"  RESULT  {RED}{result.status.value}, findings withheld{OFF}")


async def main() -> int:
    source = create_engine(SOURCE_URL)
    meta = create_engine(META_URL)
    try:
        resolved = await manifests(source)
        # Registered durably, not in memory: a Result pins the Dataset versions
        # it read, and a pin into a registry that died with the process is a
        # provenance chain that cannot be walked afterwards.
        registry = PostgresDatasetRegistry(meta)
        versions = [await registry.register(m) for m in resolved.values()]

        service = AnalysisService(
            adapter=PostgresEngineAdapter(source),
            manifests=resolved,
            pins=pins_for(versions),
        )
        analysis = load_analysis_spec(ANALYSIS).to_analysis()

        operations = OperationStore(meta)
        results = PostgresResultStore(meta)
        artifacts = PostgresArtifactStore(meta)

        well_formed = await service.run(analysis)
        report("well-formed  (temporal join: nearest_preceding)", well_formed)
        assert well_formed.result is not None

        await operations.ensure(analysis.name, OperationType.ANALYSIS)
        await results.put(well_formed.result)
        await artifacts.put(well_formed.artifact)

        expanding_spec = without_temporal_join(analysis)
        expanding = await service.run(expanding_spec)
        report("expanding    (same SQL, no temporal qualifier)", expanding)

        assert expanding.result is not None
        if expanding.result.status is ResultStatus.OK:
            print(f"\n{RED}the expanding join was not rejected{OFF}", file=sys.stderr)
            return 1

        print(
            f"\n{DIM}Both ran. One is trustworthy. The engine could not tell them "
            f"apart.{OFF}\n{DIM}Result stored: {well_formed.result.name}{OFF}"
        )
        return 0
    finally:
        await source.dispose()
        await meta.dispose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
