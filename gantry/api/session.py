"""The entry point: `gantry.connect()`.

Holds the engines and the stores so the three namespaces - datasets, analysis,
results - share one metadata connection and one policy. The alternative is
every caller wiring six objects together and getting the access rules subtly
different in each place, which is how a policy stops being a policy.
"""

from __future__ import annotations

from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.adapters.engine.postgres import PostgresEngineAdapter
from gantry.analysis.service import AnalysisService
from gantry.api.analysis import AnalysisApi
from gantry.api.datasets import Datasets
from gantry.api.results import ResultsApi
from gantry.core.dataset import DatasetManifest
from gantry.policy.audit import AccessLog, access_log_for
from gantry.policy.gate import AccessGate
from gantry.policy.rules import AccessRules
from gantry.results.store import PostgresResultStore
from gantry.state.artifacts import PostgresArtifactStore
from gantry.state.database import create_engine, database_url
from gantry.state.operations import OperationStore
from gantry.state.registry import PostgresDatasetRegistry


class Gantry:
    """A connected session.

    One policy object is shared by everything reachable from here. A second
    gate configured differently would be a second policy, and the one an agent
    happened to reach would decide what it could read.
    """

    def __init__(
        self,
        *,
        source: AsyncEngine,
        meta: AsyncEngine,
        rules: AccessRules | None = None,
        principal: str = "agent",
        owns_engines: bool = False,
    ) -> None:
        self._source = source
        self._meta = meta
        self._owns_engines = owns_engines
        self._rules = rules or AccessRules()
        self._gate = AccessGate(self._rules)
        self._audit: AccessLog = access_log_for(
            meta, persist=self._rules.evidence.persist, principal=principal
        )

        self._registry = PostgresDatasetRegistry(meta)
        self._results = PostgresResultStore(meta)
        self._artifacts = PostgresArtifactStore(meta)
        self._adapter = PostgresEngineAdapter(source)

        self.datasets = Datasets(
            registry=self._registry, engine=source, gate=self._gate, audit=self._audit
        )
        self.results = ResultsApi(
            results=self._results,
            registry=self._registry,
            artifacts=self._artifacts,
            adapter=self._adapter,
        )

    @property
    def rules(self) -> AccessRules:
        return self._rules

    @property
    def gate(self) -> AccessGate:
        return self._gate

    @property
    def audit(self) -> AccessLog:
        return self._audit

    def analysis(self, manifests: dict[str, DatasetManifest]) -> AnalysisApi:
        """The Analysis namespace, bound to the manifests it will compile against.

        Manifests are passed in rather than resolved here because an Analysis
        must run against the Dataset versions it was planned against - reading
        whatever is current at execution time is the version skew the registry
        exists to prevent.
        """
        return AnalysisApi(
            service=AnalysisService(adapter=self._adapter, manifests=manifests),
            operations=OperationStore(self._meta),
        )

    async def close(self) -> None:
        if self._owns_engines:
            await self._source.dispose()
            await self._meta.dispose()

    async def __aenter__(self) -> Gantry:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()


def connect(
    *,
    source_url: str,
    meta_url: str | None = None,
    rules: AccessRules | None = None,
    principal: str = "agent",
) -> Gantry:
    """Open a session against a source database and the metadata store."""
    return Gantry(
        source=create_engine(source_url),
        meta=create_engine(meta_url or database_url()),
        rules=rules,
        principal=principal,
        owns_engines=True,
    )
