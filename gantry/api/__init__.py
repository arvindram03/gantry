"""The typed Python API.

    async with gantry.connect(source_url=...) as session:
        await session.datasets.describe("orders")
        await session.datasets.query("orders", AggregateQuery(...))
        await session.results.provenance("checkout-regression.analysis")

Everything that can return Dataset content goes through the access gate. That
is not a convention this package follows; it is the only path the functions
have to the data.
"""

from __future__ import annotations

from gantry.api.aggregates import Aggregate, AggregateFunction, AggregateQuery
from gantry.api.analysis import AnalysisApi, AnalysisPlan
from gantry.api.datasets import Datasets, QueryOutcome
from gantry.api.results import ResultsApi
from gantry.api.session import Gantry, connect

__all__ = [
    "Aggregate",
    "AggregateFunction",
    "AggregateQuery",
    "AnalysisApi",
    "AnalysisPlan",
    "Datasets",
    "Gantry",
    "QueryOutcome",
    "ResultsApi",
    "connect",
]
