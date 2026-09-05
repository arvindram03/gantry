# SPDX-License-Identifier: Apache-2.0
"""Masking what a decision said to mask.

A `REDACT` decision is a promise about the bytes that come back, and a promise
nothing enforces is a comment. These functions are the enforcement, and every
path that returns Dataset content runs its output through them.

Masking replaces a value with a marker rather than dropping the field. A
missing column looks like a Dataset that does not have one; a masked column
says plainly that something is there and policy withheld it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TypeVar

from gantry.core.dataset import DatasetManifest, DatasetStatistics
from gantry.policy.gate import AccessDecision

REDACTED = "[redacted]"

T = TypeVar("T")


def redact_columns(
    rows: Iterable[Mapping[str, object]], columns: Iterable[str]
) -> list[dict[str, object]]:
    """Mask these output columns in every row.

    Takes column names rather than field names because the two differ wherever
    a projection is aliased. Masking by source field against an aliased output
    matches nothing while the decision still reads REDACT - a mask that is not
    applied is worse than one that was never promised.
    """
    masked = set(columns)
    if not masked:
        return [dict(row) for row in rows]
    return [
        {key: (REDACTED if key in masked else value) for key, value in row.items()} for row in rows
    ]


def redact_rows(
    rows: Iterable[Mapping[str, object]], decision: AccessDecision
) -> list[dict[str, object]]:
    """Mask the fields a decision named, where rows carry them under those names."""
    return redact_columns(rows, decision.redacted_fields)


def redact_manifest(manifest: DatasetManifest, decision: AccessDecision) -> DatasetManifest:
    """Mask sampled values a manifest carries.

    Field names and types survive - describing a Dataset is the top rung and
    is what makes the ladder usable at all. What is masked is the content that
    profiling sampled out of the data itself.
    """
    masked = set(decision.redacted_fields)
    if not masked:
        return manifest
    return manifest.model_copy(
        update={
            "statistics": _redact_statistics(
                manifest.statistics, masked, manifest.dataset_schema.keys
            )
        }
    )


def _redact_statistics(
    statistics: DatasetStatistics, masked: set[str], keys: Sequence[str]
) -> DatasetStatistics:
    """Drop the sampled values a profile carries for masked fields.

    Two things in here are real data rather than description. Histogram
    boundaries are values taken out of the column - enough of them, and the
    distribution of a masked field is readable straight off the profile. The
    key range is the same thing for the key, and is only sensitive when the key
    itself is.

    Null rates and counts survive: they describe the column without quoting it,
    which is exactly what the profile rung is for.
    """
    updates: dict[str, object] = {
        "histograms": {
            field: values for field, values in statistics.histograms.items() if field not in masked
        }
    }
    if masked & set(keys):
        updates["key_min"] = None
        updates["key_max"] = None
    return statistics.model_copy(update=updates)


def redact_boundaries(
    boundaries: Sequence[Mapping[str, object]], decision: AccessDecision
) -> list[dict[str, object]]:
    """Mask key values at partition boundaries.

    A boundary is a real key value out of the data. Listing them is how an
    agent reasons about splitting work, and it is also a way to read a sensitive
    key one boundary at a time.
    """
    return redact_rows(boundaries, decision)
