# SPDX-License-Identifier: Apache-2.0
"""SQL helpers shared by the verifiers."""

from __future__ import annotations

import re

from gantry.core.dataset import DatasetManifest
from gantry.movement.partitioning import Partition

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_SAFE_TYPE = re.compile(r"^[a-z][a-z0-9 _]*(\(\d+(,\s*\d+)?\))?(\[\])?$")


def quote(identifier: str) -> str:
    if not _SAFE_IDENT.match(identifier):
        raise ValueError(f"unsafe identifier: {identifier!r}")
    return f'"{identifier}"'


def qualified(reference: str) -> str:
    return ".".join(quote(part) for part in reference.split("."))


def column_type(manifest: DatasetManifest, column: str) -> str:
    field = manifest.dataset_schema.field(column)
    if field is None:
        raise ValueError(f"dataset {manifest.name!r} has no field {column!r}")
    if not _SAFE_TYPE.match(field.type):
        raise ValueError(f"unsupported column type: {field.type!r}")
    return field.type


def partition_predicate(
    manifest: DatasetManifest, partition: Partition | None
) -> tuple[str, dict[str, str]]:
    """A WHERE clause restricting a query to one partition.

    Bounds are the plan's, re-typed from the schema, and bound as text before
    casting - the driver infers a parameter's type from its cast target, so
    binding a string against a bigint column fails without the extra step.
    """
    if partition is None:
        return "TRUE", {}

    column = quote(partition.column)
    declared = column_type(manifest, partition.column)
    clauses: list[str] = []
    params: dict[str, str] = {}

    if partition.lo is not None:
        params["lo"] = partition.lo
        clauses.append(f"{column} >= CAST(CAST(:lo AS text) AS {declared})")
    if partition.hi is not None:
        params["hi"] = partition.hi
        clauses.append(f"{column} < CAST(CAST(:hi AS text) AS {declared})")

    return (" AND ".join(clauses) if clauses else "TRUE"), params
