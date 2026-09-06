# SPDX-License-Identifier: Apache-2.0
"""What an Iceberg target can and cannot hold.

The point of this module is the refusing. A type that Iceberg cannot represent
must fail while the Movement is being prepared, with the column named — not at
row forty million, halfway through a job, with a stack trace from a Java writer.

Iceberg's type system is deliberately small, and the gaps against PostgreSQL are
real rather than theoretical: there is no `json`, no `interval`, no enum, and
decimals stop at 38 digits of precision. Guessing a mapping for those would mean
choosing, on the user's behalf and silently, to store something other than what
they have.
"""

from __future__ import annotations

import re

from gantry.core import DatasetManifest, FieldSchema

_DECIMAL = re.compile(r"^(?:numeric|decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_UNSIZED_DECIMAL = re.compile(r"^(?:numeric|decimal)$")

# Iceberg's decimals are fixed at 38 digits, which is also PostgreSQL's limit
# for a *declared* precision — but an undeclared `numeric` in PostgreSQL is
# unbounded, and that is the case worth refusing rather than truncating.
MAX_DECIMAL_PRECISION = 38

_DIRECT = {
    "boolean": "boolean",
    "smallint": "int",
    "integer": "int",
    "int": "int",
    "int2": "int",
    "int4": "int",
    "bigint": "long",
    "int8": "long",
    "real": "float",
    "float4": "float",
    "double precision": "double",
    "float8": "double",
    "text": "string",
    "date": "date",
    "bytea": "binary",
    "uuid": "uuid",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
}

# Named so the error can say *why*, which is the difference between a message
# an operator can act on and one they have to research.
_REFUSED = {
    "json": "Iceberg has no JSON type; store it as text and say so, or leave it behind",
    "jsonb": "Iceberg has no JSON type; store it as text and say so, or leave it behind",
    "interval": "Iceberg has no interval type; a duration must be stored as a number of units",
    "money": "`money` is locale-dependent even within PostgreSQL; use numeric",
    "tsvector": "a full-text index is derived data, not data to move",
    "xml": "Iceberg has no XML type; store it as text and say so",
}


class UnsupportedTypeError(Exception):
    """A column this target cannot hold, named while there is still time.

    Raised in Prepare rather than during a write. The whole reason to know the
    schema before moving anything is to fail here.
    """


def iceberg_type(field: FieldSchema) -> str:
    """The Iceberg type for one column, or an error naming the column."""
    declared = field.type.strip().lower()

    if declared in _DIRECT:
        return _DIRECT[declared]
    if declared in _REFUSED:
        raise UnsupportedTypeError(f"{field.name} ({field.type}): {_REFUSED[declared]}")
    if declared.startswith("character varying") or declared.startswith("varchar"):
        # Iceberg strings are unbounded; a length limit is a source-side
        # constraint and does not travel.
        return "string"
    if declared in ("character", "char", "bpchar"):
        return "string"
    if declared.startswith("timestamp with time zone"):
        return "timestamptz"
    if declared.startswith("timestamp"):
        return "timestamp"
    if _UNSIZED_DECIMAL.match(declared):
        raise UnsupportedTypeError(
            f"{field.name} ({field.type}): an unconstrained numeric has no bound Iceberg can "
            f"hold. Declare a precision and scale — numeric(p,s) with p <= {MAX_DECIMAL_PRECISION}"
        )
    found = _DECIMAL.match(declared)
    if found:
        precision, scale = int(found.group(1)), int(found.group(2))
        if precision > MAX_DECIMAL_PRECISION:
            raise UnsupportedTypeError(
                f"{field.name} ({field.type}): Iceberg decimals stop at "
                f"{MAX_DECIMAL_PRECISION} digits of precision"
            )
        return f"decimal({precision}, {scale})"
    if declared.endswith("[]"):
        raise UnsupportedTypeError(
            f"{field.name} ({field.type}): arrays are representable in Iceberg but not yet "
            f"mapped here, and guessing the element type is how the wrong thing gets stored"
        )

    raise UnsupportedTypeError(
        f"{field.name} ({field.type}): no Iceberg type is mapped for this. Refused rather "
        f"than guessed, because a guess would silently store something else"
    )


def iceberg_schema(manifest: DatasetManifest) -> dict[str, str]:
    """Every column mapped, or every problem reported at once.

    All of them, not the first: an operator fixing a schema wants the list, not
    one round trip per column.
    """
    mapped: dict[str, str] = {}
    problems: list[str] = []
    for field in manifest.dataset_schema.fields:
        try:
            mapped[field.name] = iceberg_type(field)
        except UnsupportedTypeError as refused:
            problems.append(str(refused))
    if problems:
        raise UnsupportedTypeError(
            f"{manifest.name} cannot be held by an Iceberg target:\n  - " + "\n  - ".join(problems)
        )
    return mapped
