# SPDX-License-Identifier: Apache-2.0
"""The same checksum, computed outside a database.

`gantry.verification.checksum` renders the checksum as SQL and lets the engine
compute it, which is right whenever there is an engine. A target like Iceberg is
a pile of Parquet files and a metadata tree; there is nothing to send SQL to.

Verification is the acceptance test, so a target that cannot be verified has no
guarantee at all. Rather than let that stand, this reproduces the identical
algorithm in Python:

    normalise each column to text -> join with US (0x1f) -> md5
    -> take the first 15 hex digits as an integer -> sum over rows

Summing is order-independent, which matters more here than it does in SQL: files
in an Iceberg table have no order worth speaking of. Fifteen hex digits keeps
each term inside a signed 64-bit integer, exactly as the SQL does.

**This file and `checksum.py` must agree exactly.** They are two implementations
of one definition, which is a standing hazard — so they are checked against each
other over awkward values in `tests/integration/test_portable_checksum.py`,
against a real PostgreSQL rather than against my reading of the manual.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal

from gantry.core import DatasetManifest, FieldSchema
from gantry.verification.checksum import NULL_SENTINEL, ChunkChecksum

SEPARATOR = "\x1f"


class UnrepresentableValueError(ValueError):
    """A value this checksum cannot render the same way SQL would.

    Raised rather than guessed at. A checksum that quietly disagrees with the
    other side reports corruption that is not there, and — worse — could agree
    by accident about data that differs.
    """


def normalize_value(field: FieldSchema, value: object) -> str:
    """One value as text, matching `checksum.normalize_column` exactly.

    Every branch here mirrors a branch there. When one changes, both change.
    """
    if value is None:
        return NULL_SENTINEL

    declared = field.type.lower()

    if declared.startswith(("numeric", "decimal")):
        return _trim_scale(value)
    if declared in ("real", "double precision"):
        return _fixed_ten(value)
    if declared.startswith("timestamp with time zone"):
        return _timestamp(value, utc=True)
    if declared.startswith("timestamp"):
        return _timestamp(value, utc=False)
    if declared == "date":
        if not isinstance(value, date):
            raise UnrepresentableValueError(f"{field.name}: expected a date, got {value!r}")
        return value.strftime("%Y-%m-%d")
    if declared == "boolean":
        return "1" if value else "0"
    if declared == "bytea":
        if not isinstance(value, bytes | bytearray | memoryview):
            raise UnrepresentableValueError(f"{field.name}: expected bytes, got {value!r}")
        return bytes(value).hex()
    return str(value)


def row_digest(manifest: DatasetManifest, row: Mapping[str, object]) -> int:
    """One row's contribution to the checksum."""
    rendered = SEPARATOR.join(
        normalize_value(field, row.get(field.name)) for field in manifest.dataset_schema.fields
    )
    digest = hashlib.md5(rendered.encode("utf-8")).hexdigest()
    # Not a security hash. It matches what the SQL side computes, and the SQL
    # side uses md5 because every engine has it.
    return int(digest[:15], 16)


def checksum(manifest: DatasetManifest, rows: Iterable[Mapping[str, object]]) -> ChunkChecksum:
    """The checksum and row count for a set of rows, in the SQL side's terms."""
    total = 0
    count = 0
    for row in rows:
        total += row_digest(manifest, row)
        count += 1
    return ChunkChecksum(checksum=str(total), rows=count)


def _trim_scale(value: object) -> str:
    """PostgreSQL's `trim_scale(x)::text`.

    Trailing zeroes go, because 1.50 and 1.5 are the same number and different
    strings — and a target that stores one is not wrong about the other.
    """
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, int | float | str):
        number = Decimal(str(value))
    else:
        raise UnrepresentableValueError(f"not a number: {value!r}")
    if number == number.to_integral_value():
        # `trim_scale(1.00)` is `1`, not `1.` or `1E+0`.
        return str(number.quantize(Decimal(1)))
    return str(number.normalize())


def _fixed_ten(value: object) -> str:
    """PostgreSQL's `round(x::numeric, 10)::text`.

    Note what this is *not*: it does not trim the scale. `0.5` becomes
    `0.5000000000`, and the ten places stay. Trimming them was my first
    implementation, and it disagreed with the engine on every float in the
    fixture — a checksum quietly wrong on one column type, which is the exact
    failure this pairing exists to catch.

    Scientific notation is likewise ruled out: `1e-7` renders as `0.0000001000`
    and not `1E-7`, because the engine renders a numeric and never an exponent.
    """
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, int | float | str):
        # str() of a float is its shortest round-trip form, which is what
        # casting a double to numeric gives in PostgreSQL too.
        number = Decimal(str(value))
    else:
        raise UnrepresentableValueError(f"not a number: {value!r}")
    if number == 0:
        # -0.0 and 0.0 are the same number, and the engine renders both as zero.
        number = abs(number)
    return format(number.quantize(Decimal("1E-10")), "f")


def _timestamp(value: object, *, utc: bool) -> str:
    if not isinstance(value, datetime):
        raise UnrepresentableValueError(f"expected a timestamp, got {value!r}")
    moment = value
    if utc:
        if moment.tzinfo is None:
            raise UnrepresentableValueError(
                "a timestamptz column produced a value with no timezone; "
                "it cannot be rendered in UTC without inventing one"
            )
        moment = moment.astimezone(UTC)
    # 'YYYY-MM-DD"T"HH24:MI:SS.US' — microseconds always, never truncated.
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")


def field_names(manifest: DatasetManifest) -> Sequence[str]:
    return [field.name for field in manifest.dataset_schema.fields]
