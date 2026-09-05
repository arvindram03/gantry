"""Chunk checksums.

Comparing two large tables row by row is the obvious approach and the one that
costs more than the migration. A checksum over a key range answers "is this
range identical" in a single pass on each side, and the drill-down turns a
disagreement into a location.

The whole thing lives or dies on normalisation. Two databases holding the same
data will happily render it as different text - `1.50` against `1.5`, a
timestamp in whatever the session's timezone happens to be, a NULL that
silently poisons a concatenation. Every one of those produces a checksum
mismatch on data that is perfectly correct, and a verification layer that cries
wolf is worse than none. The rules below are the contract; they are documented
in `docs/guarantees.md` because they are the part most likely to need extending
when a new type shows up.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest
from gantry.core.schema import FieldSchema
from gantry.state.database import transaction
from gantry.verification.sql import qualified, quote

# A NULL rendered as nothing is indistinguishable from an empty string, and a
# NULL inside a concatenation makes the whole row NULL. Both are silent.
NULL_SENTINEL = r"\N"


def normalize_column(field: FieldSchema) -> str:
    """SQL rendering one column into text that compares equal across databases.

    The cases here are the ones that actually bite:

    - `numeric` carries its scale, so 1.50 and 1.5 are the same number and
      different strings. `trim_scale` removes trailing zeroes.
    - `timestamptz` renders in the session's TimeZone. Forcing UTC and an
      explicit format makes the text independent of who is asking.
    - floating point is rounded before rendering, because the last bits of a
      double are not meaningful data.
    - `bytea` hex-encodes rather than relying on `bytea_output`.
    - booleans become 0/1 rather than t/f, which varies by client.
    """
    column = quote(field.name)
    declared = field.type.lower()

    if declared.startswith("numeric") or declared.startswith("decimal"):
        rendered = f"trim_scale({column})::text"
    elif declared in ("real", "double precision"):
        rendered = f"round({column}::numeric, 10)::text"
    elif declared.startswith("timestamp with time zone"):
        rendered = f"to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US')"
    elif declared.startswith("timestamp"):
        rendered = f"to_char({column}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')"
    elif declared == "date":
        rendered = f"to_char({column}, 'YYYY-MM-DD')"
    elif declared == "boolean":
        rendered = f"({column})::int::text"
    elif declared == "bytea":
        rendered = f"encode({column}, 'hex')"
    else:
        rendered = f"{column}::text"

    return f"coalesce({rendered}, '{NULL_SENTINEL}')"


def row_expression(manifest: DatasetManifest) -> str:
    """One row rendered as a single normalised string."""
    fields = manifest.dataset_schema.fields
    if not fields:
        raise ValueError(
            f"dataset {manifest.name!r} has no discovered fields; "
            f"a checksum needs to know what it is summing"
        )
    parts = " || '\x1f' || ".join(normalize_column(field) for field in fields)
    return parts


def checksum_expression(manifest: DatasetManifest) -> str:
    """An order-independent checksum over the rows in scope.

    Summing per-row hashes rather than hashing a concatenation means no sort,
    no unbounded string, and no dependence on the order rows come back in.

    Sum rather than XOR deliberately: XOR cancels duplicates, so a row written
    twice would be invisible to exactly the check meant to catch it.
    """
    row = row_expression(manifest)
    # 15 hex digits keeps each term inside a signed bigint; sum() over bigint
    # returns numeric, so the total cannot overflow.
    return f"coalesce(sum(('x' || substr(md5({row}), 1, 15))::bit(60)::bigint), 0)::text"


@dataclass(frozen=True)
class ChunkChecksum:
    """What one side reported for a key range."""

    checksum: str
    rows: int

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ChunkChecksum):
            return NotImplemented
        return self.checksum == other.checksum and self.rows == other.rows

    def __hash__(self) -> int:
        return hash((self.checksum, self.rows))

    def describe(self) -> str:
        return f"{self.checksum} over {self.rows:,} rows"


async def compute_checksum(
    engine: AsyncEngine,
    manifest: DatasetManifest,
    table: str,
    *,
    predicate: str,
    params: dict[str, str],
) -> ChunkChecksum:
    """Compute a chunk checksum inside the database.

    The row count travels with it: a checksum alone cannot distinguish an empty
    range from one whose hashes happen to cancel.
    """
    query = text(
        f"SELECT {checksum_expression(manifest)} AS checksum, count(*) AS rows "
        f"FROM {qualified(table)} WHERE {predicate}"
    )
    async with transaction(engine) as connection:
        row = (await connection.execute(query, params)).one()
    return ChunkChecksum(checksum=str(row.checksum), rows=int(row.rows))
