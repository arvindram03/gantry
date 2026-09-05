"""PostgreSQL source adapter.

Discovery and profiling both read the catalog and the planner's own statistics.
Nothing here scans a table: `pg_class.reltuples`, `pg_stats.null_frac` and
`pg_stats.n_distinct` are already maintained from a sample, and min/max come
from an index scan. Profiling a 100M-row table is therefore a metadata
operation, not a read of 100M rows.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest, DatasetStatistics, PhysicalRef
from gantry.core.positions import PositionKind, SourcePosition
from gantry.core.schema import DatasetSchema, FieldSchema, ForeignKey, Index
from gantry.movement.partitioning import Partition
from gantry.state.database import transaction

ADAPTER = "postgres"

_TABLES = text(
    """
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           GREATEST(c.reltuples, 0)::bigint AS row_estimate,
           pg_total_relation_size(c.oid) AS byte_estimate
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND n.nspname = ANY(:schemas)
     ORDER BY n.nspname, c.relname
    """
)

_COLUMNS = text(
    """
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           a.attname AS column_name,
           format_type(a.atttypid, a.atttypmod) AS column_type,
           NOT a.attnotnull AS nullable
      FROM pg_attribute a
      JOIN pg_class c ON c.oid = a.attrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND n.nspname = ANY(:schemas)
       AND a.attnum > 0
       AND NOT a.attisdropped
     ORDER BY n.nspname, c.relname, a.attnum
    """
)

_INDEXES = text(
    """
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           i.relname AS index_name,
           ix.indisunique AS is_unique,
           ix.indisprimary AS is_primary,
           array_agg(a.attname ORDER BY k.ord) AS columns
      FROM pg_index ix
      JOIN pg_class c ON c.oid = ix.indrelid
      JOIN pg_class i ON i.oid = ix.indexrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
     WHERE n.nspname = ANY(:schemas)
     GROUP BY 1, 2, 3, 4, 5
     ORDER BY 1, 2, 3
    """
)

_FOREIGN_KEYS = text(
    """
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           array_agg(att.attname ORDER BY k.ord) AS columns,
           fn.nspname || '.' || fc.relname AS referenced_table,
           array_agg(fatt.attname ORDER BY k.ord) AS referenced_columns
      FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_class fc ON fc.oid = con.confrelid
      JOIN pg_namespace fn ON fn.oid = fc.relnamespace
      JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS k(att, fatt, ord) ON TRUE
      JOIN pg_attribute att ON att.attrelid = c.oid AND att.attnum = k.att
      JOIN pg_attribute fatt ON fatt.attrelid = fc.oid AND fatt.attnum = k.fatt
     WHERE con.contype = 'f'
       AND n.nspname = ANY(:schemas)
     GROUP BY 1, 2, con.conname, 4
     ORDER BY 1, 2
    """
)

_STATS = text(
    """
    SELECT attname AS column_name,
           null_frac,
           n_distinct,
           -- histogram_bounds is anyarray; the double cast is the only way to
           -- read it generically. CAST(...) rather than `::` because text()
           -- reads a colon as bind-parameter syntax.
           CAST(CAST(histogram_bounds AS text) AS text[]) AS histogram_bounds
      FROM pg_stats
     WHERE schemaname = :schema_name
       AND tablename = :table_name
    """
)


@dataclass
class _Table:
    schema_name: str
    table_name: str
    row_estimate: int
    byte_estimate: int
    columns: list[FieldSchema] = field(default_factory=list)
    indexes: list[Index] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)

    @property
    def qualified(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    @property
    def primary_key(self) -> tuple[str, ...]:
        return next((idx.columns for idx in self.indexes if idx.primary), ())


class PostgresSourceAdapter:
    """Reads catalog and planner statistics from a PostgreSQL source."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def discover(self, *, schemas: Sequence[str] = ("public",)) -> Sequence[DatasetManifest]:
        wanted = list(schemas)
        async with transaction(self._engine) as connection:
            tables = {
                (row.schema_name, row.table_name): _Table(
                    schema_name=row.schema_name,
                    table_name=row.table_name,
                    row_estimate=row.row_estimate,
                    byte_estimate=row.byte_estimate,
                )
                for row in (await connection.execute(_TABLES, {"schemas": wanted})).all()
            }

            for row in (await connection.execute(_COLUMNS, {"schemas": wanted})).all():
                table = tables.get((row.schema_name, row.table_name))
                if table is not None:
                    table.columns.append(
                        FieldSchema(
                            name=row.column_name, type=row.column_type, nullable=row.nullable
                        )
                    )

            for row in (await connection.execute(_INDEXES, {"schemas": wanted})).all():
                table = tables.get((row.schema_name, row.table_name))
                if table is not None:
                    table.indexes.append(
                        Index(
                            name=row.index_name,
                            columns=tuple(row.columns),
                            unique=row.is_unique,
                            primary=row.is_primary,
                        )
                    )

            for row in (await connection.execute(_FOREIGN_KEYS, {"schemas": wanted})).all():
                table = tables.get((row.schema_name, row.table_name))
                if table is not None:
                    table.foreign_keys.append(
                        ForeignKey(
                            columns=tuple(row.columns),
                            references=row.referenced_table,
                            referenced_columns=tuple(row.referenced_columns),
                        )
                    )

        return tuple(_to_manifest(table) for _, table in sorted(tables.items()))

    async def profile(self, manifest: DatasetManifest) -> DatasetManifest:
        schema_name, _, table_name = manifest.physical.reference.partition(".")
        keys = manifest.dataset_schema.keys

        async with transaction(self._engine) as connection:
            stats = (
                await connection.execute(
                    _STATS, {"schema_name": schema_name, "table_name": table_name}
                )
            ).all()

            key_min, key_max = await self._key_range(connection, manifest, keys)

        rows = manifest.physical.estimated_rows or 0
        null_rates = {row.column_name: float(row.null_frac) for row in stats}
        distinct = _distinct_keys(stats, keys, rows)
        histograms = _histograms(stats)

        return manifest.model_copy(
            update={
                "statistics": DatasetStatistics(
                    row_count=manifest.physical.estimated_rows,
                    profiled_at=datetime.now(UTC),
                    key_min=key_min,
                    key_max=key_max,
                    distinct_keys=distinct,
                    null_rates=null_rates,
                    histograms=histograms,
                    # No planner statistics means "unknown", which is a
                    # different thing from "uniform" - partitioning must not
                    # read the absence of skew as evidence of its absence.
                    stale_statistics=not stats,
                )
            }
        )

    async def read_partition(
        self, manifest: DatasetManifest, partition: Partition, *, batch_size: int = 10_000
    ) -> AsyncIterator[Sequence[tuple[object, ...]]]:
        """Stream one partition through a server-side cursor."""
        preparer = self._engine.dialect.identifier_preparer
        table = ".".join(preparer.quote(part) for part in manifest.physical.reference.split("."))
        column = preparer.quote(partition.column)

        # Partition bounds are type-agnostic strings by design, so the adapter
        # re-types them from the schema it discovered. This is why the manifest
        # travels with the partition rather than the bounds carrying a type.
        # The parameter is pinned to text first: asyncpg infers a bind's type
        # from its cast target, so CAST(:lo AS bigint) would demand an int.
        column_type = _column_type(manifest, partition.column)

        clauses: list[str] = []
        params: dict[str, str] = {}
        if partition.lo is not None:
            clauses.append(f"{column} >= CAST(CAST(:lo AS text) AS {column_type})")
            params["lo"] = partition.lo
        if partition.hi is not None:
            clauses.append(f"{column} < CAST(CAST(:hi AS text) AS {column_type})")
            params["hi"] = partition.hi
        where = " AND ".join(clauses) if clauses else "TRUE"

        # Ordered by key so a partition read twice yields rows in one order,
        # which is what makes a checksum over it reproducible.
        query = text(f"SELECT * FROM {table} WHERE {where} ORDER BY {column}")

        async with self._engine.connect() as connection:
            await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            result = await connection.stream(query, params)
            async for batch in result.partitions(batch_size):
                yield tuple(tuple(row) for row in batch)

    def copy_query(
        self, manifest: DatasetManifest, partition: Partition
    ) -> tuple[str, tuple[str, ...]]:
        """A COPY-able SELECT for one partition, with positional parameters.

        COPY does not go through SQLAlchemy, so this uses the driver's own $N
        placeholders. Columns are named explicitly and in manifest order: the
        binary COPY format carries no column names, so both sides must agree on
        the order or the bytes land in the wrong columns.
        """
        preparer = self._engine.dialect.identifier_preparer
        table = ".".join(preparer.quote(part) for part in manifest.physical.reference.split("."))
        column = preparer.quote(partition.column)
        column_type = _column_type(manifest, partition.column)
        columns = ", ".join(preparer.quote(field.name) for field in manifest.dataset_schema.fields)

        clauses: list[str] = []
        params: list[str] = []
        if partition.lo is not None:
            params.append(partition.lo)
            clauses.append(f"{column} >= CAST(CAST(${len(params)} AS text) AS {column_type})")
        if partition.hi is not None:
            params.append(partition.hi)
            clauses.append(f"{column} < CAST(CAST(${len(params)} AS text) AS {column_type})")
        where = " AND ".join(clauses) if clauses else "TRUE"

        return f"SELECT {columns} FROM {table} WHERE {where}", tuple(params)

    async def current_position(self) -> SourcePosition:
        """The source's current WAL position, as a number.

        Numeric rather than the `7/9B77D6D0` text form, because this value is
        compared against the LSN Debezium puts in each change event - and
        Debezium reports `pg_wal_lsn_diff(lsn, '0/0')`. Two representations of
        the same position that cannot be compared are worse than one.
        """
        async with transaction(self._engine) as connection:
            lsn = (
                await connection.execute(
                    text("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")
                )
            ).scalar_one()
        return SourcePosition(kind=PositionKind.LSN, value=str(int(lsn)))

    async def _key_range(
        self, connection: object, manifest: DatasetManifest, keys: tuple[str, ...]
    ) -> tuple[str | None, str | None]:
        """Read the key range, which an index makes cheap even on a huge table."""
        if len(keys) != 1:
            # Composite and absent keys are a Day 7 partitioning concern; there
            # is no single range to read here.
            return None, None

        preparer = self._engine.dialect.identifier_preparer
        # Each part is quoted separately: quoting "public.orders" whole yields
        # one identifier containing a dot, which names no table.
        table = ".".join(preparer.quote(part) for part in manifest.physical.reference.split("."))
        column = preparer.quote(keys[0])
        query = text(f"SELECT min({column})::text AS lo, max({column})::text AS hi FROM {table}")
        row = (await connection.execute(query)).one()  # type: ignore[attr-defined]
        return row.lo, row.hi


def _to_manifest(table: _Table) -> DatasetManifest:
    return DatasetManifest(
        name=table.qualified,
        physical=PhysicalRef(
            adapter=ADAPTER,
            reference=table.qualified,
            estimated_rows=table.row_estimate,
            estimated_bytes=table.byte_estimate,
        ),
        dataset_schema=DatasetSchema(
            keys=table.primary_key,
            fields=tuple(table.columns),
            foreign_keys=tuple(table.foreign_keys),
            indexes=tuple(table.indexes),
        ),
    )


def _distinct_keys(
    stats: Sequence[Row[tuple[object, ...]]], keys: tuple[str, ...], rows: int
) -> int | None:
    """Resolve `n_distinct` into a count.

    PostgreSQL reports a negative `n_distinct` as a fraction of the table,
    which is how it expresses "this grows with the table" - a unique key on a
    growing table reports -1, not the row count.
    """
    if len(keys) != 1:
        return None
    entry = next((row for row in stats if row.column_name == keys[0]), None)
    if entry is None:
        return None
    value = float(entry.n_distinct)
    if value < 0:
        return round(-value * rows)
    return int(value)


def _histograms(
    stats: Sequence[Row[tuple[object, ...]]],
) -> dict[str, tuple[str, ...]]:
    """Read every column's equi-depth histogram, where the planner has one.

    A column that is entirely most-common-values has no histogram, and neither
    does an unanalyzed table. Both are simply absent here, which the
    partitioner treats as missing information rather than as uniformity.
    """
    return {
        row.column_name: tuple(str(bound) for bound in row.histogram_bounds)
        for row in stats
        if row.histogram_bounds
    }


# Catalog type names are trusted but still validated: nothing built into SQL
# text should be able to carry a surprise, however it got there.
_SAFE_TYPE = re.compile(r"^[a-z][a-z0-9 _]*(\(\d+(,\s*\d+)?\))?(\[\])?$")


def _column_type(manifest: DatasetManifest, column: str) -> str:
    """The source-declared type of a partition column."""
    field = manifest.dataset_schema.field(column)
    if field is None:
        raise ValueError(
            f"dataset {manifest.name!r} has no field {column!r}; "
            f"discover the source before reading partitions"
        )
    if not _SAFE_TYPE.match(field.type):
        raise ValueError(f"unsupported column type for partitioning: {field.type!r}")
    return field.type
