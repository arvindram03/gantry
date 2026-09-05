"""The Movement used by the crash-replay test.

Shared by the test and the worker subprocess so both compile the same plan -
node identities must match across processes or a restart would not resume.
"""

from __future__ import annotations

from datetime import UTC, datetime

from gantry.core.verification import RowCountCheck
from gantry.movement.model import (
    Endpoint,
    Movement,
    MovementDataset,
    MovementMode,
    Ordering,
    OrderingScope,
    Partitioning,
    PartitionStrategy,
    WriteMode,
)

PLAN_AT = datetime(2026, 9, 17, tzinfo=UTC)
TARGET_TABLE = "public.customers_day9"

# public.customers rather than public.orders: a hundred million rows is a
# demo, not a test. One million rows across ten partitions is small enough to
# run in seconds and large enough that a kill lands mid-copy.
SOURCE_TABLE = "public.customers"
ROWS_PER_PARTITION = 100_000


def build_movement() -> Movement:
    return Movement(
        name="day9-crash-replay",
        source=Endpoint(adapter="postgres", connection_ref="source"),
        destination=Endpoint(adapter="postgres", connection_ref="target"),
        mode=MovementMode.SNAPSHOT,
        datasets=(
            MovementDataset(
                name=SOURCE_TABLE,
                source=SOURCE_TABLE,
                target=TARGET_TABLE,
                key_columns=("customer_id",),
                ordering=Ordering(scope=OrderingScope.NONE),
                partitioning=Partitioning(
                    strategy=PartitionStrategy.RANGE,
                    column="customer_id",
                    rows_per_partition=ROWS_PER_PARTITION,
                ),
                write_mode=WriteMode.UPSERT,
                verification=(RowCountCheck(),),
            ),
        ),
    )
