# SPDX-License-Identifier: Apache-2.0
"""Change events.

The common shape a CDC adapter produces, whatever the source. A Debezium
envelope, an Oracle redo record and a Kafka topic message all carry the same
essentials: what happened, to which row, and where in the source's ordering it
sits.

`source_lsn` is the one field the runtime cannot do without. Ordering and
stale-write rejection both depend on being able to say that one version of a
row is older than another, and no amount of care in the apply path substitutes
for the source telling us.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ChangeOperation(StrEnum):
    """What happened to a row.

    `SNAPSHOT_READ` is distinct from `INSERT`: Debezium emits it while reading
    existing rows, and treating it as an insert would double-count work the
    snapshot phase already did.
    """

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    SNAPSHOT_READ = "snapshot_read"
    TRUNCATE = "truncate"


class StreamPosition(BaseModel):
    """Where an event sat in the transport.

    Kept separate from the source position because they answer different
    questions: this one says where to resume reading, the LSN says how old the
    data is. Conflating them is how a runtime ends up trusting a consumer group
    to be correct about data it never saw.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    topic: str
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)

    def __str__(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"

    @classmethod
    def parse(cls, value: str) -> StreamPosition:
        topic, partition, offset = value.rsplit(":", 2)
        return cls(topic=topic, partition=int(partition), offset=int(offset))


class ChangeEvent(BaseModel):
    """One row change, as the runtime sees it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset: str
    operation: ChangeOperation
    # Primary key values as text, so the shape does not depend on column types.
    key: dict[str, str]
    before: dict[str, object] | None = None
    after: dict[str, object] | None = None

    # The source's own ordering. Everything downstream that decides whether one
    # version supersedes another reads this.
    source_lsn: int = Field(ge=0)
    source_timestamp: datetime
    transaction_id: str | None = None
    stream_position: StreamPosition

    @model_validator(mode="after")
    def _check_event(self) -> ChangeEvent:
        if self.source_timestamp.tzinfo is None:
            raise ValueError("source_timestamp must be timezone-aware")
        if not self.key and self.operation is not ChangeOperation.TRUNCATE:
            raise ValueError(f"{self.operation.value} event carries no key")
        if (
            self.operation in (ChangeOperation.INSERT, ChangeOperation.UPDATE)
            and self.after is None
        ):
            raise ValueError(f"{self.operation.value} event carries no after image")
        if self.operation is ChangeOperation.DELETE and self.before is None:
            raise ValueError("delete event carries no before image")
        return self

    @property
    def key_text(self) -> str:
        """A stable rendering of the key, for routing and deduplication."""
        return "|".join(f"{name}={self.key[name]}" for name in sorted(self.key))

    @property
    def is_from_snapshot(self) -> bool:
        return self.operation is ChangeOperation.SNAPSHOT_READ
