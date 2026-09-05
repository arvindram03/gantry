# SPDX-License-Identifier: Apache-2.0
"""Consuming Debezium change events from Kafka.

Design document open question #2, answered for v1: **Gantry owns the applied
position.** Kafka's consumer group offsets are a transport detail and the
runtime does not rely on them.

The reason is not distrust of Kafka. A consumer group commit is a separate
durability domain from the target write, so committing an offset after applying
a change is a second commit that can fail independently - the classic
distributed-transaction problem, reintroduced through the back door. Recording
the transport position in the same store as the checkpoint puts progress in one
place that either advances or does not.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

from aiokafka import AIOKafkaConsumer, TopicPartition

from gantry.core.changes import ChangeEvent, ChangeOperation, StreamPosition
from gantry.core.positions import PositionKind, SourcePosition

DEFAULT_BOOTSTRAP = "localhost:19092"

# Debezium's operation codes.
_OPERATIONS = {
    "c": ChangeOperation.INSERT,
    "u": ChangeOperation.UPDATE,
    "d": ChangeOperation.DELETE,
    "r": ChangeOperation.SNAPSHOT_READ,
    "t": ChangeOperation.TRUNCATE,
}


class MalformedEventError(Exception):
    """An envelope that is not a Debezium change event."""


class KafkaCDCAdapter:
    """Streams Debezium events for one or more tables."""

    def __init__(
        self,
        *,
        topics: Sequence[str],
        bootstrap_servers: str = DEFAULT_BOOTSTRAP,
        group_id: str = "gantry",
    ) -> None:
        self._topics = tuple(topics)
        self._bootstrap = bootstrap_servers
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None
        self._latest_source_timestamp: datetime | None = None
        self._latest_lsn: int | None = None

    async def start(self, *, resume_from: StreamPosition | None = None) -> None:
        """Begin consuming, from a position the runtime recorded if given.

        Auto-commit is off. The runtime's own record of what it applied is the
        only progress that counts, so an offset committed by the client library
        could only ever disagree with it.
        """
        consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self._consumer = consumer

        if resume_from is not None:
            partition = TopicPartition(resume_from.topic, resume_from.partition)
            # Wait for the assignment before seeking; seeking an unassigned
            # partition is silently ignored.
            await consumer.seek_to_beginning()
            consumer.seek(partition, resume_from.offset + 1)

    def events(self) -> AsyncIterator[ChangeEvent]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[ChangeEvent]:
        consumer = self._require_consumer()
        async for message in consumer:
            event = parse_debezium(
                message.value, message.key, message.topic, message.partition, message.offset
            )
            if event is None:
                continue
            self._latest_source_timestamp = event.source_timestamp
            self._latest_lsn = event.source_lsn
            yield event

    async def poll(self, *, timeout_ms: int = 1000, max_records: int = 500) -> list[ChangeEvent]:
        """Take whatever is available now.

        A batch-shaped alternative to iterating, for callers that want to apply
        a group of changes in one transaction.
        """
        consumer = self._require_consumer()
        batches = await consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        events: list[ChangeEvent] = []
        for _, messages in batches.items():
            for message in messages:
                event = parse_debezium(
                    message.value, message.key, message.topic, message.partition, message.offset
                )
                if event is None:
                    continue
                self._latest_source_timestamp = event.source_timestamp
                self._latest_lsn = event.source_lsn
                events.append(event)
        return events

    async def source_position(self) -> SourcePosition:
        """The most recent source LSN this stream has produced."""
        return SourcePosition(
            kind=PositionKind.LSN, value=str(self._latest_lsn if self._latest_lsn else 0)
        )

    async def lag(self) -> timedelta:
        """How old the most recent event is.

        Measured against the source's own timestamp rather than against when
        the runtime received it, so a slow consumer and a quiet source are not
        confused for one another.
        """
        if self._latest_source_timestamp is None:
            return timedelta(0)
        return datetime.now(UTC) - self._latest_source_timestamp

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    def _require_consumer(self) -> AIOKafkaConsumer:
        if self._consumer is None:
            raise RuntimeError("adapter is not started")
        return self._consumer


def parse_debezium(
    value: bytes | None, key: bytes | None, topic: str, partition: int, offset: int
) -> ChangeEvent | None:
    """Turn a Debezium envelope into a ChangeEvent.

    Returns None for a tombstone - a null value marking a deleted key for log
    compaction, which carries no change of its own.
    """
    if value is None:
        return None

    try:
        envelope = json.loads(value)
    except json.JSONDecodeError as error:
        raise MalformedEventError(f"{topic}:{partition}:{offset} is not JSON: {error}") from error

    operation_code = envelope.get("op")
    operation = _OPERATIONS.get(operation_code)
    if operation is None:
        raise MalformedEventError(
            f"{topic}:{partition}:{offset} carries unknown operation {operation_code!r}"
        )

    source = envelope.get("source") or {}
    table = source.get("table")
    schema = source.get("schema")
    dataset = f"{schema}.{table}" if schema and table else topic

    timestamp_ms = source.get("ts_ms")
    if timestamp_ms is None:
        raise MalformedEventError(f"{topic}:{partition}:{offset} carries no source timestamp")

    return ChangeEvent(
        dataset=dataset,
        operation=operation,
        key=_parse_key(key),
        before=envelope.get("before"),
        after=envelope.get("after"),
        # The LSN is what every ordering decision downstream depends on.
        source_lsn=int(source.get("lsn") or 0),
        source_timestamp=datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC),
        transaction_id=str(source["txId"]) if source.get("txId") is not None else None,
        stream_position=StreamPosition(topic=topic, partition=partition, offset=offset),
    )


def _parse_key(key: bytes | None) -> dict[str, str]:
    if key is None:
        return {}
    try:
        decoded = json.loads(key)
    except json.JSONDecodeError:
        return {}
    if not isinstance(decoded, dict):
        return {}
    return {str(name): str(value) for name, value in decoded.items()}
