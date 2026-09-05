"""Routing changes so ordering means something.

Design document section 8.3: ordering is scoped explicitly, and the default is
per entity key. That means two changes to the same row must be applied in
source order, and two changes to different rows need not be.

Routing by key hash is what makes the default enforceable with more than one
worker. Every change to a key lands in the same lane, so no two workers can
ever race on one row - without paying for the global ordering the design warns
against buying by accident.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Iterable, Sequence

from gantry.core.changes import ChangeEvent


def lane_for(key_text: str, lanes: int) -> int:
    """Which lane a key belongs to.

    A stable hash, not Python's `hash()`, which is randomised per process - two
    workers would disagree about the same key.
    """
    if lanes < 1:
        raise ValueError(f"lanes must be at least 1, got {lanes}")
    digest = hashlib.blake2b(key_text.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % lanes


def route(events: Iterable[ChangeEvent], lanes: int) -> dict[int, list[ChangeEvent]]:
    """Split events into lanes, preserving order within each."""
    routed: dict[int, list[ChangeEvent]] = defaultdict(list)
    for event in events:
        routed[lane_for(event.key_text, lanes)].append(event)
    return dict(routed)


def order_by_key(events: Sequence[ChangeEvent]) -> list[ChangeEvent]:
    """Sort into source order within each key.

    Across keys the order is arbitrary but deterministic, so a shuffled batch
    and an ordered one produce the same sequence of writes per row - which is
    what makes the outcome independent of delivery order.
    """
    return sorted(events, key=lambda event: (event.key_text, event.source_lsn))


def latest_per_key(events: Sequence[ChangeEvent]) -> list[ChangeEvent]:
    """Keep only the newest change for each key.

    Collapsing a batch this way is safe because the apply path is
    last-writer-wins by LSN: applying the intermediate versions of a row would
    reach the same final state, more slowly.
    """
    newest: dict[str, ChangeEvent] = {}
    for event in events:
        current = newest.get(event.key_text)
        if current is None or event.source_lsn > current.source_lsn:
            newest[event.key_text] = event
    return [newest[key] for key in sorted(newest)]
