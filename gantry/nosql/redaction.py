# SPDX-License-Identifier: Apache-2.0
"""Field redaction for document stores: documents come back, a field's value does not.

The same `Redaction` as SQL (`gantry.sql.redaction`), with patterns named
`database.collection.field`; a nested field is a dotted path,
`shop.customers.address.street`. The database and collection may use `*` and
`?`; the field is named exactly, because a document store has no list of
fields to match a wildcard against.

**Masked before the agent's pipeline runs.** A pipeline is a list of stages,
so nothing has to be parsed: the engine puts its own `$set` first, which
replaces each redacted field with its mask, and every stage the agent wrote
sees only that. A `$match` on the field matches the mask, a `$group` groups by
it, a `$project` returns it. A `$lookup` into a collection with redacted
fields gets the same `$set` at the head of its own pipeline, so joined
documents are masked too; its `localField`/`foreignField` equality still
joins, on the values, inside the database.

- `hide` reads as `[redacted]`.
- `hash` reads as `h_` and a number, from MongoDB's `$toHashedIndexKey` over
  the value with the key in front, so the same value gives the same token and
  a guess cannot be hashed without the key. Needs MongoDB 7.0 or later. A
  field holding an object or an array cannot be hashed and fails the query.

A field that is null or missing stays so; a nested field whose parent is not
an object is left as it is. The pipeline is recorded as the agent wrote it;
the one that runs carries the key and is never stored.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping, Sequence
from typing import Any

from gantry.sql.redaction import HIDDEN, Redaction, RedactionError, RedactionMode

_EMPTY = ["missing", "null"]


def redact_pipeline(
    collection: str,
    pipeline: Sequence[Mapping[str, object]],
    *,
    database: str | None,
    redaction: Redaction,
) -> tuple[list[dict[str, object]], tuple[str, ...]]:
    """The pipeline to run, with masking stages in front of the agent's, and what they mask."""
    masked: dict[str, None] = {}
    head = _masking_stages(collection, database, redaction, masked)
    body = [_with_lookups_masked(stage, database, redaction, masked) for stage in pipeline]
    return [*head, *body], tuple(masked)


def fields_for(
    collection: str, database: str | None, redaction: Redaction
) -> dict[str, RedactionMode]:
    """The redacted fields of one collection, by path."""
    found: dict[str, RedactionMode] = {}
    for pattern, mode in redaction.columns.items():
        database_part, collection_part, field = pattern.split(".", 2)
        if not fnmatch.fnmatchcase((database or "").lower(), database_part):
            continue
        if not fnmatch.fnmatchcase(collection.lower(), collection_part):
            continue
        if any(mark in field for mark in "*?"):
            raise RedactionError(
                f"{pattern} names fields with a wildcard; on a document store, name each "
                "field exactly"
            )
        found[field] = RedactionMode(mode)
    return found


def _masking_stages(
    collection: str, database: str | None, redaction: Redaction, masked: dict[str, None]
) -> list[dict[str, object]]:
    stages: list[dict[str, object]] = []
    for path, mode in fields_for(collection, database, redaction).items():
        parts = path.split(".")
        stages.append({"$set": {parts[0]: _nested(parts, 1, mode, redaction.key)}})
        masked[f"{database or ''}.{collection}.{path}"] = None
    return stages


def _nested(parts: list[str], depth: int, mode: RedactionMode, key: str) -> object:
    """The value of `parts[:depth]`, with the field at the end of `parts` masked.

    Built from the outside in, so an object on the way is merged rather than
    replaced, and a parent that is missing or not an object is left alone
    instead of being made into `{}`.
    """
    here = "$" + ".".join(parts[:depth])
    if depth == len(parts):
        return _mask(here, mode, key)
    return {
        "$cond": [
            {"$eq": [{"$type": here}, "object"]},
            {"$mergeObjects": [here, {parts[depth]: _nested(parts, depth + 1, mode, key)}]},
            here,
        ]
    }


def _mask(path: str, mode: RedactionMode, key: str) -> object:
    if mode is RedactionMode.HIDE:
        replacement: object = {"$literal": HIDDEN}
    else:
        replacement = {
            "$concat": [
                "h_",
                {
                    "$toString": {
                        "$abs": {"$toHashedIndexKey": {"$concat": [key, {"$toString": path}]}}
                    }
                },
            ]
        }
    return {"$cond": [{"$in": [{"$type": path}, _EMPTY]}, path, replacement]}


def _with_lookups_masked(
    stage: Mapping[str, object],
    database: str | None,
    redaction: Redaction,
    masked: dict[str, None],
) -> dict[str, object]:
    """The stage as written, with any `$lookup` masking what it brings in."""
    if len(stage) != 1:
        return dict(stage)
    operator, body = next(iter(stage.items()))
    if operator != "$lookup" or not isinstance(body, Mapping):
        return dict(stage)
    joined = body.get("from")
    if not isinstance(joined, str):
        return dict(stage)
    inner = body.get("pipeline")
    own: list[Any] = list(inner) if isinstance(inner, Sequence) else []
    head = _masking_stages(joined, database, redaction, masked)
    rewritten = [
        _with_lookups_masked(item, database, redaction, masked)
        if isinstance(item, Mapping)
        else item
        for item in own
    ]
    if not head and rewritten == own:
        return dict(stage)
    return {operator: {**body, "pipeline": [*head, *rewritten]}}


__all__ = ["fields_for", "redact_pipeline"]
