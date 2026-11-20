# SPDX-License-Identifier: Apache-2.0
"""Field redaction against a real MongoDB.

What is claimed: a redacted field's value never comes back from a governed
query, however the pipeline reaches it (returned as is, projected under
another name, grouped by, matched on, or brought in by a `$lookup`); a hashed
field gives the same token for the same value, so it still groups and
counts; nested fields are masked without inventing parents; the key never
appears in a run; and a materialization from a collection with redacted
fields is refused.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
from collections.abc import Mapping
from typing import cast

import gantry
import pytest
from gantry.runs.status import RunStatus
from gantry.sql.redaction import HIDDEN, Redaction

from _live import require_live_or_skip

URI = os.environ.get("GANTRY_TEST_MONGODB_URI", "mongodb://localhost:27017")
DATABASE = os.environ.get("GANTRY_TEST_MONGODB_DATABASE", "gantry_test")
KEY = "test-key-0123456789abcdef"
TOKEN = re.compile(r"^h_\d+$")

CUSTOMERS = [
    {"_id": 1, "email": "ada@example.com", "plan": "pro", "address": {"street": "1 Main St"}},
    {"_id": 2, "email": "bo@example.com", "plan": "free", "address": {"street": "2 Side St"}},
    {"_id": 3, "email": "ada@example.com", "plan": "free"},
    {"_id": 4, "email": None, "plan": "pro", "phone": "+44 7700 900000"},
]
SECRETS = ("ada@example.com", "bo@example.com", "Main St", "Side St", "7700", KEY)


def _redaction() -> Redaction:
    return Redaction(
        {
            f"{DATABASE}.customers.email": "hash",
            f"{DATABASE}.customers.phone": "hide",
            f"{DATABASE}.customers.address.street": "hide",
        },
        key=KEY,
    )


async def _db() -> gantry.nosql.NoSQLConnection:
    import importlib

    pytest.importorskip("pymongo")
    pymongo = importlib.import_module("pymongo")
    client = pymongo.AsyncMongoClient(URI, serverSelectionTimeoutMS=2000)
    try:
        database = client[DATABASE]
        await database.drop_collection("customers")
        await database.drop_collection("orders")
        await database.drop_collection("copied")
        await database["customers"].insert_many(CUSTOMERS)
        await database["orders"].insert_many(
            [{"_id": 10, "customer": 1, "total": 5}, {"_id": 11, "customer": 2, "total": 7}]
        )
    except Exception:
        require_live_or_skip(f"no usable MongoDB at {URI.rsplit('@', 1)[-1]}")
        raise
    finally:
        with contextlib.suppress(Exception):
            await client.close()
    return gantry.nosql.connect("mongodb", uri=URI, database=DATABASE)


def _leaks(run: gantry.Run) -> list[str]:
    said = json.dumps(run.as_tool_result(), default=str)
    return [secret for secret in SECRETS if secret in said]


def _documents(run: gantry.Run) -> list[dict[str, object]]:
    assert run.status is RunStatus.ACCEPTED, (run.status, run.failure)
    return [dict(cast("Mapping[str, object]", document)) for document in run.documents]


async def test_every_way_of_reading_a_redacted_field_reads_the_mask() -> None:
    db = await _db()
    query = db.query(collections=["customers", "orders"], redaction=_redaction())

    pipelines: list[list[Mapping[str, object]]] = [
        [{"$sort": {"_id": 1}}],
        [{"$project": {"contact": "$email", "where": "$address.street", "phone": 1}}],
        [{"$group": {"_id": "$email", "n": {"$sum": 1}}}],
        [{"$set": {"lower": {"$toLower": "$email"}}}],
        [{"$project": {"all": "$$ROOT"}}],
        [
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "customer",
                    "foreignField": "_id",
                    "as": "who",
                }
            }
        ],
    ]
    for pipeline in pipelines:
        collection = "orders" if "$lookup" in pipeline[0] else "customers"
        run = await query(collection, pipeline)
        assert run.status is RunStatus.ACCEPTED, (pipeline, run.failure)
        assert _leaks(run) == [], (pipeline, _leaks(run))

    every = _documents(await query("customers", [{"$sort": {"_id": 1}}]))
    assert [document["plan"] for document in every] == ["pro", "free", "free", "pro"]
    assert TOKEN.match(str(every[0]["email"])) and every[0]["email"] == every[2]["email"]
    assert every[3]["email"] is None, "a null stays null"
    assert every[3]["phone"] == HIDDEN
    assert every[0]["address"] == {"street": HIDDEN}
    assert "address" not in every[2], "a missing parent is not made into {}"


async def test_a_match_on_a_redacted_field_matches_the_mask() -> None:
    db = await _db()
    query = db.query(collections=["customers"], redaction=_redaction())

    by_value = _documents(await query("customers", {"email": "ada@example.com"}))
    by_regex = _documents(await query("customers", {"email": {"$regex": "^ada"}}))
    [first] = _documents(await query("customers", {"_id": 1}))
    by_token = _documents(await query("customers", {"email": first["email"]}))
    grouped = _documents(
        await query("customers", [{"$group": {"_id": "$email", "n": {"$sum": 1}}}])
    )

    assert by_value == [] and by_regex == []
    assert sorted(document["_id"] for document in by_token) == [1, 3]  # type: ignore[type-var]
    assert sorted(document["n"] for document in grouped) == [1, 1, 2]  # type: ignore[type-var]


async def test_the_key_never_reaches_a_run_even_in_an_error() -> None:
    db = await _db()
    query = db.query(collections=["customers"], redaction=_redaction())

    failed = await query("customers", [{"$project": {"n": {"$toInt": "$email"}}}])

    assert failed.status is not RunStatus.ACCEPTED
    assert _leaks(failed) == []


async def test_a_materialization_from_a_redacted_collection_is_refused() -> None:
    db = await _db()
    build = db.materialize(
        sources=["customers", "orders"], destinations=["copied"], redaction=_redaction()
    )

    refused = await build("customers", [{"$out": "copied"}])

    assert refused.status is RunStatus.POLICY_REJECTED
    assert refused.failure is not None and "redacted fields" in refused.failure.message


async def test_describe_fields_lists_nested_paths_from_a_sample() -> None:
    db = await _db()

    fields = await db.describe_fields("customers")

    assert {"_id", "email", "plan", "address", "address.street", "phone"} <= set(fields)


def test_a_field_must_be_named_exactly() -> None:
    from gantry.nosql.redaction import fields_for
    from gantry.sql.redaction import RedactionError

    with pytest.raises(RedactionError):
        fields_for("customers", "shop", Redaction({"shop.customers.*_email": "hide"}))
    assert fields_for("customers", "shop", Redaction({"*.customers.address.street": "hide"})) == {
        "address.street": "hide"
    }
