# SPDX-License-Identifier: Apache-2.0
"""The MongoDB adapter against a real MongoDB.

Skipped unless a database is reachable, so a clone without one still passes.
Point it somewhere with `GANTRY_TEST_MONGODB_URI`.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Mapping

import gantry
import pytest

URI = os.environ.get("GANTRY_TEST_MONGODB_URI", "mongodb://localhost:27017")
DATABASE = os.environ.get("GANTRY_TEST_MONGODB_DATABASE", "gantry_test")


def _connect() -> gantry.nosql.NoSQLConnection:
    pytest.importorskip("pymongo")
    return gantry.nosql.connect("mongodb", uri=URI, database=DATABASE)


async def _reachable() -> bool:
    import importlib

    pymongo = importlib.import_module("pymongo")
    try:
        client = pymongo.AsyncMongoClient(URI, serverSelectionTimeoutMS=2000)
        await client.admin.command("ping")
    except Exception:
        return False
    finally:
        with contextlib.suppress(Exception):
            await client.close()
    return True


@pytest.fixture
async def db() -> gantry.nosql.NoSQLConnection:
    connection = _connect()
    if not await _reachable():
        pytest.skip(f"no MongoDB at {URI}")
    return connection


@pytest.fixture(autouse=True)
async def _seed(db: gantry.nosql.NoSQLConnection) -> None:
    import importlib

    pymongo = importlib.import_module("pymongo")
    client = pymongo.AsyncMongoClient(URI)
    database = client[DATABASE]
    await database.drop_collection("orders")
    await database.drop_collection("reporting.rollup")
    await database["orders"].insert_many(
        [
            {"status": "open", "region": "us", "amount": 10},
            {"status": "open", "region": "eu", "amount": 20},
            {"status": "closed", "region": "us", "amount": 30},
        ]
    )
    await client.close()


async def test_a_read_only_query_returns_bounded_documents(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    query = db.query(read_only=True, collections=["orders"], max_documents=2, timeout=15)
    result = await query("orders", {"status": "open"})

    assert result.inline is not None
    assert len(result.documents) <= 2


async def test_a_write_is_refused_before_it_reaches_the_database(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    query = db.query(read_only=True)
    result = await query("orders", [{"$match": {}}, {"$out": "reporting.rollup"}])

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "read-only" in result.failure.message


async def test_materialize_creates_a_governed_rollup(db: gantry.nosql.NoSQLConnection) -> None:
    materialize = db.materialize(
        sources=["orders"],
        destinations=["reporting.rollup"],
        verify=[gantry.nosql.destination_exists(), gantry.nosql.document_count(min=1)],
    )

    result = await materialize(
        "orders",
        [
            {"$match": {"status": "open"}},
            {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
            {"$out": "reporting.rollup"},
        ],
    )

    assert result.ok
    assert result.uri == "mongodb://reporting.rollup"


async def test_a_query_against_a_collection_outside_the_allow_list_is_rejected(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    query = db.query(read_only=True, collections=["orders"], timeout=15)
    result = await query("customers", {"status": "open"})

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "not allowed" in result.failure.message


async def test_a_query_against_a_denied_collection_is_rejected(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    query = db.query(read_only=True, denied_collections=["orders"], timeout=15)
    result = await query("orders", {"status": "open"})

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "denied" in result.failure.message


async def test_a_lookup_referencing_a_disallowed_collection_is_rejected(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    query = db.query(read_only=True, collections=["orders"], timeout=15)
    pipeline: list[Mapping[str, object]] = [
        {"$match": {}},
        {
            "$lookup": {
                "from": "customers",
                "localField": "region",
                "foreignField": "region",
                "as": "matched",
            }
        },
    ]
    result = await query("orders", pipeline)

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "customers" in result.failure.message


async def test_bounded_results_are_flagged_as_truncated_when_the_cap_is_hit(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    query = db.query(read_only=True, collections=["orders"], max_documents=1, timeout=15)
    result = await query("orders", {"status": "open"})

    assert result.inline is not None
    assert len(result.documents) == 1
    assert result.truncated is True


async def test_materialize_rejects_a_source_outside_the_allowed_sources(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    materialize = db.materialize(sources=["orders"], destinations=["reporting.rollup"])
    pipeline: list[Mapping[str, object]] = [
        {"$match": {"status": "open"}},
        {
            "$lookup": {
                "from": "customers",
                "localField": "region",
                "foreignField": "region",
                "as": "matched",
            }
        },
        {"$out": "reporting.rollup"},
    ]
    result = await materialize("orders", pipeline)

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "customers" in result.failure.message


async def test_materialize_rejects_a_destination_outside_the_allowed_destinations(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    materialize = db.materialize(sources=["orders"], destinations=["reporting.rollup"])
    result = await materialize(
        "orders", [{"$match": {"status": "open"}}, {"$out": "scratch.other"}]
    )

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "scratch.other" in result.failure.message


async def test_materialize_out_refuses_to_overwrite_an_existing_destination(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    materialize = db.materialize(sources=["orders"], destinations=["reporting.rollup"])
    pipeline: list[Mapping[str, object]] = [
        {"$match": {"status": "open"}},
        {"$out": "reporting.rollup"},
    ]

    first = await materialize("orders", pipeline)
    assert first.ok

    second = await materialize("orders", pipeline)
    assert second.status.value == "REJECTED"
    assert second.failure is not None
    assert "already exists" in second.failure.message


async def test_merge_with_a_disallowed_when_matched_is_rejected(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    materialize = db.materialize(sources=["orders"], destinations=["reporting.rollup"])
    pipeline: list[Mapping[str, object]] = [
        {"$match": {"status": "open"}},
        {"$merge": {"into": "reporting.rollup", "whenMatched": "keepExisting"}},
    ]
    result = await materialize("orders", pipeline)

    assert result.status.value == "REJECTED"
    assert result.failure is not None
    assert "whenMatched" in result.failure.message


async def test_merge_with_an_allowed_when_matched_writes_into_an_existing_destination(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    import importlib

    pymongo = importlib.import_module("pymongo")
    client = pymongo.AsyncMongoClient(URI)
    await client[DATABASE]["reporting.rollup"].insert_one({"_id": "us", "total": 999})
    await client.close()

    materialize = db.materialize(
        sources=["orders"],
        destinations=["reporting.rollup"],
        verify=[gantry.nosql.destination_exists(), gantry.nosql.document_count(min=1)],
    )
    pipeline: list[Mapping[str, object]] = [
        {"$match": {"status": "open"}},
        {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
        {"$merge": {"into": "reporting.rollup", "whenMatched": "replace"}},
    ]
    result = await materialize("orders", pipeline)

    assert result.ok
    assert result.uri == "mongodb://reporting.rollup"


async def test_document_count_verification_fails_outside_the_configured_range(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    materialize = db.materialize(
        sources=["orders"],
        destinations=["reporting.rollup"],
        verify=[gantry.nosql.document_count(min=5)],
    )
    pipeline: list[Mapping[str, object]] = [
        {"$match": {"status": "open"}},
        {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
        {"$out": "reporting.rollup"},
    ]
    result = await materialize("orders", pipeline)

    assert result.status.value == "VERIFICATION_FAILED"
    assert result.verification is not None
    assert not result.verification.ok


async def test_required_fields_verification_fails_when_a_field_is_missing(
    db: gantry.nosql.NoSQLConnection,
) -> None:
    materialize = db.materialize(
        sources=["orders"],
        destinations=["reporting.rollup"],
        verify=[gantry.nosql.required_fields(["total", "nonexistent_field"])],
    )
    pipeline: list[Mapping[str, object]] = [
        {"$match": {"status": "open"}},
        {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
        {"$out": "reporting.rollup"},
    ]
    result = await materialize("orders", pipeline)

    assert result.status.value == "VERIFICATION_FAILED"
    assert result.verification is not None
    check = next(c for c in result.verification.checks if c.name == "required_fields")
    assert not check.ok
    assert check.message is not None
    assert "nonexistent_field" in check.message
