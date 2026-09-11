# SPDX-License-Identifier: Apache-2.0
"""The MongoDB adapter against a real MongoDB.

Skipped unless a database is reachable, so a clone without one still passes.
Point it somewhere with `GANTRY_TEST_MONGODB_URI`.
"""

from __future__ import annotations

import contextlib
import os

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
    assert len(result.inline.documents) <= 2


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
