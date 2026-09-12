# SPDX-License-Identifier: Apache-2.0
"""An agent builds a revenue rollup in MongoDB, and it gets checked before anyone uses it.

**The situation.** There is an `orders` collection with a handful of open and closed orders
across two regions. Somebody — an analyst, a scheduled agent, a chat request — wants revenue
broken down by region, written to a collection a dashboard reads. The pipeline is written by a
model. The collection is read by people who will act on it.

    docker run -d --name mongo -p 27017:27017 mongo:7
    python examples/mongodb_rollup.py

**What the operator decides** (this file): which collections may be read, which collection may be
written, what must be true of the result, and how long it may take.

**What the agent decides**: the pipeline, and nothing else.
"""

from __future__ import annotations

import asyncio
import os

import gantry

URI = os.environ.get("GANTRY_MONGODB_URI", "mongodb://localhost:27017")
DATABASE = os.environ.get("GANTRY_MONGODB_DATABASE", "gantry_example")

ORDERS = "orders"
ROLLUP = "reporting.daily_rollup"
SCRATCH = "agent_scratch.orders_copy"


async def _seed(uri: str, database: str) -> None:
    import importlib

    pymongo = importlib.import_module("pymongo")
    client = pymongo.AsyncMongoClient(uri)
    db = client[database]
    await db.drop_collection(ORDERS)
    await db.drop_collection(ROLLUP)
    await db[ORDERS].insert_many(
        [
            {"status": "open", "region": "us", "amount": 120},
            {"status": "open", "region": "eu", "amount": 80},
            {"status": "closed", "region": "us", "amount": 200},
            {"status": "open", "region": "us", "amount": 40},
        ]
    )
    await client.close()


async def main() -> int:
    try:
        await _seed(URI, DATABASE)
    except Exception as error:
        print(f"could not seed MongoDB at {URI}: {error}")
        print("docker run -d --name mongo -p 27017:27017 mongo:7")
        return 1

    db = gantry.nosql.connect("mongodb", uri=URI, database=DATABASE)

    # The contract. Everything restrictive is decided here, once, by code that holds the
    # credentials — not by the model that writes the pipeline.
    materialize = db.materialize(
        sources=[ORDERS],
        destinations=[ROLLUP],
        verify=[
            gantry.nosql.destination_exists(),
            gantry.nosql.document_count(min=1, max=10),
        ],
    )

    print("1. the pipeline an agent proposed")
    result = await materialize(
        ORDERS,
        [
            {"$match": {"status": "open"}},
            {"$group": {"_id": "$region", "orders": {"$sum": 1}, "revenue": {"$sum": "$amount"}}},
            {"$out": ROLLUP},
        ],
    )
    print(f"   {result.status.value}  ->  {result.uri}")
    for check in result.verification.checks if result.verification else ():
        print(f"     {check.name:16s} ok={check.ok!s:6s} actual={check.actual}")
    if not result.ok:
        print(f"   {result.failure.message if result.failure else 'no result'}")
        return 1

    print("\n2. the same pipeline again — $out is create-only")
    again = await materialize(
        ORDERS,
        [
            {"$match": {"status": "open"}},
            {"$group": {"_id": "$region", "orders": {"$sum": 1}, "revenue": {"$sum": "$amount"}}},
            {"$out": ROLLUP},
        ],
    )
    print(f"   {again.status.value}: {again.failure.message if again.failure else ''}")

    print("\n3. a pipeline that writes somewhere it was not asked to")
    stray = await materialize(ORDERS, [{"$match": {}}, {"$out": SCRATCH}])
    print(f"   {stray.status.value}: {stray.failure.message if stray.failure else ''}")

    print("\n4. a read-only query an agent can run directly")
    query = db.query(collections=[ORDERS], max_documents=10)
    read = await query(ORDERS, {"status": "open"})
    print(f"   {read.status.value}, {len(read.inline.documents) if read.inline else 0} documents")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
