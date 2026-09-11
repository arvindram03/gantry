# SPDX-License-Identifier: Apache-2.0
"""An agent runs nine pipelines against MongoDB, and every policy limit gets hit on purpose.

**The situation.** An `orders` collection has 5,000 documents across three regions and three
statuses, plus a `customers_pii` collection an agent must never read. An agent — a model, not the
operator — proposes queries and rollups against this data. Some of what it proposes is fine. Some
of it asks for more documents than the policy allows. Some of it reaches for a collection it was
never granted. Some of it tries to write through a read-only tool. Some of it produces a rollup
that fails verification after it already ran. Gantry is the layer between "the agent proposed
this" and "this actually happened," and this script prints what it decides at every step.

    docker compose -f examples/mongo/docker-compose.yml up -d --wait
    python examples/mongodb_policy_demo.py

**What the operator decides** (this file): which collections may be read, which may be written,
how many documents a read may return, and what must be true of a materialized rollup before
anyone reads it.

**What the agent decides**: the pipeline, and nothing else.
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
from collections.abc import Sequence
from dataclasses import dataclass

import gantry

URI = os.environ.get("GANTRY_MONGODB_URI", "mongodb://localhost:27017")
DATABASE = os.environ.get("GANTRY_MONGODB_DATABASE", "gantry_policy_demo")

ORDERS = "orders"
CUSTOMERS_PII = "customers_pii"
ROLLUP = "reporting.daily_rollup"
TIGHT_ROLLUP = "reporting.tight_rollup"
FIELDS_ROLLUP = "reporting.fields_rollup"
SCRATCH = "agent_scratch.orders_copy"

ORDER_COUNT = 5_000
REGIONS = ("us", "eu", "apac")
STATUSES = ("open", "closed", "refunded")

_TTY = sys.stdout.isatty()
_RESET = "\033[0m" if _TTY else ""
_BOLD = "\033[1m" if _TTY else ""
_DIM = "\033[2m" if _TTY else ""
_GREEN = "\033[32m" if _TTY else ""
_RED = "\033[31m" if _TTY else ""
_YELLOW = "\033[33m" if _TTY else ""

_OUTCOME_COLOR = {
    "ACCEPTED": _GREEN,
    "TRUNCATED": _YELLOW,
    "VERIFICATION_FAILED": _YELLOW,
    "REJECTED": _RED,
}


@dataclass(frozen=True, slots=True)
class Scenario:
    label: str
    category: str
    outcome: str
    detail: str


def _header(text: str) -> None:
    print(f"\n{_BOLD}{text}{_RESET}")


def _bar(count: int, cap: int, width: int = 30) -> str:
    filled = width if cap <= 0 else min(width, round(width * count / cap))
    return "█" * filled + "░" * (width - filled)


async def _seed(uri: str, database: str) -> None:
    import importlib

    pymongo = importlib.import_module("pymongo")
    client = pymongo.AsyncMongoClient(uri)
    db = client[database]
    for name in (ORDERS, CUSTOMERS_PII, ROLLUP, TIGHT_ROLLUP, FIELDS_ROLLUP, SCRATCH):
        await db.drop_collection(name)
    random.seed(7)
    await db[ORDERS].insert_many(
        [
            {
                "status": random.choice(STATUSES),
                "region": random.choice(REGIONS),
                "amount": random.randint(10, 500),
            }
            for _ in range(ORDER_COUNT)
        ]
    )
    await db[CUSTOMERS_PII].insert_many(
        [{"email": f"customer{i}@example.com", "name": f"Customer {i}"} for i in range(20)]
    )
    await client.close()


def _summary(scenarios: Sequence[Scenario]) -> None:
    _header("=== policy decisions this run ===")
    label_width = max(len(s.label) for s in scenarios)
    category_width = max(len(s.category) for s in scenarios)
    for index, scenario in enumerate(scenarios, start=1):
        color = _OUTCOME_COLOR.get(scenario.outcome, "")
        print(
            f"  {index:2d}. {scenario.label:<{label_width}}  "
            f"{_DIM}[{scenario.category:<{category_width}}]{_RESET}  "
            f"{color}{scenario.outcome:<19}{_RESET} {_DIM}{scenario.detail}{_RESET}"
        )
    accepted = sum(1 for s in scenarios if s.outcome == "ACCEPTED")
    truncated = sum(1 for s in scenarios if s.outcome == "TRUNCATED")
    rejected = sum(1 for s in scenarios if s.outcome == "REJECTED")
    verification_failed = sum(1 for s in scenarios if s.outcome == "VERIFICATION_FAILED")
    print(
        f"\n  {len(scenarios)} scenarios: {_GREEN}{accepted} accepted{_RESET}, "
        f"{_YELLOW}{truncated} capped by a document limit{_RESET}, "
        f"{_RED}{rejected} rejected by policy{_RESET}, "
        f"{_YELLOW}{verification_failed} rejected at verification{_RESET}\n"
    )


async def main() -> int:
    try:
        await _seed(URI, DATABASE)
    except Exception as error:
        print(f"could not seed MongoDB at {URI}: {error}")
        print("docker compose -f examples/mongo/docker-compose.yml up -d --wait")
        return 1

    db = gantry.nosql.connect("mongodb", uri=URI, database=DATABASE)
    scenarios: list[Scenario] = []

    _header("1. a governed read that stays inside its document limit")
    reader = db.query(collections=[ORDERS], max_documents=ORDER_COUNT)
    closed_apac = await reader(ORDERS, {"status": "closed", "region": "apac"})
    n1 = len(closed_apac.inline.documents) if closed_apac.inline else 0
    print(f"   {closed_apac.status.value}: {n1} documents, well under the limit")
    scenarios.append(
        Scenario("read within its document limit", "document limit", "ACCEPTED", f"{n1} docs")
    )

    _header("2. the same agent asks for everything — the document limit is hit")
    capped = db.query(collections=[ORDERS], max_documents=50)
    everything = await capped(ORDERS, {})
    n2 = len(everything.inline.documents) if everything.inline else 0
    truncated = bool(everything.inline and everything.inline.truncated)
    print(f"   policy caps this tool at 50 documents; {ORDER_COUNT} orders actually match")
    print(f"   {_bar(n2, 50)} {n2}/50 returned, truncated={truncated}")
    scenarios.append(
        Scenario(
            "unbounded read capped at 50 documents",
            "document limit",
            "TRUNCATED" if truncated else everything.status.value,
            f"{n2} of {ORDER_COUNT} matching documents returned",
        )
    )

    _header("3. the agent reaches for a collection it was never granted")
    leak = await reader(CUSTOMERS_PII, {})
    message = leak.failure.message if leak.failure else ""
    print(f"   {leak.status.value}: {message}")
    scenarios.append(
        Scenario("read outside allowed collections", "collection scope", leak.status.value, message)
    )

    _header("4. the agent tries to write through a read-only tool")
    write_attempt = await reader(ORDERS, [{"$match": {}}, {"$out": SCRATCH}])
    message = write_attempt.failure.message if write_attempt.failure else ""
    print(f"   {write_attempt.status.value}: {message}")
    scenarios.append(
        Scenario(
            "write attempted through a read-only tool",
            "read-only policy",
            write_attempt.status.value,
            message,
        )
    )

    _header("5. a governed materialization, accepted and verified")
    rollup = db.materialize(
        sources=[ORDERS],
        destinations=[ROLLUP],
        verify=[gantry.nosql.destination_exists(), gantry.nosql.document_count(min=1, max=20)],
    )
    rollup_pipeline = [
        {
            "$group": {
                "_id": {"region": "$region", "status": "$status"},
                "orders": {"$sum": 1},
                "revenue": {"$sum": "$amount"},
            }
        },
        {"$out": ROLLUP},
    ]
    first = await rollup(ORDERS, rollup_pipeline)
    print(f"   {first.status.value} -> {first.uri}")
    scenarios.append(
        Scenario(
            "materialization accepted and verified",
            "verification",
            first.status.value,
            first.uri or "",
        )
    )

    _header("6. the same materialization again — $out is create-only")
    second = await rollup(ORDERS, rollup_pipeline)
    message = second.failure.message if second.failure else ""
    print(f"   {second.status.value}: {message}")
    scenarios.append(
        Scenario(
            "$out rerun against an existing destination",
            "create-only",
            second.status.value,
            message,
        )
    )

    _header("7. verification catches a rollup with too many groups")
    tight = db.materialize(
        sources=[ORDERS],
        destinations=[TIGHT_ROLLUP],
        verify=[gantry.nosql.document_count(min=1, max=2)],
    )
    tight_pipeline = [
        {
            "$group": {
                "_id": {"region": "$region", "status": "$status"},
                "orders": {"$sum": 1},
            }
        },
        {"$out": TIGHT_ROLLUP},
    ]
    over = await tight(ORDERS, tight_pipeline)
    check = over.verification.checks[0] if over.verification and over.verification.checks else None
    detail = f"document_count actual={check.actual if check else '?'}, policy max=2"
    print(f"   {over.status.value}: {detail}")
    scenarios.append(
        Scenario(
            "rollup exceeds its document_count check", "verification", over.status.value, detail
        )
    )

    _header("8. verification catches a field that never showed up")
    fields = db.materialize(
        sources=[ORDERS],
        destinations=[FIELDS_ROLLUP],
        verify=[gantry.nosql.required_fields(["average_order_value"])],
    )
    fields_pipeline = [
        {"$group": {"_id": "$region", "orders": {"$sum": 1}, "revenue": {"$sum": "$amount"}}},
        {"$out": FIELDS_ROLLUP},
    ]
    missing = await fields(ORDERS, fields_pipeline)
    check = (
        missing.verification.checks[0]
        if missing.verification and missing.verification.checks
        else None
    )
    detail = (check.message or "") if check else ""
    print(f"   {missing.status.value}: {detail}")
    scenarios.append(
        Scenario(
            "required field missing from the rollup", "verification", missing.status.value, detail
        )
    )

    _header("9. the agent tries to materialize somewhere it was not granted")
    stray = await rollup(ORDERS, [{"$match": {}}, {"$out": SCRATCH}])
    message = stray.failure.message if stray.failure else ""
    print(f"   {stray.status.value}: {message}")
    scenarios.append(
        Scenario(
            "materialize to an ungranted destination",
            "collection scope",
            stray.status.value,
            message,
        )
    )

    _summary(scenarios)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
