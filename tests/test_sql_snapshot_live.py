# SPDX-License-Identifier: Apache-2.0
"""Reading a whole schema, and building it again through governed steps.

The claim worth testing is a round trip: a schema read from one PostgreSQL
database, every object created again in an empty one through
`schema_change`, reads back the same. A stub would agree with whatever this
code wrote; only the engine can say its own definitions survive.
"""

from __future__ import annotations

import os
from typing import Any

import gantry
import pytest
from gantry import Policy
from gantry.failure import FailureKind
from gantry.runs.status import RunStatus
from gantry.sql.schema_change import SchemaStep
from gantry.sql.snapshot import ObjectKind, SchemaObject

from _live import require_live_or_skip

POSTGRES = os.environ.get(
    "GANTRY_TEST_POSTGRES_URL", "postgresql://gantry:gantry@localhost:5432/gantry"
)
DESTINATION_DB = "gantry_snapshot_target"
DESTINATION = POSTGRES.rsplit("/", 1)[0] + f"/{DESTINATION_DB}"

SOURCE_SCHEMA = (
    "DROP SCHEMA IF EXISTS snap CASCADE",
    "CREATE SCHEMA snap",
    "CREATE TYPE snap.mood AS ENUM ('calm', 'busy', 'it''s fine')",
    "CREATE SEQUENCE snap.ticket_numbers START WITH 1000 INCREMENT BY 5",
    "CREATE TABLE snap.people (id serial PRIMARY KEY, email text NOT NULL, "
    "mood snap.mood DEFAULT 'calm', score numeric(6,2) CHECK (score >= 0), "
    "created_at timestamptz DEFAULT now(), "
    "email_domain text GENERATED ALWAYS AS (split_part(email, '@', 2)) STORED, "
    "CONSTRAINT people_email_unique UNIQUE (email))",
    "CREATE TABLE snap.tickets (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
    "person_id int NOT NULL REFERENCES snap.people (id) ON DELETE CASCADE, "
    "number int DEFAULT nextval('snap.ticket_numbers'), title text, closed boolean "
    "DEFAULT false)",
    "CREATE INDEX tickets_open ON snap.tickets (person_id) WHERE NOT closed",
    "CREATE INDEX people_lower_email ON snap.people (lower(email))",
    "CREATE VIEW snap.open_tickets AS SELECT t.id, p.email FROM snap.tickets t "
    "JOIN snap.people p ON p.id = t.person_id WHERE NOT t.closed",
    "CREATE MATERIALIZED VIEW snap.ticket_counts AS SELECT person_id, count(*) AS n "
    "FROM snap.tickets GROUP BY person_id",
    "CREATE FUNCTION snap.touch() RETURNS trigger LANGUAGE plpgsql AS $$ "
    "BEGIN NEW.title := coalesce(NEW.title, 'untitled'); RETURN NEW; END $$",
    "CREATE TRIGGER tickets_touch BEFORE INSERT ON snap.tickets FOR EACH ROW "
    "EXECUTE FUNCTION snap.touch()",
    "ALTER TABLE snap.people ENABLE ROW LEVEL SECURITY",
    "CREATE POLICY own_rows ON snap.people FOR SELECT TO public USING (email <> '')",
)

#: The order a schema is built in, for this test's own small planner.
ORDER = (
    ObjectKind.SCHEMA,
    ObjectKind.ENUM,
    ObjectKind.SEQUENCE,
    ObjectKind.FUNCTION,
    ObjectKind.TABLE,
    ObjectKind.CONSTRAINT,
    ObjectKind.INDEX,
    ObjectKind.VIEW,
    ObjectKind.MATERIALIZED_VIEW,
    ObjectKind.TRIGGER,
    ObjectKind.POLICY,
)


async def _execute(url: str, *statements: str) -> None:
    import asyncpg  # type: ignore[import-untyped]

    try:
        connection = await asyncpg.connect(url, timeout=5)
    except Exception:
        require_live_or_skip(f"no PostgreSQL at {url.rsplit('@', 1)[-1]}")
        raise
    try:
        for statement in statements:
            await connection.execute(statement)
    finally:
        await connection.close()


@pytest.fixture
async def databases() -> Any:
    pytest.importorskip("asyncpg")
    await _execute(
        POSTGRES,
        f"DROP DATABASE IF EXISTS {DESTINATION_DB} WITH (FORCE)",
        f"CREATE DATABASE {DESTINATION_DB}",
    )
    await _execute(POSTGRES, *SOURCE_SCHEMA)
    yield
    await _execute(POSTGRES, "DROP SCHEMA IF EXISTS snap CASCADE")
    await _execute(POSTGRES, f"DROP DATABASE IF EXISTS {DESTINATION_DB} WITH (FORCE)")


def _comparable(objects: tuple[SchemaObject, ...]) -> dict[tuple[str, str], str | None]:
    return {
        (item.kind.value, item.qualified_name): item.definition
        for item in objects
        if item.kind is not ObjectKind.EXTENSION
    }


def _steps(objects: tuple[SchemaObject, ...]) -> list[SchemaStep]:
    """Every object as a step, in build order: constraints by kind, keys first."""
    rank = {"primary key": 0, "unique": 1, "check": 2, "exclusion": 3, "foreign key": 4}
    steps: list[SchemaStep] = []
    for kind in ORDER:
        found = [item for item in objects if item.kind is kind]
        if kind is ObjectKind.CONSTRAINT:
            found.sort(key=lambda item: rank[str(item.details["type"])])
        for item in found:
            assert item.definition is not None
            steps.append(SchemaStep(kind, item.schema, item.name, item.definition, item.table))
            if kind is ObjectKind.TABLE and item.details.get("row_security"):
                steps.append(
                    SchemaStep(
                        kind,
                        item.schema,
                        item.name,
                        f'ALTER TABLE "{item.schema}"."{item.name}" ENABLE ROW LEVEL SECURITY',
                    )
                )
    return steps


async def test_a_schema_read_and_built_again_reads_back_the_same(databases: None) -> None:
    source = gantry.sql.connect("postgres", url=POSTGRES)
    target = gantry.sql.connect(
        "postgres",
        url=DESTINATION,
        policy=Policy("build", [gantry.allow.schema(destinations=["snap.*"])]),
    )

    read = await source.snapshot(["snap"])

    kinds = {item.kind for item in read.objects}
    assert kinds >= set(ORDER), f"every kind was read: missing {set(ORDER) - kinds}"
    people = read.find(ObjectKind.TABLE, "snap.people")
    assert people is not None and people.details["row_security"] is True
    assert [column.name for column in people.columns][:2] == ["id", "email"]
    generated = next(column for column in people.columns if column.name == "email_domain")
    assert generated.generated is not None and generated.default is None
    tickets = read.find(ObjectKind.TABLE, "snap.tickets")
    assert tickets is not None and tickets.columns[0].identity == "always"
    sequence = read.find(ObjectKind.SEQUENCE, "snap.people_id_seq")
    assert sequence is not None and sequence.details["owned_by"] == "snap.people.id"
    assert read.find(ObjectKind.SEQUENCE, "snap.tickets_id_seq") is None, (
        "an identity column's sequence comes with the column"
    )

    change = target.schema_change(schemas=["snap"])
    for step in _steps(read.objects):
        run = await change(step)
        assert run.status is RunStatus.ACCEPTED, (step.sql, run.failure)

    built = await target.snapshot(["snap"])
    assert _comparable(built.objects) == _comparable(read.objects)


async def test_a_step_outside_the_schemas_allowed_is_refused(databases: None) -> None:
    target = gantry.sql.connect(
        "postgres",
        url=DESTINATION,
        policy=Policy("build", [gantry.allow.schema(destinations=["*"])]),
    )
    change = target.schema_change(schemas=["snap"])

    elsewhere = await change(SchemaStep(ObjectKind.SCHEMA, "other", "other", "CREATE SCHEMA other"))
    lying = await change(
        SchemaStep(ObjectKind.TABLE, "snap", "t", "CREATE TABLE public.t (id int)")
    )
    not_ddl = await change(
        SchemaStep(ObjectKind.TABLE, "snap", "t", "DELETE FROM snap.t WHERE true")
    )
    extension = await change(
        SchemaStep(ObjectKind.EXTENSION, "public", "pgcrypto", "CREATE EXTENSION pgcrypto")
    )

    for run in (elsewhere, lying, not_ddl, extension):
        assert run.status is RunStatus.POLICY_REJECTED, run
    assert lying.failure is not None and "public.t" in lying.failure.message
    assert extension.failure is not None
    assert extension.failure.kind is FailureKind.DESTINATION_NOT_ALLOWED


async def test_the_policy_decides_and_a_rule_that_asks_refuses(databases: None) -> None:
    await _execute(DESTINATION, "CREATE SCHEMA snap")
    step = SchemaStep(ObjectKind.TABLE, "snap", "t", "CREATE TABLE snap.t (id int)")
    nothing = gantry.sql.connect("postgres", url=DESTINATION, policy=Policy("none", []))
    asks = gantry.sql.connect(
        "postgres",
        url=DESTINATION,
        policy=Policy(
            "asks", [gantry.allow.schema(destinations=["snap.*"], require_confirmation=True)]
        ),
    )

    refused = await nothing.schema_change(schemas=["snap"])(step)
    held = await asks.schema_change(schemas=["snap"])(step)

    assert refused.status is RunStatus.POLICY_REJECTED
    assert held.status is RunStatus.POLICY_REJECTED
    assert held.failure is not None and "cannot wait" in held.failure.message


async def test_a_step_the_engine_rejects_changes_nothing(databases: None) -> None:
    target = gantry.sql.connect(
        "postgres",
        url=DESTINATION,
        policy=Policy("build", [gantry.allow.schema(destinations=["snap.*"])]),
    )
    await _execute(DESTINATION, "CREATE SCHEMA snap")

    run = await target.schema_change(schemas=["snap"])(
        SchemaStep(
            ObjectKind.TABLE, "snap", "t", "CREATE TABLE snap.t (id int REFERENCES snap.nope (id))"
        )
    )

    assert run.status is not RunStatus.ACCEPTED
    built = await target.snapshot(["snap"])
    assert built.find(ObjectKind.TABLE, "snap.t") is None


# ------------------------------------------------------------------ mysql

MYSQL = os.environ.get("GANTRY_TEST_MYSQL_URL", "mysql://gantry:gantry@127.0.0.1:3306/analytics")


async def _mysql(*statements: str) -> None:
    aiomysql = pytest.importorskip("aiomysql")
    from gantry.sql.adapters.mysql import _connect_kwargs

    config = _connect_kwargs({"url": MYSQL})
    config["autocommit"] = True
    try:
        connection = await aiomysql.connect(**config)
    except Exception:
        require_live_or_skip(f"no MySQL at {MYSQL.rsplit('@', 1)[-1]}")
        raise
    try:
        async with connection.cursor() as cursor:
            for statement in statements:
                await cursor.execute(statement)
    finally:
        connection.close()


async def test_mysql_builds_a_schema_through_governed_steps_and_reads_it_back() -> None:
    await _mysql(
        "DROP VIEW IF EXISTS analytics.snap_open",
        "DROP TABLE IF EXISTS analytics.snap_orders",
        "DROP TABLE IF EXISTS analytics.snap_people",
    )
    db = gantry.sql.connect(
        "mysql",
        url=MYSQL,
        policy=Policy("build", [gantry.allow.schema(destinations=["analytics.snap_*"])]),
    )
    change = db.schema_change(schemas=["analytics"])
    steps = [
        SchemaStep(
            ObjectKind.TABLE,
            "analytics",
            "snap_people",
            "CREATE TABLE `analytics`.`snap_people` (`id` BIGINT NOT NULL AUTO_INCREMENT, "
            "`email` VARCHAR(255) NOT NULL, `mood` ENUM('calm','busy') DEFAULT 'calm', "
            "PRIMARY KEY (`id`))",
        ),
        SchemaStep(
            ObjectKind.TABLE,
            "analytics",
            "snap_orders",
            "CREATE TABLE `analytics`.`snap_orders` (`id` INT NOT NULL, `person_id` BIGINT "
            "NOT NULL, PRIMARY KEY (`id`))",
        ),
        SchemaStep(
            ObjectKind.CONSTRAINT,
            "analytics",
            "snap_people_email",
            "ALTER TABLE `analytics`.`snap_people` ADD CONSTRAINT `snap_people_email` "
            "UNIQUE (`email`)",
            table="snap_people",
        ),
        SchemaStep(
            ObjectKind.CONSTRAINT,
            "analytics",
            "snap_orders_person",
            "ALTER TABLE `analytics`.`snap_orders` ADD CONSTRAINT `snap_orders_person` "
            "FOREIGN KEY (`person_id`) REFERENCES `analytics`.`snap_people` (`id`)",
            table="snap_orders",
        ),
        SchemaStep(
            ObjectKind.INDEX,
            "analytics",
            "snap_orders_by_person",
            "CREATE INDEX `snap_orders_by_person` ON `analytics`.`snap_orders` (`person_id`, `id`)",
            table="snap_orders",
        ),
        SchemaStep(
            ObjectKind.VIEW,
            "analytics",
            "snap_open",
            "CREATE VIEW `analytics`.`snap_open` AS SELECT o.id, p.email FROM "
            "analytics.snap_orders o JOIN analytics.snap_people p ON p.id = o.person_id",
        ),
    ]
    try:
        for step in steps:
            run = await change(step)
            assert run.status is RunStatus.ACCEPTED, (step.sql, run.failure)

        read = await db.snapshot(["analytics"])

        people = read.find(ObjectKind.TABLE, "analytics.snap_people")
        assert people is not None and people.definition is not None
        assert [column.name for column in people.columns] == ["id", "email", "mood"]
        assert people.columns[0].identity == "by default", "AUTO_INCREMENT"
        unique = read.find(ObjectKind.CONSTRAINT, "analytics.snap_people.snap_people_email")
        assert unique is not None and unique.details["type"] == "unique"
        key = read.find(ObjectKind.CONSTRAINT, "analytics.snap_orders.snap_orders_person")
        assert key is not None and key.details["references"] == "analytics.snap_people"
        index = read.find(ObjectKind.INDEX, "analytics.snap_orders.snap_orders_by_person")
        assert index is not None and index.details["columns"] == ["person_id", "id"]
        assert read.find(ObjectKind.VIEW, "analytics.snap_open") is not None

        broken = await change(
            SchemaStep(
                ObjectKind.INDEX,
                "analytics",
                "nope",
                "CREATE INDEX `nope` ON `analytics`.`snap_orders` (`no_such_column`)",
                table="snap_orders",
            )
        )
        assert broken.status is not RunStatus.ACCEPTED, "prepared first, and refused"
    finally:
        await _mysql(
            "DROP VIEW IF EXISTS analytics.snap_open",
            "DROP TABLE IF EXISTS analytics.snap_orders",
            "DROP TABLE IF EXISTS analytics.snap_people",
        )
