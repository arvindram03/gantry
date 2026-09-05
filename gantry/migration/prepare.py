# SPDX-License-Identifier: Apache-2.0
"""The Prepare phase (RFC 0 §7 Phase 4).

Everything here answers one question: **would this migration fail, and can we
find out before spending six hours discovering it?** A schema mismatch found in
Prepare costs a minute. The same mismatch found during cutover costs the
snapshot, the catch-up, and whatever the operator scheduled around them.

So each check below is cheap, runs before any data moves, and returns a
structured refusal rather than raising — the same shape as Analysis validation,
because a planner or an agent has to be able to act on it.

The checks are ordered by cost and by what makes later ones meaningless. There
is no point comparing schemas on a database you cannot reach, and no point
validating a replication slot for a snapshot-only migration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from gantry.core.dataset import DatasetManifest
from gantry.migration.compatibility import (
    CompatibilityRepair,
    CompatibilityReport,
    check_manifest,
)
from gantry.state.database import transaction

# A source with fewer free slots than this is one connector away from being
# unable to start CDC at all.
MINIMUM_FREE_SLOTS = 1


class PrepareCheck(StrEnum):
    SOURCE_REACHABLE = "source_reachable"
    TARGET_REACHABLE = "target_reachable"
    TARGET_WRITABLE = "target_writable"
    REPLICATION_CONFIGURED = "replication_configured"
    REPLICATION_SLOTS_AVAILABLE = "replication_slots_available"
    SCHEMA_COMPATIBLE = "schema_compatible"
    RECONCILIATION_RUNS = "reconciliation_runs"


@dataclass(frozen=True)
class PrepareFailure:
    check: PrepareCheck
    problem: str
    repair: str
    subject: str | None = None

    def describe(self) -> str:
        # No square brackets: these lines are rendered through Rich, which
        # reads `[...]` as markup and silently swallows it. A refusal that
        # loses the name of what it refused is worse than no refusal.
        where = f" ({self.subject})" if self.subject else ""
        return f"{self.check.value}{where}: {self.problem}"


@dataclass
class PrepareReport:
    """Whether this migration may start moving data."""

    migration: str
    failures: list[PrepareFailure] = field(default_factory=list)
    compatibility: list[CompatibilityReport] = field(default_factory=list)
    # Targets that do not exist yet. Not a failure — the target adapter
    # creates them — but worth saying out loud, because "we are about to
    # create four tables" is something an operator may want to stop.
    to_create: tuple[str, ...] = ()

    @property
    def ready(self) -> bool:
        return not self.failures

    def describe(self) -> str:
        if self.ready:
            creating = f", creating {len(self.to_create)} target(s)" if self.to_create else ""
            return f"{self.migration}: ready{creating}"
        return "; ".join(failure.describe() for failure in self.failures)


async def check_reachable(
    engine: AsyncEngine, *, check: PrepareCheck, label: str
) -> PrepareFailure | None:
    """The cheapest check there is, and the one that makes the rest moot."""
    try:
        async with transaction(engine) as connection:
            await connection.execute(text("SELECT 1"))
    # OSError as well as SQLAlchemyError: a refused connection or an unresolved
    # host never reaches the driver's own exception hierarchy, and that is
    # exactly the case this check exists to catch.
    except (SQLAlchemyError, OSError) as error:
        return PrepareFailure(
            check=check,
            subject=label,
            problem=f"cannot connect: {_brief(error)}",
            repair="check the connection string, credentials and network path",
        )
    return None


async def check_writable(engine: AsyncEngine) -> PrepareFailure | None:
    """Whether we can actually create a table, not whether we were told we can.

    Asking the catalog for granted privileges gets this wrong in every
    interesting case — inherited roles, default privileges, a read replica
    that answers happily until you write. Creating and dropping a temporary
    table is the question actually being asked.
    """
    try:
        async with transaction(engine) as connection:
            await connection.execute(
                text("CREATE TEMPORARY TABLE gantry_prepare_probe (id integer) ON COMMIT DROP")
            )
    except SQLAlchemyError as error:
        return PrepareFailure(
            check=PrepareCheck.TARGET_WRITABLE,
            subject="target",
            problem=f"cannot create tables: {_brief(error)}",
            repair="grant CREATE on the target schema to the migration's role",
        )
    return None


async def check_replication(engine: AsyncEngine) -> list[PrepareFailure]:
    """Whether the source could support CDC, before six hours of snapshot.

    A migration that discovers at hour six that `wal_level` is `replica` has
    wasted six hours, and changing it needs a restart.
    """
    failures: list[PrepareFailure] = []
    try:
        async with transaction(engine) as connection:
            wal_level = (await connection.execute(text("SHOW wal_level"))).scalar_one()
            maximum = int(
                (await connection.execute(text("SHOW max_replication_slots"))).scalar_one()
            )
            used = (
                await connection.execute(text("SELECT count(*) FROM pg_replication_slots"))
            ).scalar_one()
    except SQLAlchemyError as error:
        return [
            PrepareFailure(
                check=PrepareCheck.REPLICATION_CONFIGURED,
                subject="source",
                problem=f"cannot read replication settings: {_brief(error)}",
                repair="grant the migration's role permission to read replication state",
            )
        ]

    if wal_level != "logical":
        failures.append(
            PrepareFailure(
                check=PrepareCheck.REPLICATION_CONFIGURED,
                subject="source",
                problem=f"wal_level is {wal_level!r}, logical decoding needs 'logical'",
                repair="set wal_level=logical and restart the source",
            )
        )

    free = maximum - int(used)
    if free < MINIMUM_FREE_SLOTS:
        failures.append(
            PrepareFailure(
                check=PrepareCheck.REPLICATION_SLOTS_AVAILABLE,
                subject="source",
                problem=f"{used} of {maximum} replication slots in use, none free",
                repair="raise max_replication_slots, or drop a slot nothing is consuming",
            )
        )
    return failures


async def check_reconciliation(engine: AsyncEngine, *, target: str) -> PrepareFailure | None:
    """Prove the reconciliation query runs before there is data to reconcile.

    A counting query against an empty table costs nothing and catches the
    boring failures — a table name that does not resolve, a schema the role
    cannot see — at the point where fixing them is free.
    """
    try:
        async with transaction(engine) as connection:
            await connection.execute(text(f"SELECT count(*) FROM {_qualified(target)}"))
    except SQLAlchemyError as error:
        return PrepareFailure(
            check=PrepareCheck.RECONCILIATION_RUNS,
            subject=target,
            problem=f"reconciliation query does not run: {_brief(error)}",
            repair="check the target table name and the role's read access",
        )
    return None


async def prepare(
    migration: str,
    *,
    source: AsyncEngine,
    target: AsyncEngine,
    manifests: dict[str, DatasetManifest],
    targets: dict[str, str],
    target_manifests: dict[str, DatasetManifest] | None = None,
    needs_replication: bool = True,
) -> PrepareReport:
    """Run every Prepare check, cheapest first, and report what refuses.

    Stops early only where continuing would be meaningless: there is nothing to
    say about a schema on a database that will not answer.
    """
    report = PrepareReport(migration=migration)

    for engine, check, label in (
        (source, PrepareCheck.SOURCE_REACHABLE, "source"),
        (target, PrepareCheck.TARGET_REACHABLE, "target"),
    ):
        failure = await check_reachable(engine, check=check, label=label)
        if failure is not None:
            report.failures.append(failure)

    if report.failures:
        return report

    writable = await check_writable(target)
    if writable is not None:
        report.failures.append(writable)

    if needs_replication:
        report.failures.extend(await check_replication(source))

    discovered = target_manifests or {}
    to_create: list[str] = []
    for name, manifest in manifests.items():
        target_name = targets.get(name, name)
        compatibility = check_manifest(
            manifest, discovered.get(target_name), target_name=target_name
        )
        report.compatibility.append(compatibility)

        for mismatch in compatibility.failures:
            # A target that does not exist yet is not a refusal: the adapter
            # creates it. Saying so is still worth doing, because "about to
            # create four tables" is a thing an operator may want to stop.
            if mismatch.repair is CompatibilityRepair.CREATE_TARGET:
                to_create.append(target_name)
                continue
            # The column, not just the dataset. "Rows would be rejected" is
            # not actionable without knowing which column rejects them, and
            # the exit criterion for this phase is naming every one.
            column = f".{mismatch.column}" if mismatch.column else ""
            report.failures.append(
                PrepareFailure(
                    check=PrepareCheck.SCHEMA_COMPATIBLE,
                    subject=f"{name}{column} -> {target_name}",
                    problem=mismatch.problem,
                    repair=mismatch.repair.value,
                )
            )

        if target_name not in to_create:
            unreadable = await check_reconciliation(target, target=target_name)
            if unreadable is not None:
                report.failures.append(unreadable)

    report.to_create = tuple(sorted(set(to_create)))
    return report


def _qualified(reference: str) -> str:
    return ".".join(f'"{part}"' for part in reference.split("."))


def _brief(error: BaseException) -> str:
    """The line that identifies the problem, not the whole traceback.

    Same reasoning as the Analysis validator: the engine's own words are the
    useful part, and everything after the first line is context the reader
    already has.
    """
    text_form = str(getattr(error, "orig", error)).strip()
    return text_form.splitlines()[0] if text_form else error.__class__.__name__
