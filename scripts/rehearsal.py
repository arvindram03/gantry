"""The v1 rehearsal: every guarantee, in order, timed, without intervention.

Drives the public surfaces — the CLI and the typed API — rather than reaching
into the runtime. That is deliberate. A rehearsal built out of internals proves
the internals work, which the test suite already does; what it cannot show is
whether the thing a person actually uses holds together, and that is the part
worth rehearsing.

    make dev-up && uv run alembic upgrade head
    uv run python scripts/rehearsal.py --rows 2000000

Each step says what it proved and how long it took. A step that does not hold
stops the run: a rehearsal that continues past a failure is a slideshow.

Two things here are deliberately delegated rather than reimplemented. The
kill -9 replay and duplicate-delivery idempotency live in the chaos suite,
which this script runs as one of its steps — a second implementation of a
crash test is a second thing that can be subtly wrong. Likewise the CDC lag
measurement, which needs Debezium and Kafka running and is asserted by the
handoff integration test.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MOVEMENT = ROOT / "examples/postgres-to-postgres/movement.yaml"
POLICY = ROOT / "spec/policy/agent-access.yaml"

SOURCE_URL = os.environ.get(
    "GANTRY_SOURCE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15432/gantry"
)
TARGET_URL = os.environ.get(
    "GANTRY_TARGET_URL", "postgresql+asyncpg://gantry:gantry@localhost:15433/gantry"
)
META_URL = os.environ.get(
    "GANTRY_DATABASE_URL", "postgresql+asyncpg://gantry:gantry@localhost:15434/gantry_meta"
)

BOLD, GREEN, RED, DIM, OFF = "\033[1m", "\033[32m", "\033[31m", "\033[2m", "\033[0m"


class RehearsalError(AssertionError):
    """A step did not prove what it claimed to."""


@dataclass
class Timeline:
    steps: list[tuple[str, float, str]] = field(default_factory=list)

    def record(self, name: str, seconds: float, proved: str) -> None:
        self.steps.append((name, seconds, proved))

    def render(self) -> str:
        width = max((len(name) for name, _, _ in self.steps), default=4)
        lines = [f"{'step'.ljust(width)}  {'seconds':>8}  proved"]
        for name, seconds, proved in self.steps:
            lines.append(f"{name.ljust(width)}  {seconds:>8.1f}  {proved}")
        total = sum(seconds for _, seconds, _ in self.steps)
        lines.append(f"{'total'.ljust(width)}  {total:>8.1f}")
        return "\n".join(lines)


TIMELINE = Timeline()


@contextmanager
def step(number: int, name: str) -> Iterator[list[str]]:
    proved: list[str] = []
    print(f"\n{BOLD}{number}. {name}{OFF}", flush=True)
    started = time.perf_counter()
    yield proved
    elapsed = time.perf_counter() - started
    TIMELINE.record(f"{number}. {name}", elapsed, "; ".join(proved))
    print(f"   {GREEN}ok{OFF}  {elapsed:.1f}s  {'; '.join(proved)}", flush=True)


def run(command: Sequence[str], *, expect: int = 0, cwd: Path = ROOT) -> str:
    finished = subprocess.run(list(command), capture_output=True, text=True, cwd=cwd)
    output = finished.stdout + finished.stderr
    if finished.returncode != expect:
        raise RehearsalError(
            f"`{' '.join(command)}` exited {finished.returncode}, expected {expect}\n{output}"
        )
    return output


def gantry(*args: str, expect: int = 0) -> str:
    return run([sys.executable, "-m", "gantry.cli.main", *args], expect=expect)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RehearsalError(message)


MOVEMENT_NAME = "orders-snapshot"


def _clear_operation(name: str) -> None:
    """Remove an Operation and everything hanging off it.

    The table list is derived from the schema rather than written out here, so
    a table added later is cleared without anyone remembering to add it.
    """
    from gantry.state.tables import operation_dependents

    tables = [table.name for table in operation_dependents()] + ["operations"]
    column = {"operations": "name"}
    statements = ";".join(
        f"DELETE FROM {table} WHERE {column.get(table, 'operation')} = '{name}'" for table in tables
    )
    psql(META_URL, statements)


def psql(url: str, statement: str) -> str:
    """Run one statement through psql, for the fault injection a user would do."""
    dsn = url.replace("postgresql+asyncpg://", "postgresql://")
    return run(["psql", dsn, "-t", "-A", "-c", statement])


# ---------------------------------------------------------------- the steps


def seed_and_register(args: argparse.Namespace) -> None:
    with step(1, "seed and register") as proved:
        if args.reset:
            # Seeding tops up rather than replaces, so a smaller --rows cannot
            # shrink a source that already holds more. Resetting is explicit
            # because it destroys data, and a rehearsal that quietly truncated
            # whatever it found would be a bad neighbour on a shared stack.
            psql(SOURCE_URL, "TRUNCATE public.orders, public.customers CASCADE")
            psql(TARGET_URL, "TRUNCATE public.orders, public.customers CASCADE")
            # And the control state with it. A Movement left mid-execution by
            # an earlier run holds leases and quarantined partitions that a
            # fresh plan version does not clear - see docs/guarantees.md on
            # starting against an Operation that is already executing.
            _clear_operation(MOVEMENT_NAME)
        if not args.skip_seed:
            # Customers are seeded independently of orders because the crash
            # test partitions customers: tying the two together would mean
            # seeding a hundred million orders to exercise a kill.
            gantry("seed", "--rows", str(args.rows), "--customers", str(args.customers))
        output = gantry("discover", "--registry", str(args.registry))
        require("public.orders" in output, "discovery did not find public.orders")
        proved.append(
            f"{args.rows:,} orders and {args.customers:,} customers seeded"
            if not args.skip_seed
            else "reused source"
        )
        proved.append(f"{output.count('registered')} datasets registered from discovery")


def move(args: argparse.Namespace) -> None:
    with step(3, "movement: plan and run") as proved:
        planned = gantry("plan", str(MOVEMENT))
        require("plan" in planned.lower(), f"plan produced nothing:\n{planned}")

        started = time.perf_counter()
        output = gantry("start", str(MOVEMENT), "--backend", args.backend)
        seconds = time.perf_counter() - started
        require("ok" in output.lower() or "verified" in output.lower(), output)

        moved = _int_after(output, r"rows[_ ]inserted[^0-9]*([0-9,]+)")
        proved.append(f"{moved:,} rows moved" if moved else "movement completed")
        if moved and seconds > 0:
            proved.append(f"{moved / seconds:,.0f} rows/sec")


# The crash-replay test partitions public.customers at 100,000 rows each and
# kills the worker after three commits, so it needs enough customers to make
# several partitions. Below this the worker finishes before it can be killed
# and the failure looks like a crash-replay bug rather than a short table.
CHAOS_MIN_CUSTOMERS = 1_000_000


def faults(args: argparse.Namespace) -> None:
    if args.skip_chaos:
        return
    if args.customers < CHAOS_MIN_CUSTOMERS:
        print(
            f"\n{BOLD}2. faults{OFF}\n   {DIM}skipped: the crash test needs at least "
            f"{CHAOS_MIN_CUSTOMERS:,} customers to kill a worker mid-partition, "
            f"and this run seeded {args.customers:,}{OFF}",
            flush=True,
        )
        TIMELINE.record("2. faults", 0.0, f"skipped, needs >= {CHAOS_MIN_CUSTOMERS:,} customers")
        return
    with step(2, "faults: crash replay and duplicate delivery") as proved:
        env = dict(os.environ, PYTHONPATH="tests/integration/helpers")
        finished = subprocess.run(
            [sys.executable, "-m", "pytest", "-m", "chaos", "-q", "-p", "no:randomly"],
            capture_output=True,
            text=True,
            cwd=ROOT,
            env=env,
        )
        require(finished.returncode == 0, finished.stdout + finished.stderr)
        passed = _int_after(finished.stdout, r"([0-9]+) passed") or 0
        proved.append(f"{passed} chaos tests passed, including a real kill -9")


def corrupt_and_repair(args: argparse.Namespace) -> None:
    with step(4, "corruption caught and repaired in place") as proved:
        clean = gantry("verify", str(MOVEMENT))
        require("failed" not in clean.lower(), f"the target was already wrong:\n{clean}")

        psql(TARGET_URL, "UPDATE public.orders SET amount = amount + 1 WHERE order_id = 1")
        broken = gantry("verify", str(MOVEMENT), expect=args.verify_failure_exit)
        require("chunk_checksum" in broken or "failed" in broken.lower(), broken)

        partition = _first_match(broken, r"(public\.orders/\d+)")
        require(partition is not None, f"verification named no partition to repair:\n{broken}")
        assert partition is not None

        repaired = gantry("repair", str(MOVEMENT), partition)
        require("failed" not in repaired.lower(), repaired)

        after = gantry("verify", str(MOVEMENT))
        require("failed" not in after.lower(), f"still wrong after repair:\n{after}")
        proved.append(f"one row corrupted, found in {partition}, repaired without a full re-copy")


def analysis(args: argparse.Namespace) -> None:
    with step(5, "analysis: findings, and a rejection despite engine success") as proved:
        output = run(
            [sys.executable, str(ROOT / "scripts" / "guarantee_boundary.py")],
            cwd=ROOT,
        )
        require("PASSED" in output and "FAILED" in output, output)
        require("withheld" in output, f"findings were not withheld on failure:\n{output}")
        proved.append("well-formed join passed and published findings")
        proved.append("expanding join rejected by Gantry after the engine reported SUCCESS")


def provenance(args: argparse.Namespace) -> None:
    with step(6, "provenance: a finding traced to a Movement checkpoint") as proved:
        output = gantry("results", "provenance", "checkout-regression.analysis")
        require("computation" in output, output)
        require("read " in output, f"no Dataset versions resolved:\n{output}")
        require("unresolved" not in output, f"the chain had gaps:\n{output}")
        require(
            "produced by" in output,
            f"no upstream Movement was linked, so nothing was traced:\n{output}",
        )
        checkpoints = _int_after(output, r"checkpoints\s+(\d+)") or 0
        require(checkpoints > 0, f"the chain reached no checkpoint:\n{output}")
        upstream = _first_match(output, r"produced by\s+(\S+)")
        proved.append(f"artifact, dataset versions and {checkpoints} checkpoints from {upstream}")


def agent_access(args: argparse.Namespace) -> None:
    with step(7, "agent access: rows denied, aggregate allowed") as proved:
        gantry("policy", "show", "--policy", str(POLICY))

        denied = gantry(
            "dataset",
            "sample",
            "public.orders",
            "--registry",
            str(args.registry),
            "--limit",
            "5",
            "--reason",
            "rehearsal",
            expect=3,
        )
        require("denied" in denied, f"raw rows were not denied:\n{denied}")

        allowed = gantry(
            "dataset",
            "query",
            "public.orders",
            "--registry",
            str(args.registry),
            "--agg",
            "count",
            "--agg",
            "avg:amount",
        )
        require("allow" in allowed, f"the aggregate was not permitted:\n{allowed}")
        proved.append("sample denied by policy default; aggregate over the same table returned")


# ---------------------------------------------------------------- helpers


# ------------------------------------------------------- the migration suite

MIGRATION_SPEC = ROOT / "spec/examples/migration-rehearsal.yaml"
MIGRATION_NAME = "orders-rehearsal"


def _clear_migration(name: str) -> None:
    psql(
        META_URL,
        f"DELETE FROM migration_transitions WHERE migration = '{name}';"
        f"DELETE FROM migrations WHERE name = '{name}'",
    )


def _break_target(statement: str) -> None:
    psql(TARGET_URL, statement)


def migration_prepare(args: argparse.Namespace) -> None:
    with step(2, "prepare refuses an incompatible target before anything moves") as proved:
        _clear_migration(MIGRATION_NAME)
        _clear_operation(MOVEMENT_NAME)
        gantry("migration", "plan", str(MIGRATION_SPEC))

        before = _target_rows()
        _break_target("ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric(6,2)")

        output = gantry(
            "migration", "start", str(MIGRATION_SPEC), "--backend", args.backend, expect=1
        )
        require("not ready" in output, f"prepare did not refuse:\n{output}")
        require("amount" in output, f"the refusal did not name the column:\n{output}")

        after = _target_rows()
        require(after == before, f"rows moved despite a refusal: {before} -> {after}")

        state = _migration_state()
        require(state == "planned", f"expected planned after a refusal, got {state!r}")
        proved.append("refused, named the column, moved no rows, stayed replannable")


def migration_snapshot(args: argparse.Namespace) -> None:
    with step(3, "movement runs and reconciliation opens the door to cutover") as proved:
        _break_target("ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric(12,2)")
        output = gantry("migration", "start", str(MIGRATION_SPEC), "--backend", args.backend)

        require("ready_for_cutover" in output, f"did not reach cutover:\n{output}")
        agreed = output.count("agrees")
        require(agreed >= 1, f"nothing reconciled:\n{output}")
        proved.append(f"{agreed} dataset(s) agreed at a stated watermark")


def migration_localise(args: argparse.Namespace) -> None:
    with step(4, "a corrupted row is localised, repaired, and reconciles clean") as proved:
        # Derived, not hardcoded. A fixed key silently stops corrupting
        # anything the moment the rehearsal runs at a smaller scale, and a
        # reconciliation that agrees because nothing was broken looks exactly
        # like one that agrees because everything is right.
        victim = _mid_key()
        _break_target(f"UPDATE public.orders SET amount = amount + 1 WHERE order_id = {victim}")

        output = gantry("migration", "reconcile", str(MIGRATION_SPEC), expect=1)
        require("disagrees" in output, f"corruption went unnoticed:\n{output}")
        require(str(victim) in output, f"the differing key was not named:\n{output}")

        comparisons = _int_after(output, r"in (\d+) comparisons") or 0
        require(comparisons > 1, "the drill-down enumerated instead of halving")

        _break_target(f"UPDATE public.orders SET amount = amount - 1 WHERE order_id = {victim}")
        clean = gantry("migration", "reconcile", str(MIGRATION_SPEC))
        require("disagrees" not in clean, f"still disagreeing after repair:\n{clean}")
        proved.append(f"located one row in {comparisons} comparisons, repaired, reconciled clean")


def migration_gates(args: argparse.Namespace) -> None:
    with step(5, "cutover refused: a gate fails, and nobody has approved") as proved:
        # The plan called for a CDC-lag refusal. This demo migration is
        # snapshot-only, so its lag gate is legitimately disabled; the refusal
        # is driven by a gate that genuinely applies instead. Forcing a lag
        # reading nobody took would demonstrate the wrong thing.
        _break_target("ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric(6,2)")

        refused = gantry(
            "migration",
            "cutover",
            str(MIGRATION_SPEC),
            "--approved-by",
            "rehearsal",
            "--reason",
            "trying it on",
            expect=1,
        )
        require("refused" in refused, f"a failing gate did not refuse:\n{refused}")
        require("schemaCompatible" in refused, f"the failing gate was not named:\n{refused}")

        _break_target("ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric(12,2)")
        gantry("migration", "start", str(MIGRATION_SPEC), "--backend", args.backend)

        unapproved = gantry("migration", "gates", str(MIGRATION_SPEC), expect=1)
        require("requireApproval" in unapproved, f"approval was not required:\n{unapproved}")
        require(
            "blocked by requireApproval" in unapproved,
            f"an unapproved cutover was not blocked:\n{unapproved}",
        )
        proved.append("a failing gate refused by name; without an approver, approval blocks")


def migration_cutover(args: argparse.Namespace) -> None:
    with step(6, "approved cutover opens the rollback window") as proved:
        output = gantry(
            "migration",
            "cutover",
            str(MIGRATION_SPEC),
            "--approved-by",
            "rehearsal",
            "--reason",
            "gates green",
        )
        require("cut over" in output, f"cutover did not complete:\n{output}")

        position = _first_match(output, r"lsn=(\d+)")
        require(position is not None, f"no source position recorded:\n{output}")

        window = gantry("migration", "window", str(MIGRATION_SPEC))
        require("holding" in window, f"the window did not open:\n{window}")
        require("source authoritative" in window, f"authority unclear:\n{window}")
        proved.append(f"cut over at lsn={position}, window holding, source authoritative")


def migration_rollback(args: argparse.Namespace) -> None:
    with step(7, "divergence is reported, not acted on; rollback is on command") as proved:
        drifted = _mid_key()
        _break_target(f"UPDATE public.orders SET amount = amount + 1 WHERE order_id = {drifted}")

        window = gantry("migration", "window", str(MIGRATION_SPEC))
        require("diverged" in window, f"divergence went unnoticed:\n{window}")
        require("your call" in window, f"the window did not defer the decision:\n{window}")
        require(
            _migration_state() == "rollback_window",
            "reporting divergence moved the workflow; it must only report",
        )

        _break_target(f"UPDATE public.orders SET amount = amount - 1 WHERE order_id = {drifted}")
        rolled = gantry(
            "migration",
            "rollback",
            str(MIGRATION_SPEC),
            "--decided-by",
            "rehearsal",
            "--reason",
            "checkout errors spiked",
        )
        require("rolled back" in rolled, f"rollback did not happen:\n{rolled}")
        require("no data moved back" in rolled, f"rollback was not traffic-only:\n{rolled}")
        proved.append("divergence reported without acting; rolled back from the cutover position")


def migration_finalize(args: argparse.Namespace) -> None:
    with step(8, "re-cut over, wait out the window, finalize, audit") as proved:
        _clear_migration(MIGRATION_NAME)
        _clear_operation(MOVEMENT_NAME)
        gantry("migration", "start", str(MIGRATION_SPEC), "--backend", args.backend)
        gantry(
            "migration",
            "cutover",
            str(MIGRATION_SPEC),
            "--approved-by",
            "rehearsal",
            "--reason",
            "second attempt",
        )

        early = gantry("migration", "finalize", str(MIGRATION_SPEC), expect=1)
        require("cannot finalize" in early, f"finalize did not refuse an open window:\n{early}")

        # The spec's window is seconds, so waiting it out is honest rather than
        # simulated. Its duration is policy; the mechanism is what is on trial.
        time.sleep(6)

        final = gantry("migration", "finalize", str(MIGRATION_SPEC))
        require("completed" in final, f"finalize did not complete:\n{final}")

        audit = gantry("migration", "audit", MIGRATION_NAME)
        for decision in ("cutting_over", "rollback_window", "completed"):
            require(decision in audit, f"the audit lost {decision}:\n{audit}")
        # Tokens, not the joined phrase. Rich wraps to the terminal width, so
        # "operator (rehearsal)" can arrive split across two lines — the second
        # time rendering has broken a check in this script, after markup
        # swallowing a bracketed subject on Day 3.
        require("operator" in audit, f"no operator decision in the trail:\n{audit}")
        require("rehearsal" in audit, f"the approver is not in the trail:\n{audit}")
        decisions = audit.count("->")
        proved.append(f"finalize refused early then completed; audit holds {decisions} decisions")


def _mid_key() -> int:
    """A key that exists, in the middle of the range.

    Middle rather than an end so the drill-down actually has to halve: a row
    at the boundary can be found by a search that is not a binary one.
    """
    value = psql(TARGET_URL, "SELECT (min(order_id) + max(order_id)) / 2 FROM public.orders")
    return int(value.strip() or 1)


def _target_rows() -> int:
    return int(psql(TARGET_URL, "SELECT count(*) FROM public.orders").strip() or 0)


def _migration_state() -> str:
    return psql(META_URL, f"SELECT state FROM migrations WHERE name = '{MIGRATION_NAME}'").strip()


def _int_after(text: str, pattern: str) -> int | None:
    match = re.search(pattern, text, re.IGNORECASE)
    return int(match.group(1).replace(",", "")) if match else None


def _first_match(text: str, pattern: str) -> str | None:
    match = re.search(pattern, text)
    return match.group(1) if match else None


V1_STAGES: tuple[Callable[[argparse.Namespace], None], ...] = (
    seed_and_register,
    # Before the Movement, not after: the chaos suite clears every
    # Operation, which would take the Movement Result that step 6
    # traces a finding back to.
    faults,
    move,
    corrupt_and_repair,
    analysis,
    provenance,
    agent_access,
)

# The Migration suite reuses v1's seeding and then drives the workflow. It is
# a separate sequence rather than more steps on the end, because it proves a
# different claim: v1 proves the guarantees, this proves the workflow composed
# over them.
MIGRATION_STAGES: tuple[Callable[[argparse.Namespace], None], ...] = (
    migration_prepare,
    migration_snapshot,
    migration_localise,
    migration_gates,
    migration_cutover,
    migration_rollback,
    migration_finalize,
)


def _stages(suite: str) -> tuple[Callable[[argparse.Namespace], None], ...]:
    if suite == "v1":
        return V1_STAGES
    if suite == "migration":
        # Seeding still has to happen; the workflow needs data to move.
        return (seed_and_register, *MIGRATION_STAGES)
    return (*V1_STAGES, *MIGRATION_STAGES)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the v1 rehearsal end to end.")
    parser.add_argument(
        "--rows",
        type=int,
        default=2_000_000,
        help=(
            "Orders the source should hold. Seeding tops up rather than replacing, "
            "so this cannot shrink a source that already has more. The M1 "
            "measurement on Day 10 used 100,000,000."
        ),
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=1_000_000,
        help="Customers to seed. The crash test partitions these, so it needs a million.",
    )
    parser.add_argument(
        "--suite",
        default="v1",
        choices=("v1", "migration", "all"),
        help="Which sequence to run: the v1 guarantees, the Migration workflow, or both.",
    )
    parser.add_argument("--registry", type=Path, default=ROOT / ".gantry" / "rehearsal.json")
    parser.add_argument("--backend", default="temporal", choices=("temporal", "queue"))
    parser.add_argument("--skip-seed", action="store_true", help="Reuse the data already there.")
    parser.add_argument(
        "--reset", action="store_true", help="Truncate source and target before seeding."
    )
    parser.add_argument("--skip-chaos", action="store_true", help="Skip the fault-injection step.")
    parser.add_argument(
        "--verify-failure-exit",
        type=int,
        default=1,
        help="Exit code `gantry verify` uses when verification fails.",
    )
    args = parser.parse_args(argv)
    args.registry.parent.mkdir(parents=True, exist_ok=True)

    label = {"v1": "v1", "migration": "migration workflow", "all": "v1 + migration"}[args.suite]
    print(
        f"{BOLD}gantry {label} rehearsal{OFF}  {DIM}{args.rows:,} rows, {args.backend} backend{OFF}"
    )
    try:
        for stage in _stages(args.suite):
            stage(args)
    except RehearsalError as failure:
        print(f"\n{RED}rehearsal failed{OFF}\n{failure}", file=sys.stderr)
        if TIMELINE.steps:
            print(f"\n{TIMELINE.render()}", file=sys.stderr)
        return 1

    print(f"\n{BOLD}rehearsal clean{OFF}\n\n{TIMELINE.render()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
