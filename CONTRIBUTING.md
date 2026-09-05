# Contributing

## Setup

```bash
make install    # dependencies and pre-commit hooks
make dev-up     # postgres x3, kafka, debezium, temporal, prometheus
uv run alembic upgrade head
```

The local stack binds high ports (154xx, 19xxx) deliberately, so it does not
collide with a PostgreSQL or Kafka you already run.

## Before you push

```bash
make check      # lint, strict typecheck, unit tests — what CI runs
make test-int   # integration tests, needs the stack
```

`make test-chaos` runs the fault-injection suite, including a real `kill -9`
against a live worker. It is worth running before touching anything in the
scheduler, the target adapter, or the checkpoint store.

## What the tests are for

Unit tests state the rules. Integration tests prove them against real
PostgreSQL, real Kafka and real Debezium — not simulations, because the bugs
worth catching here are the ones a simulation would reproduce incorrectly.

**A test that skips when its fixture is missing is a test that has stopped
running.** The Analysis tests used to skip when the scenario tables were
absent, which meant they were silently uncovered on every machine but one. They
now seed what they need. If you find yourself writing `pytest.skip`, ask
whether the test can build its own fixture instead.

**Name the behaviour, not the function.** `test_a_killed_worker_loses_nothing`
says what would break. `test_run_worker` does not.

## Style

Line length 100, `ruff` for both format and lint, `mypy --strict` over
`gantry`, `tests` and `scripts`. Run `make fmt` and let the tools decide the
formatting arguments.

Comments explain **why**, especially where the obvious approach was tried and
rejected — several places in this codebase carry a note about the version that
looked right and was wrong, because that note is the only thing preventing the
next person from putting it back.

## Adding a guarantee

If a change makes a new promise about correctness, it needs three things:

1. A test that fails without it, against real infrastructure where the promise
   is about real infrastructure.
2. An entry in [docs/guarantees.md](docs/guarantees.md) saying what is now
   guaranteed and — the part more likely to be skipped — what is still not.
3. A statement of the residual gap where one exists. The group-of-one gap in
   the access ladder is documented rather than hidden; that is the standard.

## Adding an adapter

Implement the `Protocol` in `gantry/adapters/<kind>/base.py`. There is no base
class and no registration step. [docs/adapters.md](docs/adapters.md) lists what
an adapter must not do — interpret a position it did not produce, checkpoint on
its own, report unconfirmed success, or skip the ordering guarantee for speed.

## Changing the metadata schema

```bash
uv run alembic revision --autogenerate -m "what changed"
uv run alembic upgrade head
uv run alembic check          # must report no new operations
```

Every table that records a decision is append-only. Evidence that can be
rewritten is not evidence.

## Changing a spec format

Spec parsing lives entirely in `gantry/spec/`, separate from the domain models
in `gantry/core/`. A YAML spelling change should not reach past that boundary.
`gantry schema show` emits JSON Schema for each kind.

## Commits

Explain the change and the reasoning. If something surprised you — a wrong
first attempt, a behaviour that was not what the documentation implied — put it
in the message. That is the part that is expensive to rediscover.
