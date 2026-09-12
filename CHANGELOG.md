# Changelog

Notable changes. Dates are release dates; the format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added

- **Verification evidence.** Every governed run now carries a serializable
  record of what Gantry observed while deciding: `result.evidence`. Three
  sources kept apart because they are trusted differently — the engine's
  account of its own execution, measurements Gantry took at the destination,
  and what the control plane was configured to require. `EvidenceBundle`,
  `Observation` and `ObservationSource` are on the top-level namespace.
- **Durable run records.** `gantry.runs.record(result.evidence)` persists a
  run; `gantry.runs.get(run_id)` reads it back in another process, and
  `run.render()` lays it out for a person. SQLite-backed by default, in memory
  until a caller configures a store, because writing a file into someone's
  working directory on import is not a default worth having.
- **`db.query(verify=...)` takes the same checks as `db.materialize(...)`.** The
  `gantry.verify` library is evaluated against the rows a query returned,
  described as a table, so `row_count` means one thing on both paths. Query
  results carry `evidence` in the same shape too. Two checks cannot mean the
  same thing and say so instead of guessing: `destination_exists` on a query,
  and any count-based check on a result truncated by `max_rows`, both report
  `UNSUPPORTED_VERIFICATION`.
- `gantry.verify.null_rate(column=..., max=...)`, measured at the destination
  by the provider. This is the check `row_count` cannot stand in for: a query
  that runs, produces the expected number of rows, and joins wrongly, so the
  column everything downstream keys on is null in most of them.
- `CheckResult.source` and `CheckResult.supported`, so a check says where its
  observation came from and whether it could be evaluated at all.

### Changed

- A check the provider cannot evaluate now fails as
  `FailureKind.UNSUPPORTED_VERIFICATION` rather than an ordinary verification
  failure. Both reject — an unmeasured bound is not a bound — but "I could not
  measure this" is a gap in the provider and "I measured it and it was wrong"
  is a problem with the data, and they call for different fixes.
- `VerificationResult` exposes `failed_checks` and `unsupported_checks`.

- **Materialization for PostgreSQL, Neon and Supabase.** `db.materialize(...)`
  was refused on those providers with "adapter does not support CREATE TABLE
  AS" — not because PostgreSQL lacked anything, but because the adapter never
  declared the capabilities or implemented `inspect_table`. It does both now,
  for tables and views.

  `CREATE TABLE AS` is validated with `EXPLAIN`, which does not execute it.
  `EXPLAIN CREATE VIEW` is a syntax error, so a view is validated by running
  its definition in a transaction and rolling back — every name resolves, and
  nothing is left behind. PostgreSQL's `statement_timeout` already covers
  writes, unlike MySQL's, and its DDL is transactional, so a timed-out
  materialization leaves no half-built destination.
- **MySQL as a SQL provider** (#13): `gantry.sql.connect("mysql", url=...)`, with
  `query` and `materialize` behind the same contract as every other provider.
  `pip install "data-gantry[mysql]"`.

  Three things are MySQL rather than PostgreSQL-with-different-spelling, and
  each was measured against MySQL 8.4 rather than assumed. `max_execution_time`
  bounds `SELECT` and nothing else — a `CREATE TABLE ... AS SELECT` ran to
  completion under a 200ms limit — so the timeout is enforced by the server
  variable for reads and by a deadline plus `KILL QUERY` for everything else;
  a timed-out materialization leaves no half-built destination. `EXPLAIN`
  cannot describe DDL, so native validation uses `PREPARE`/`DEALLOCATE`, which
  resolves a statement without running it. And a schema is a database, with
  `table_catalog` always the literal `def`, so Gantry reports no catalog rather
  than inventing one.
- `gantry.sql.MySQLDialect`, because MySQL's lexing differs where it matters:
  backslashes escape inside ordinary strings, so `'a\'; SELECT 2'` is one
  statement there and two under PostgreSQL's rules, and there is no
  dollar-quoting. `ConservativeDialect` now takes those two rules as options.
- `examples/mysql_customers.py` and `examples/mysql/docker-compose.yml`: an
  agent with two tools against MySQL, four of whose six statements are refused.
- CI runs a real MySQL alongside the real PostgreSQL, so
  `tests/test_mysql_live.py` executes rather than skipping.
- An API reference for the public surface, published to GitHub Pages from
  `mkdocs.yml` and the pages under `docs/api/`. Signatures and types are
  generated from the source by `mkdocstrings`, so the reference cannot drift
  from the code; `mkdocs build --strict` fails on a dead link or an
  unresolvable reference, and runs on every pull request.
- Docstrings for every symbol in `gantry.__all__` and `gantry.sql.__all__`,
  which the reference renders. A docstring inherited from `Exception` or
  `Protocol`, or generated by `dataclass` from the signature, does not count:
  both render as prose that tells a reader nothing.
- `tests/test_api_reference.py` guards the reference against the two gaps a
  docs build cannot see — a symbol on no page, and a symbol whose only
  documentation is its own signature. The first published version of the
  reference had both: `submit`, `wait` and `run` were on no page at all, and
  42 of 75 exported symbols rendered without prose.
- The capability matrix in the reference is generated from each adapter's
  declared `SQLCapabilities` by `scripts/capability_matrix.py`. Capabilities
  that depend on how a connection was opened are rendered as the condition
  ("read-only conn" / "writable conn") rather than as a flat yes or no.

### Changed

- The coverage floor is enforced in CI rather than only measured: `pytest`
  fails below 77%.
- CI runs the type check and test suite against both supported interpreters,
  3.12 and 3.13, rather than one unpinned one. `mypy`'s `python_version` pin is
  removed so each leg checks under its own interpreter's semantics.

### Security

- `SELECT ... INTO new_table` is no longer classified as a read. It creates a
  table — PostgreSQL refuses it in a read-only transaction — and a read-only
  policy admitted it (#9).
- `SELECT ... FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE` and `FOR KEY SHARE`
  are no longer classified as reads. The locks are writes as far as the engine
  is concerned.
- `SELECT nextval(...)`, `setval(...)` and `dblink_exec(...)` are no longer
  classified as reads. The list is a floor and not a boundary — a function's
  body is not in the text — but the gap is narrower than it looks: a read-only
  session cannot create the function it would need, and an ordinary in-database
  write is refused inside the read-only transaction even from a
  `SECURITY DEFINER` function. What escapes is an effect that lands outside the
  transaction, which is why `dblink_exec` is on the list: writing to another
  server is permitted by the read-only transaction, and the row survives the
  rollback.
- Comments are blanked before the keyword scan, so `SELECT ... FOR /* x */
  UPDATE` is recognised as the locking clause it is. The words inside a comment
  no longer reach classification either way, so a comment can neither hide a
  keyword nor invent a table reference. Found by the new property tests.

### Added

- An adversarial corpus for SQL classification (#9): `tests/test_sql_corpus.py`
  asserts `(operation, read_only, statement_count)` over comment obfuscation,
  writing CTEs, quoting, unicode and procedural forms, with one rule — a write
  must never classify as `read_only=True`.
- `tests/test_sql_properties.py` generates the cases nobody thought of with
  Hypothesis: obfuscation never turns a write into a read, a write beside a
  read is never a read, and arbitrary text never crashes the classifier.
- `tests/test_sql_corpus_live.py` measures the corpus's ground truth against a
  real PostgreSQL rather than trusting it. A read-only transaction is the
  engine's own answer to "does this write", and it corrected two entries that
  were wrong by inspection. It also asserts that Gantry *refuses* each write at
  admission — `REJECTED`, before submission — rather than that the statement
  merely failed, which the read-only transaction would satisfy on its own.
- `SQLClassification.read_only_reason` says why a `SELECT` is not a read, so a
  refusal reports the `FOR UPDATE` clause or the writing function instead of
  "SELECT is not allowed by read-only policy".

### Fixed

- The statement splitter treats PostgreSQL dollar-quoted strings as opaque, so
  a `;` inside a `$$...$$` or `$tag$...$tag$` body no longer splits one
  statement into several and gets it refused as a batch (#7). Dollar-quoting is
  recognised only where a token can begin: `my$tab$le` is a single legal
  identifier in PostgreSQL, so `SELECT * FROM my$tab$le; DROP TABLE victim`
  stays two statements rather than reading as one read-only `SELECT`.
- The statement splitter honours backslash escapes inside PostgreSQL `E''`
  strings, so `E'O\'Brien; x'` is one statement rather than two. Backslashes
  stay literal in ordinary strings, matching `standard_conforming_strings`, and
  an `E` at the end of an identifier is not treated as a string prefix.
- Ship `gantry/py.typed`. Without it PEP 561 requires type checkers to ignore
  the installed package, so every Gantry symbol resolved to `Any` downstream
  and the `Typing :: Typed` classifier was a claim the wheel did not honour.

## 0.5.0 — 2026-09-09

First release published to PyPI, as `data-gantry`.

### Changed

- **The distribution is now `data-gantry`.** The import stays `gantry`. The name
  `gantry` on PyPI belongs to an unrelated project, so `pip install gantry`
  installs someone else's library.

### Added

- `gantry.batch` and `gantry.stream` as intent-named entry points for Flink SQL
  jobs; `gantry.flink` is now an implementation detail.
- Seven runnable examples in `examples/`, indexed by task, with a Docker stack
  (`examples/stack/docker-compose.yml`) that brings up PostgreSQL, Flink, and
  the catalog they read through.
- Live test suites against real PostgreSQL and a real Flink SQL Gateway. They
  skip when no engine is reachable, so a clone without one still passes.
- A release workflow that publishes to PyPI on a version tag through trusted
  publishing, and refuses to publish when the tag disagrees with the packaged
  version.
- `SECURITY.md`, stating that Gantry is an admission boundary rather than a
  replacement for database permissions.

### Fixed

- Every statement carried an `executionTimeout` that Flink's SQL Gateway
  rejects outright, so validation, submission and execution all failed against
  a real gateway.
- Health checks read metrics from an object the adapter never populates, so any
  check needing a restart count or watermark lag could not pass.
- Query results were read one page deep and positionally, so `COUNT(*)`
  returned `1` for any non-empty table.
- Identifier normalisation split on dots inside quotes and was applied to a
  job's output but not its inputs, making every table outside `public`
  unusable.
- `PostgresAdapter.describe()` selected `table_type` from
  `information_schema.columns`, where that column does not exist.
- Waiting for a streaming job decided its health contract before Flink had
  registered the job's metrics, and did not treat `SUCCEEDED` as terminal.
