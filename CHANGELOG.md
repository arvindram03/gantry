# Changelog

Notable changes. Dates are release dates; the format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
