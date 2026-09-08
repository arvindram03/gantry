# Contributing

Gantry v0 is intentionally small. Changes should preserve the single lifecycle:

```text
propose → constrain → execute → verify → accept
```

## Setup

```bash
uv sync
```

## Checks

```bash
make check
```

This runs formatting checks, linting, strict type checking, unit tests, coverage, and a package
build. Use `make fmt` to apply Ruff's formatter and safe lint fixes.

## Design guidelines

- Keep the core free of runtime dependencies and framework-specific behavior.
- Add integrations as separate packages rather than teaching the runtime about data engines.
- Treat every artifact as an untrusted proposal.
- Keep policy descriptive and make executors responsible for enforcement.
- Do not equate successful execution with a verified result.
- Fail closed when a plugin raises or violates its result contract.
- Prefer protocols and plain data objects over inheritance and registration systems.

Tests should name the behavior protected by the trust boundary and cover where the lifecycle
must stop after each failure.
