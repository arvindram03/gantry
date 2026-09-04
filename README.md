# Gantry

**Open-source reliability and execution layer for data movement and analysis.**

Gantry sits above the transport and processing infrastructure you already run. It does not
own the bytes on the wire — it owns the execution contract around them: checkpoints, replay,
ordering boundaries, idempotency, verification, policy, provenance and recovery.

> Agents may plan and re-plan. Deterministic infrastructure enforces guarantees.

## The model

Four core resources:

```text
Dataset        addressable logical data unit + manifest
   │
   ├── Movement    make or keep a Dataset reliably available
   │
   └── Analysis    compute over one or more Datasets
                    │
                    ▼
                 Result    bounded, structured, provenanced output
```

Movement and Analysis are both Operations over Datasets, and both traverse one lifecycle
under a single guarantee boundary:

```text
Plan → Generate → Validate → Execute → Verify → Result
```

An engine reporting `SUCCESS` is not a correct result. Verification decides.

## Status

Pre-alpha, under active development toward `v0.1.0`. See
[docs/execution-plan-v1.md](docs/execution-plan-v1.md) for the build order and
[docs/gantry-spec.md](docs/gantry-spec.md) for the full design (RFC 0).

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
make install    # dependencies + pre-commit hooks
make dev-up     # local stack: postgres x3, kafka, debezium, prometheus
make check      # lint, strict typecheck, unit tests
make test-int   # integration tests against the stack
```

`make help` lists every target.

## License

Apache-2.0
