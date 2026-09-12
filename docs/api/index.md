# API reference

Generated from the source, so it cannot drift from the code. Every signature
here is the real one — the package is checked with `mypy --strict` and ships a
`py.typed` marker, so your own type checker sees these types too.

## The surface a caller touches

| | |
|---|---|
| [SQL](sql.md) | `connect`, the connection, `.query(...)`, `.tool()`, and `SQLPolicy` |
| [Results and outputs](results.md) | what an operation hands back: `inline`, `outputs`, `uri`, status, metrics |
| [Failures](failures.md) | the normalized failure taxonomy, and which kinds are retryable |
| [Handles and execution](execution.md) | reconnecting to work in flight, polling it, cancelling it |
| [Batch and stream jobs](jobs.md) | `gantry.batch` and `gantry.stream` |
| [Capability matrix](capabilities.md) | which policy fields each provider can actually enforce |

## The shape of every operation

Configuration and enforcement are separated on purpose. Application code holds
the credential and decides policy and trusted checks; the agent receives a
tool whose inputs are the SQL and an optional allowlisted verification
commitment.

```python
import gantry

db = gantry.sql.connect("postgres", url=...)  # application code
query = db.query(read_only=True, schemas=("analytics",))  # application code
tool = query.tool()  # what the agent gets
```

`tool.input_schema` contains `sql` and `verify`. The latter exposes only
declarative checks supported by that operation/provider. Policy and trusted
checks are not arguments a model can pass, so the model cannot widen authority
or weaken the application acceptance contract.
