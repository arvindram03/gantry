# RFC 0000 — Gantry

The canonical text lives in [../gantry-spec.md](../gantry-spec.md).

## Revisions

- **rev 1** — Dataset added as the fourth core resource (the Core Resource Model
  said "four" but listed three). Section 9 split into a generic Operation
  lifecycle state machine with Migration workflow states layered above it.

## Open questions

The spec's section 23 open questions are tracked as issues. Two are answered
provisionally for v1 in [execution-plan-v1.md](../execution-plan-v1.md):

- **#1 Temporal** — deferred. v1 ships a Postgres leased queue behind a
  `WorkflowBackend` interface. Revisit at the Day 5 review.
- **#2 CDC offset ownership** — Gantry owns the applied-position checkpoint in
  its own metadata store. Kafka consumer offsets are a transport detail;
  correctness must not depend on connector bookkeeping.
