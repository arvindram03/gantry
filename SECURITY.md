# Security

## Reporting a vulnerability

Report privately through
[GitHub security advisories](https://github.com/arvindram03/gantry/security/advisories/new)
rather than a public issue. Please include what you did, what happened, and the
version or commit you were on.

## What Gantry is and is not, for security purposes

Gantry is an **admission boundary in front of an engine**, not a security
boundary of its own. That distinction matters when judging a report:

- **Database roles, IAM, and scoped credentials remain the primary control.**
  Policies here are an additional check, not a substitute for permissions. A
  connection handed to Gantry can do whatever that connection is allowed to do.
- **Read-only is enforced by the engine**, not merely asserted. PostgreSQL,
  Neon and Supabase use a read-only transaction; DuckDB requires the database to
  be opened read-only; BigQuery checks the dry-run statement type; Snowflake
  requires a genuinely read-only role. An adapter that cannot enforce it refuses
  the query rather than trusting itself.
- **SQL classification is conservative and advisory.** Anything it cannot parse
  is `UNKNOWN` and refused under a read-only policy. Treat a classification
  bypass as a real finding: the intended behaviour is to fail closed.

Findings we would very much like to hear about:

- A statement admitted under a read-only policy that can write.
- A way to reach a table outside `allowed_schemas`, or one named in
  `denied_tables`.
- Anything that puts a credential, a connection string, or a raw connection
  where an agent tool can reach it.
- A result reported as verified when its checks did not pass.

## Untrusted input

The SQL an agent writes is untrusted by design, and that is the threat this
project is built around. The *policy* is trusted: it is application code, and
anything that lets a model influence its own policy is a vulnerability.
