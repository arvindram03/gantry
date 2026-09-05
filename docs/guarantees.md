# Guarantees

What the runtime promises, what it does not, and where the edges are. This file
grows as capabilities land; sections marked *not yet* are honest gaps rather
than omissions.

## Checksum normalisation

Chunk checksums compare a rendering of each row, so two databases holding
identical data must render it identically. They frequently do not. These are
the rules Gantry applies, and the reasoning behind each — this is the part most
likely to need extending when a new column type appears.

| Type | Rendering | Why |
|---|---|---|
| `numeric`, `decimal` | `trim_scale(v)::text` | `1.50` and `1.5` are the same number and different strings |
| `real`, `double precision` | `round(v::numeric, 10)::text` | The last bits of a double are not meaningful data |
| `timestamp with time zone` | `to_char(v AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US')` | Rendering otherwise depends on the session's `TimeZone` |
| `timestamp` | same, without conversion | Explicit format rather than the session's `DateStyle` |
| `date` | `to_char(v, 'YYYY-MM-DD')` | As above |
| `boolean` | `v::int::text` | `t`/`f` versus `true`/`false` varies by client |
| `bytea` | `encode(v, 'hex')` | Independent of the `bytea_output` setting |
| everything else | `v::text` | |

Two further rules apply to every column:

- **NULL becomes `\N`.** A NULL rendered as nothing is indistinguishable from an
  empty string, and a NULL inside a concatenation makes the entire row NULL —
  both silently.
- **Columns are joined with `\x1f`** (ASCII unit separator), so `("ab", "c")`
  and `("a", "bc")` do not render identically.

**Known gaps.** Composite types, ranges, `json` (as opposed to `jsonb`, whose
key order is already normalised by PostgreSQL), and arrays of any of these fall
through to `v::text` and may therefore compare unequal across databases that
render them differently. A checksum mismatch on one of these types is worth
investigating as a normalisation gap before it is treated as data corruption.

### Why sum, not XOR

The checksum sums per-row hashes rather than XOR-ing them. XOR cancels
duplicates: a row written twice would XOR to the same value as no rows at all,
which is invisible to precisely the check meant to catch a duplicated write.
Summing is order-independent in the same way and does not have that property.

Each per-row term is 60 bits so it fits a signed `bigint`, and PostgreSQL's
`sum()` over `bigint` returns `numeric`, so the total cannot overflow.

### Row counts travel with checksums

A checksum alone cannot distinguish an empty range from one whose hashes happen
to cancel. Every comparison carries the row count with it.

## Mismatch localisation

A checksum says a range disagrees; halving it says which half. Locating one
corrupted row among ten million takes **27 comparisons**, not ten million.

Drilling stops when a range is small enough to enumerate (2,000 rows by
default), and only then does the runtime read individual keys — at which point
it can distinguish a missing row from an extra one from a differing one.

**Limitation:** halving requires a numeric single-column key. A composite or
non-numeric key falls back to enumerating the range, which is correct but not
sub-linear.

## Repair

A failed checksum means one partition is wrong, not that the migration is.
`gantry repair <spec> <partition>` re-copies that partition alone and
re-verifies only it. This is safe because the copy is idempotent: re-running a
partition restores exactly the rows that are missing or stale and touches
nothing else.

## Not yet

- CDC ordering and stale-write rejection (Days 13–15)
- Cutover gates and approvals (v1.1)
- Analysis verification: row expansion, join coverage, temporal alignment (Day 18)
