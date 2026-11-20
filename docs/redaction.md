# Redaction

Some columns should never reach an agent, even from a table it may read: an
email, a phone number, a salary. A `Redaction` names them, and a governed
query masks them at their source, so the rows come back and the value does
not.

```python
from gantry.sql.redaction import Redaction

redaction = Redaction(
    {
        "public.users.email": "hash",  # a stable token: h_3f9a0c...
        "*.*.phone": "hide",  # [redacted]
        "hr.staff.salary": "hide",
    },
    key=os.environ["REDACTION_KEY"],  # 16 to 128 letters, digits, - or _
)

query = db.query(schemas=["public", "hr"], redaction=redaction)
write = db.write(destinations=["public.*"], redaction=redaction)
build = db.materialize(sources=["public.*"], destinations=["scratch.*"], redaction=redaction)
```

Install the parser it needs with `pip install 'data-gantry[redaction]'`.

## The two modes

| Mode | The agent sees | Still works |
|---|---|---|
| `hide` | `[redacted]` where it is returned; NULL inside the statement | nothing on that column |
| `hash` | `h_` and 16 hex digits, keyed with `key` | joins, `count(DISTINCT ...)`, `GROUP BY` |

The same value always gives the same token, and without the key a guess
cannot be hashed to check it. Use `hash` for identifiers. On a column with
few distinct values, a plan or a zip code, a token is as good as the value.

## Masked at the source

Masking the result by column name would be undone by `SELECT email AS e`,
`lower(email)` or `WHERE email LIKE 'a%'`. So each statement is rewritten
before it runs. Every column it reads is resolved to its table, `*` included,
through CTEs, derived tables, joins, unions and correlated subqueries, and
each redacted one is replaced by its mask wherever it appears. A WHERE,
ORDER BY or GROUP BY on a redacted column works on the mask.

The rewrite is never recorded. The run keeps the statement as it was
proposed, and the key is removed from any error the database returns.

## What is refused

Anything the rewrite cannot follow is refused, not guessed at:

- a statement the parser cannot read, or more than one;
- a table the connection's `describe()` does not list, such as `pg_stats`;
- a reference to a whole row: `row_to_json(u)`, `u::text`, `SELECT u`;
- a function the parser does not model, such as `query_to_xml`, which runs
  SQL given as a string.

## Writes and materializations

A write finds rows by the token a query showed. An agent that read
`h_3f9a0c41b7e2d815` for someone's email can, in that call or any later one:

```sql
UPDATE public.users SET plan = 'pro' WHERE email = 'h_3f9a0c41b7e2d815'
UPDATE public.users SET email = 'new@example.com' WHERE email = 'h_3f9a0c41b7e2d815'
```

The WHERE is rewritten as a query's is, so the column's mask is compared
with the token inside the database and the value never comes out. A real
value in its place matches nothing, and without the key a token cannot be
made for a guess. A token belongs to the value: once the email changes, the
person has a new one.

Refused: copying a redacted value anywhere it would not be redacted (a SET
value, what an `INSERT ... SELECT` inserts, an explicit `RETURNING email`),
and finding rows by a hidden column, which has no token; use the row's id.
Rows a write returns, and the rows of a `dry_run` preview, show the written
table's redacted columns as `[redacted]`.

A materialization that reads a redacted column is refused. What it created
would hold the column, or for a view, the key.

## Document stores

MongoDB takes the same `Redaction`, named `database.collection.field`; a nested
field is a dotted path:

```python
redaction = Redaction(
    {"shop.customers.email": "hash", "shop.customers.address.street": "hide"},
    key=os.environ["REDACTION_KEY"],
)
query = db.query(collections=["customers", "orders"], redaction=redaction)
```

Nothing is parsed. The engine puts its own `$set` in front of the agent's
pipeline, replacing each redacted field with its mask, so every stage the
agent wrote (`$match`, `$group`, `$project`, `$$ROOT`) sees only the mask. A
`$lookup` into a collection with redacted fields gets the same `$set` at the
head of its own pipeline. A null or missing field stays so, and a nested field
whose parent is not an object is left alone.

`hash` uses MongoDB's `$toHashedIndexKey` over the key and the value, so it
needs MongoDB 7.0 or later; tokens look like `h_1617972135505165306`. A field
holding an object or an array can be hidden but not hashed. The database and
collection may be patterns; the field is named exactly, since a document store
has no list of fields to match against. `describe_fields(collection)` lists
what a sample of documents holds, to choose from.

A materialization from a collection with redacted fields is refused.

## What it does not cover

A view in the database that exposes a redacted column under another name is
a different column. Name it as well, or leave the view out of the schemas the
connection reads. `SQLWrite.check`, which answers without the database,
does not know about redaction; the write itself and `dry_run` do.
