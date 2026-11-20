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

A write may set a redacted column (`SET email = 'new@example.com'`) but not
read one: `SET notes = email` would copy it out, and `WHERE email = ...`
would test it. Rows it returns, and the rows of a `dry_run` preview, show the
written table's redacted columns as `[redacted]`.

A materialization that reads a redacted column is refused. What it created
would hold the column, or for a view, the key.

## What it does not cover

A view in the database that exposes a redacted column under another name is
a different column. Name it as well, or leave the view out of the schemas the
connection reads. `SQLWrite.check`, which answers without the database,
does not know about redaction; the write itself and `dry_run` do.
