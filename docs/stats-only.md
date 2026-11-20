# Stats only

Some people should see what a database says in aggregate, how many users
went inactive, revenue by plan, signups by week, without ever seeing a row.
`StatsOnly` lets an agent ask in its own SQL and answers only numbers about
groups, each computed over enough records that it cannot be traced to one
person.

```python
from gantry.sql.stats_only import StatsOnly

query = db.query(schemas=["public"], stats_only=StatsOnly(min_group_size=10))

await query("SELECT plan, count(*) FROM public.users WHERE NOT active GROUP BY plan")
```

Install the parser it needs with `pip install 'data-gantry[redaction]'`.

## What is answered

Every column the outer SELECT returns is a GROUP BY key or `count`,
`count(DISTINCT ...)`, `sum`, `avg`, `stddev` or `variance` of a plain column.
Group keys can be any expression (`date_trunc('month', placed_at)`), WHERE and
ON take any condition, joins are allowed, and subqueries and CTEs work as
filters (`NOT EXISTS`, `IN (SELECT ...)`).

Refused before anything runs:

- a plain column that is neither grouped nor aggregated, or `*`;
- `min`, `max`, `string_agg` and the like, which return a row's own value;
- a condition inside an aggregate, `sum(CASE WHEN id = 5 THEN salary END)`:
  put it in WHERE or GROUP BY, where it shrinks the group that is counted;
- window functions, `FILTER`, a subquery among what is returned;
- a subquery, CTE or function in the outer FROM: what is counted is tables.

## The minimum

For every group the answer has, the statement is extended to count how many
distinct records of each table in the outer FROM it came from: `count(*)` for
one table, `count(DISTINCT <primary key>)` per table in a join, read with
`describe()`. A join that repeats one record a thousand times is still a
group of one. A table in a join with no primary key is refused.

If any group came from fewer records than `min_group_size`, the whole answer
is refused, and the run records why. The counts themselves are taken out of
an answer that is returned. A filter down to one person is a group of one; a
filter matching nobody is a group of none. `min_group_size=0` checks the
statement and not the sizes.

With `redaction=` as well, the guarded statement is redacted in turn: a hashed
key still counts each record once.

## What it does not stop

Two answers over overlapping groups, each large enough, can still differ by a
small group: everyone, minus everyone but one. Every query is a recorded run,
so a series of near-identical questions is there to be seen. Keep the minimum
higher where that matters.
