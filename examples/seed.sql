-- A small but realistically shaped warehouse for the examples.
--
--   psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
--
-- Runs on local PostgreSQL, Neon, and Supabase alike: plain SQL, no
-- extensions. generate_series does the work server-side, so seeding a remote
-- database is fast.
--
-- Three schemas, because the separation is what the examples' policies are
-- about:
--
--   analytics       what an agent may read
--   analytics_pii   what it may not — a real table, deliberately reachable,
--                   so the denial is doing something rather than describing
--                   a table that does not exist
--   reporting       where verified results are published
--   agent_scratch   where an agent may build its own tables

CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS analytics_pii;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS agent_scratch;

-- ---------------------------------------------------------------- customers

DROP TABLE IF EXISTS analytics.customers CASCADE;
CREATE TABLE analytics.customers (
    customer_id bigint PRIMARY KEY,
    plan        text        NOT NULL,
    region      text        NOT NULL,
    signed_up   date        NOT NULL
);

INSERT INTO analytics.customers
SELECT g,
       (ARRAY['free','team','business','enterprise'])[1 + (g % 4)],
       (ARRAY['emea','amer','apac'])[1 + (g % 3)],
       DATE '2024-01-01' + ((g * 7) % 900)
FROM generate_series(1, 5000) g;

-- The table an agent must not read. Real rows, real column names.
DROP TABLE IF EXISTS analytics_pii.customer_contacts;
CREATE TABLE analytics_pii.customer_contacts (
    customer_id bigint PRIMARY KEY,
    email       text NOT NULL,
    full_name   text NOT NULL
);

INSERT INTO analytics_pii.customer_contacts
SELECT g, 'user' || g || '@example.invalid', 'Customer ' || g
FROM generate_series(1, 5000) g;

-- ------------------------------------------------------------------- orders

DROP TABLE IF EXISTS analytics.orders CASCADE;
CREATE TABLE analytics.orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint        NOT NULL,
    region      text          NOT NULL,
    channel     text          NOT NULL,
    status      text          NOT NULL,
    amount      numeric(12,2) NOT NULL,
    -- `timestamp` and not `timestamptz`: Flink's JDBC connector refuses
    -- TIMESTAMP_LTZ, so a timestamptz column makes this table unreadable from
    -- the Flink examples. Store UTC and say so, which is what you would end up
    -- doing anyway once a pipeline needs the column.
    placed_at   timestamp     NOT NULL
);

INSERT INTO analytics.orders
SELECT g,
       (g % 5000) + 1,
       (ARRAY['emea','amer','apac'])[1 + (g % 3)],
       (ARRAY['web','mobile','partner'])[1 + (g % 3)],
       -- A realistic long tail of refunds and failures, not a clean table.
       CASE WHEN g % 97 = 0 THEN 'refunded'
            WHEN g % 89 = 0 THEN 'failed'
            ELSE 'paid' END,
       ((g * 37) % 50000) / 100.0 + 1,
       (now() AT TIME ZONE 'UTC') - ((g % 90) || ' days')::interval
FROM generate_series(1, 200000) g;

CREATE INDEX ON analytics.orders (placed_at);
CREATE INDEX ON analytics.orders (region);

-- ---------------------------------------------------------- published views

-- Destinations the examples write to. Created empty: a job that cannot find
-- its destination should fail loudly rather than invent one.
DROP TABLE IF EXISTS reporting.revenue_by_region;
CREATE TABLE reporting.revenue_by_region (
    region  text PRIMARY KEY,
    orders  bigint,
    revenue numeric(18,2)
);

DROP TABLE IF EXISTS reporting.orders_replica;
CREATE TABLE reporting.orders_replica (
    order_id    bigint PRIMARY KEY,
    customer_id bigint,
    region      text,
    amount      numeric(12,2)
);
