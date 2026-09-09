-- Sample data for examples/agent_sql.py.
--
--   psql "$GANTRY_DATABASE_URL" -f examples/seed.sql
--
-- analytics.customer_pii exists so the denied-table refusal has something real
-- to refuse. Nothing in the example ever reads it.

CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.payments;
CREATE TABLE analytics.payments (
    id          bigint PRIMARY KEY,
    customer_id bigint,
    amount      numeric(12,2),
    created_at  timestamptz DEFAULT now()
);

INSERT INTO analytics.payments
SELECT g,
       (g % 50) + 1,
       (g * 7) % 1000 + 0.5,
       now() - (g || ' hours')::interval
FROM generate_series(1, 500) g;

DROP TABLE IF EXISTS analytics.customer_pii;
CREATE TABLE analytics.customer_pii (
    customer_id bigint PRIMARY KEY,
    email       text
);
