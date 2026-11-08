CREATE TABLE scores (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL,
    us_state TEXT NOT NULL,
    merch TEXT NOT NULL,
    cat_id TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON scores (created_at);
CREATE INDEX ON scores (us_state, merch, created_at);
CREATE USER grafana WITH PASSWORD 'grafana';
GRANT CONNECT ON DATABASE fraud TO grafana;
GRANT USAGE ON SCHEMA public TO grafana;
GRANT SELECT ON scores TO grafana;
