CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.trade_raw (
    cty_code        TEXT NOT NULL,
    cty_name        TEXT NOT NULL,
    hs_code         TEXT NOT NULL,
    trade_value_usd TEXT NOT NULL,
    month           TEXT NOT NULL,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    batch_id        TEXT,
    CONSTRAINT uq_trade_raw UNIQUE (cty_code, hs_code, month)
);