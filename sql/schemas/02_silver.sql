CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.trade_cleaned (
    trade_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cty_code        VARCHAR(4) NOT NULL,
    cty_name        TEXT NOT NULL,
    hs_code         VARCHAR(6) NOT NULL,
    trade_value_usd BIGINT NOT NULL CHECK (trade_value_usd >= 0),
    trade_month     DATE NOT NULL,
    source_batch_id TEXT,
    transformed_at  TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_trade_cleaned UNIQUE (cty_code, hs_code, trade_month)
);

CREATE TABLE IF NOT EXISTS silver.dead_letter_queue (
    dlq_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_table    TEXT NOT NULL,
    original_record JSONB NOT NULL,
    error_reason    TEXT NOT NULL,
    failed_at       TIMESTAMPTZ DEFAULT NOW()
);