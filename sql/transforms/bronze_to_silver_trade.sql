CREATE TABLE IF NOT EXISTS silver.reconciliation_totals (
    hs_code      VARCHAR(6) NOT NULL,
    trade_month  DATE NOT NULL,
    api_total    BIGINT NOT NULL,
    loaded_at    TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_recon UNIQUE (hs_code, trade_month)
);

INSERT INTO silver.reconciliation_totals (hs_code, trade_month, api_total)
SELECT
    hs_code,
    TO_DATE(month || '-01', 'YYYY-MM-DD'),
    trade_value_usd::BIGINT
FROM bronze.trade_raw
WHERE cty_code = '-'
  AND trade_value_usd ~ '^[0-9]+$'
ON CONFLICT ON CONSTRAINT uq_recon DO NOTHING;

INSERT INTO silver.trade_cleaned
    (cty_code, cty_name, hs_code, trade_value_usd, trade_month, source_batch_id)
SELECT
    cty_code,
    INITCAP(TRIM(cty_name)),
    hs_code,
    trade_value_usd::BIGINT,
    TO_DATE(month || '-01', 'YYYY-MM-DD'),
    batch_id
FROM bronze.trade_raw
WHERE cty_code ~ '^[1-9][0-9]{3}$'
  AND trade_value_usd ~ '^[0-9]+$'
ON CONFLICT ON CONSTRAINT uq_trade_cleaned DO NOTHING;

INSERT INTO silver.dead_letter_queue (source_table, original_record, error_reason)
SELECT
    'bronze.trade_raw',
    JSONB_BUILD_OBJECT(
        'cty_code', cty_code,
        'cty_name', cty_name,
        'hs_code', hs_code,
        'trade_value_usd', trade_value_usd,
        'month', month
    ),
    'trade_value_usd failed numeric validation'
FROM bronze.trade_raw
WHERE cty_code ~ '^[1-9][0-9]{3}$'
  AND trade_value_usd !~ '^[0-9]+$';