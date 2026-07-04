INSERT INTO silver.filings_cleaned
    (ticker, cik, taxonomy, metric, tag, period_end, value_usd, form,
     fiscal_year, fiscal_period, filed_date, source_batch_id)
SELECT
    ticker, cik, taxonomy, metric, tag,
    TO_DATE(end_date, 'YYYY-MM-DD'),
    value,
    form,
    fiscal_year,
    fiscal_period,
    CASE WHEN filed ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
         THEN TO_DATE(filed, 'YYYY-MM-DD') END,
    batch_id
FROM bronze.filings_raw
WHERE end_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
ON CONFLICT ON CONSTRAINT uq_filings_cleaned DO NOTHING;

INSERT INTO silver.dead_letter_queue (source_table, original_record, error_reason)
SELECT
    'bronze.filings_raw',
    JSONB_BUILD_OBJECT('ticker', ticker, 'metric', metric,
                       'end_date', end_date, 'value', value),
    'end_date failed date validation'
FROM bronze.filings_raw
WHERE end_date !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';