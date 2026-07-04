CREATE TABLE IF NOT EXISTS bronze.filings_raw (
    ticker          TEXT NOT NULL,
    cik             TEXT NOT NULL,
    taxonomy        TEXT NOT NULL,
    metric          TEXT NOT NULL,
    tag             TEXT NOT NULL,
    end_date        TEXT NOT NULL,
    value           NUMERIC NOT NULL,
    form            TEXT NOT NULL,
    fiscal_year     INT,
    fiscal_period   TEXT,
    filed           TEXT,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    batch_id        TEXT,
    CONSTRAINT uq_filings_raw UNIQUE (ticker, metric, end_date)
);