CREATE TABLE IF NOT EXISTS silver.regulatory_cleaned (
    reg_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_number  TEXT NOT NULL,
    title            TEXT NOT NULL,
    doc_type         TEXT NOT NULL,
    abstract         TEXT,
    publication_date DATE NOT NULL,
    html_url         TEXT,
    agencies         JSONB,
    source_batch_id  TEXT,
    transformed_at   TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_regulatory_cleaned UNIQUE (document_number)
);

CREATE TABLE IF NOT EXISTS silver.filings_cleaned (
    filing_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker          TEXT NOT NULL,
    cik             TEXT NOT NULL,
    taxonomy        TEXT NOT NULL,
    metric          TEXT NOT NULL,
    tag             TEXT NOT NULL,
    period_end      DATE NOT NULL,
    value_usd       NUMERIC NOT NULL,
    form            TEXT NOT NULL,
    fiscal_year     INT,
    fiscal_period   TEXT,
    filed_date      DATE,
    source_batch_id TEXT,
    transformed_at  TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_filings_cleaned UNIQUE (ticker, metric, period_end)
);