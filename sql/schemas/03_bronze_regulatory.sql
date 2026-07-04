CREATE TABLE IF NOT EXISTS bronze.regulatory_raw (
    document_number  TEXT NOT NULL,
    title            TEXT NOT NULL,
    doc_type         TEXT NOT NULL,
    abstract         TEXT,
    publication_date TEXT NOT NULL,
    html_url         TEXT,
    agencies         JSONB,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW(),
    batch_id         TEXT,
    CONSTRAINT uq_regulatory_raw UNIQUE (document_number)
);