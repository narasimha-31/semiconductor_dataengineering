INSERT INTO silver.regulatory_cleaned
    (document_number, title, doc_type, abstract, publication_date,
     html_url, agencies, source_batch_id)
SELECT
    document_number,
    TRIM(title),
    doc_type,
    abstract,
    TO_DATE(publication_date, 'YYYY-MM-DD'),
    html_url,
    agencies,
    batch_id
FROM bronze.regulatory_raw
WHERE publication_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
ON CONFLICT ON CONSTRAINT uq_regulatory_cleaned DO NOTHING;

INSERT INTO silver.dead_letter_queue (source_table, original_record, error_reason)
SELECT
    'bronze.regulatory_raw',
    JSONB_BUILD_OBJECT('document_number', document_number,
                       'publication_date', publication_date,
                       'title', title),
    'publication_date failed date validation'
FROM bronze.regulatory_raw
WHERE publication_date !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';