select
    document_number,
    title,
    doc_type,
    abstract,
    publication_date,
    html_url,
    agencies
from {{ source('silver', 'regulatory_cleaned') }}