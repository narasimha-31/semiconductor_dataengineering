select
    ticker,
    metric,
    tag,
    period_end,
    value_usd,
    form,
    fiscal_year,
    fiscal_period,
    filed_date
from {{ source('silver', 'filings_cleaned') }}