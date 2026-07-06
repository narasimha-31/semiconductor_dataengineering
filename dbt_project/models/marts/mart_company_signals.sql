with pivoted as (
    select
        ticker,
        period_end,
        max(value_usd) filter (where metric = 'revenue')   as revenue_usd,
        max(value_usd) filter (where metric = 'capex')     as capex_usd,
        max(value_usd) filter (where metric = 'inventory') as inventory_usd
    from {{ ref('stg_filings') }}
    group by ticker, period_end
)

select
    ticker,
    period_end,
    revenue_usd,
    capex_usd,
    inventory_usd,
    lag(revenue_usd, 4) over (partition by ticker order by period_end) as revenue_4_periods_ago,
    round(100.0 * (revenue_usd - lag(revenue_usd, 4) over (partition by ticker order by period_end))
        / nullif(lag(revenue_usd, 4) over (partition by ticker order by period_end), 0), 1
    ) as revenue_yoy_pct
from pivoted