with pivoted as (
    select
        ticker,
        period_end,
        max(value_usd) filter (where metric = 'revenue'
            and period_type = 'Q') as revenue_usd,
        max(value_usd) filter (where metric = 'revenue'
            and period_type = 'FY') as revenue_fy_usd,
        max(value_usd) filter (where metric = 'capex'
            and period_type = 'Q') as capex_usd,
        max(value_usd) filter (where metric = 'inventory') as inventory_usd
    from {{ ref('stg_filings') }}
    group by ticker, period_end
)

select
    ticker,
    period_end,
    revenue_usd,
    revenue_fy_usd,
    capex_usd,
    inventory_usd,
    lag(revenue_usd, 4) over (
        partition by ticker order by period_end
    ) as revenue_4_periods_ago,
    round(100.0 * (revenue_usd - lag(revenue_usd, 4) over (
        partition by ticker order by period_end))
        / nullif(lag(revenue_usd, 4) over (
            partition by ticker order by period_end), 0), 1
    ) as revenue_yoy_pct,
    round(100.0 * (inventory_usd - lag(inventory_usd, 4) over (
        partition by ticker order by period_end))
        / nullif(lag(inventory_usd, 4) over (
            partition by ticker order by period_end), 0), 1
    ) as inventory_yoy_pct
from pivoted