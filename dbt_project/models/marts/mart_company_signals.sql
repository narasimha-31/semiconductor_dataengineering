with pivoted as (
    select
        ticker,
        period_end,
        max(value_usd) filter (where metric = 'revenue'
            and period_type = 'Q') as revenue_q_reported,
        max(value_usd) filter (where metric = 'revenue'
            and period_type = 'FY') as revenue_fy_usd,
        max(value_usd) filter (where metric = 'capex'
            and period_type = 'Q') as capex_usd,
        max(value_usd) filter (where metric = 'inventory') as inventory_usd
    from {{ ref('stg_filings') }}
    group by ticker, period_end
),

q4_derived as (
    select
        fy.ticker,
        fy.period_end,
        fy.revenue_fy_usd - sum(q.revenue_q_reported) as q4_revenue
    from pivoted fy
    join pivoted q
      on q.ticker = fy.ticker
     and q.period_end > fy.period_end - interval '360 days'
     and q.period_end < fy.period_end
     and q.revenue_q_reported is not null
    where fy.revenue_fy_usd is not null
    group by fy.ticker, fy.period_end, fy.revenue_fy_usd
    having count(*) = 3
),

completed as (
    select
        p.ticker,
        p.period_end,
        coalesce(p.revenue_q_reported, d.q4_revenue) as revenue_usd,
        (p.revenue_q_reported is null
            and d.q4_revenue is not null) as revenue_is_derived,
        p.revenue_fy_usd,
        p.capex_usd,
        p.inventory_usd
    from pivoted p
    left join q4_derived d
      on d.ticker = p.ticker
     and d.period_end = p.period_end
)

select
    ticker,
    period_end,
    revenue_usd,
    revenue_is_derived,
    revenue_fy_usd,
    capex_usd,
    inventory_usd,
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
from completed