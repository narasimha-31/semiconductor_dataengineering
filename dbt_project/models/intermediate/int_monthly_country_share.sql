with monthly_totals as (
    select
        hs_code,
        trade_month,
        sum(trade_value_usd) as month_total_usd
    from {{ ref('stg_trade') }}
    group by hs_code, trade_month
)

select
    t.hs_code,
    t.trade_month,
    t.country_code,
    t.country_name,
    t.trade_value_usd,
    m.month_total_usd,
    t.trade_value_usd::numeric / nullif(m.month_total_usd, 0) as market_share
from {{ ref('stg_trade') }} t
join monthly_totals m
  on m.hs_code = t.hs_code
 and m.trade_month = t.trade_month