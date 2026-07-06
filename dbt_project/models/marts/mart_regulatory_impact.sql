with rules as (
    select
        document_number,
        title,
        publication_date,
        date_trunc('month', publication_date)::date as pub_month
    from {{ ref('stg_regulatory') }}
    where doc_type = 'Rule'
),

monthly as (
    select
        hs_code,
        trade_month,
        sum(trade_value_usd) as total_usd
    from {{ ref('stg_trade') }}
    group by hs_code, trade_month
)

select
    r.document_number,
    r.title,
    r.publication_date,
    m.hs_code,
    avg(m.total_usd) filter (
        where m.trade_month >= r.pub_month - interval '3 months'
          and m.trade_month <  r.pub_month
    ) as avg_3mo_before,
    avg(m.total_usd) filter (
        where m.trade_month >  r.pub_month
          and m.trade_month <= r.pub_month + interval '3 months'
    ) as avg_3mo_after,
    round(100.0 * (
        avg(m.total_usd) filter (
            where m.trade_month >  r.pub_month
              and m.trade_month <= r.pub_month + interval '3 months')
        -
        avg(m.total_usd) filter (
            where m.trade_month >= r.pub_month - interval '3 months'
              and m.trade_month <  r.pub_month)
        ) / nullif(avg(m.total_usd) filter (
            where m.trade_month >= r.pub_month - interval '3 months'
              and m.trade_month <  r.pub_month), 0), 1
    ) as pct_change_3mo
from rules r
join monthly m on true
group by r.document_number, r.title, r.publication_date, r.pub_month, m.hs_code