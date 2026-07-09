with ranked as (
    select
        hs_code,
        trade_month,
        country_code,
        country_name,
        market_share,
        row_number() over (
            partition by hs_code, trade_month
            order by market_share desc
        ) as share_rank
    from {{ ref('int_monthly_country_share') }}
)

select
    s.hs_code,
    s.trade_month,
    round(sum(power(s.market_share * 100, 2))::numeric, 1) as hhi,
    count(*) as supplier_countries,
    max(s.market_share) as top_supplier_share,
    max(r.country_name) filter (where r.share_rank = 1) as top_supplier_country,
    case
        when sum(power(s.market_share * 100, 2)) >= 2500 then 'highly concentrated'
        when sum(power(s.market_share * 100, 2)) >= 1500 then 'moderately concentrated'
        else 'competitive'
    end as concentration_band
from {{ ref('int_monthly_country_share') }} s
join ranked r
  on s.hs_code = r.hs_code
 and s.trade_month = r.trade_month
 and s.country_code = r.country_code
group by s.hs_code, s.trade_month