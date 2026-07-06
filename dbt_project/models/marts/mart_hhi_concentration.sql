select
    hs_code,
    trade_month,
    round(sum(power(market_share * 100, 2))::numeric, 1) as hhi,
    count(*) as supplier_countries,
    max(market_share) as top_supplier_share,
    case
        when sum(power(market_share * 100, 2)) >= 2500 then 'highly concentrated'
        when sum(power(market_share * 100, 2)) >= 1500 then 'moderately concentrated'
        else 'competitive'
    end as concentration_band
from {{ ref('int_monthly_country_share') }}
group by hs_code, trade_month