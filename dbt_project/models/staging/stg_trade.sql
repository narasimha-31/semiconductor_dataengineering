select
    cty_code as country_code,
    cty_name as country_name,
    hs_code,
    trade_value_usd,
    trade_month
from {{ source('silver', 'trade_cleaned') }}