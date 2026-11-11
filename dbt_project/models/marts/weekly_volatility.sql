select
    date_trunc('week', rate_date)::date as week_start,
    base_currency,
    round(avg(idr_rate), 2)    as avg_rate,
    min(idr_rate)              as min_rate,
    max(idr_rate)              as max_rate,
    round(stddev(idr_rate), 2) as volatility
from {{ ref('fct_exchange_rates') }}
group by 1, 2
