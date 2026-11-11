select
    rate_date,
    base_currency,
    idr_rate,
    idr_rate - lag(idr_rate) over w as change_abs,
    round(100.0 * (idr_rate / lag(idr_rate) over w - 1), 3) as change_pct
from {{ ref('stg_exchange_rates') }}
window w as (partition by base_currency order by rate_date)
