select
    rate_date,
    base_currency,
    rate::numeric as idr_rate,
    loaded_at
from {{ source('raw', 'exchange_rates') }}
where rate > 0
