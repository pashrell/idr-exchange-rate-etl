{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['rate_date', 'base_currency'],
        on_schema_change='sync_all_columns'
    )
}}

{# Incremental: pada run biasa hanya memproses data dari 21 hari terakhir
   (lookback) agar window function lag/avg punya konteks, lalu hanya baris
   7 hari terakhir yang ditulis ulang. Kurs yang direvisi API tetap tertangkap.
   Gunakan `dbt run --full-refresh` untuk membangun ulang dari awal. #}

with src as (

    select rate_date, base_currency, idr_rate
    from {{ ref('stg_exchange_rates') }}
    {% if is_incremental() %}
    where rate_date >= (select max(rate_date) from {{ this }}) - 21
    {% endif %}

),

calc as (

    select
        rate_date,
        base_currency,
        idr_rate,
        idr_rate - lag(idr_rate) over w as change_abs,
        round(100.0 * (idr_rate / lag(idr_rate) over w - 1), 3) as change_pct,
        round(avg(idr_rate) over (
            partition by base_currency
            order by rate_date
            rows between 6 preceding and current row
        ), 2) as moving_avg_7d
    from src
    window w as (partition by base_currency order by rate_date)

)

select * from calc
{% if is_incremental() %}
where rate_date >= (select max(rate_date) from {{ this }}) - 7
{% endif %}
