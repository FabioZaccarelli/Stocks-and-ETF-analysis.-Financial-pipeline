{{ config(
    materialized = 'incremental',
    unique_key = ['ticker', 'date'],
    incremental_strategy = 'merge'
) }}

with source_data as (

    select
        date,
        ticker,
        open,
        high,
        low,
        close,
        volume
    from {{ ref('stg_prices') }}

    {% if is_incremental() %}
        -- Carica solo le date nuove
        where date > (select max(date) from {{ this }})
    {% endif %}

),

final as (

    select
        date,
        ticker,
        open,
        high,
        low,
        close,
        volume
    from source_data
)

select *
from final
