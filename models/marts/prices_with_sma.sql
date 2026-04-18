with base as (

    select *
    from {{ ref('prices_daily') }}

),

with_sma as (

    select
        date,
        ticker,
        open,
        high,
        low,
        close,
        volume,

        -- SMA 20 giorni
        avg(close) over (
            partition by ticker
            order by date
            rows between 19 preceding and current row
        ) as sma_20

    from base
)

select *
from with_sma
order by ticker, date