with prices as (
    select  date,
            ticker,
            open,
            high,
            low,
            close,
            volume
    from {{ ref('stg_prices') }}
),

latest_date_per_ticker as (
    select ticker
          ,max(date) as latest_date 
    from prices
    group by ticker
),

final as (
    select  latest_date_per_ticker.ticker, 
            latest_date_per_ticker.latest_date as date,
            prices.open,
            prices.high,
            prices.low,
            prices.close,
            prices.volume
    from prices
    inner join latest_date_per_ticker 
        on latest_date_per_ticker.ticker = prices.ticker
        and latest_date_per_ticker.latest_date =prices.date
)

select *
from final