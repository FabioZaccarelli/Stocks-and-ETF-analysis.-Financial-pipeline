with base as (

    select *
    from {{ ref('prices_daily') }}

),

returns as (

    select
        date,
        ticker,
        close,
        lag(close) over (
            partition by ticker
            order by date
        ) as prev_close
    from base

),

gains_losses as (

    select
        date,
        ticker,
        close,
        case when close > prev_close then close - prev_close else 0 end as gain,
        case when close < prev_close then prev_close - close else 0 end as loss
    from returns

),

rsi_calc as (

    select
        date,
        ticker,
        close,
        gain,
        loss,

        avg(gain) over (
            partition by ticker
            order by date
            rows between 13 preceding and current row
        ) as avg_gain_14,

        avg(loss) over (
            partition by ticker
            order by date
            rows between 13 preceding and current row
        ) as avg_loss_14

    from gains_losses

),

final as (

    select
        b.date,
        b.ticker,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,

        -- SMA 20
        avg(b.close) over (
            partition by b.ticker
            order by b.date
            rows between 19 preceding and current row
        ) as sma_20,

        -- EMA 20 (approssimata con smoothing factor)
        (
            avg(b.close) over (
                partition by b.ticker
                order by b.date
                rows between 19 preceding and current row
            ) * (2.0 / 21)
        ) as ema_20,

        -- RSI 14
        case
            when r.avg_loss_14 = 0 then null
            else 100 - (100 / (1 + (r.avg_gain_14 / r.avg_loss_14)))
        end as rsi_14

    from base b
    left join rsi_calc r
        on b.date = r.date
       and b.ticker = r.ticker
)

select *
from final
order by ticker, date
