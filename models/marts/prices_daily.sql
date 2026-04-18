with base as (
    select date,
           ticker,
           open,
           high,
           low,
           close,
           volume 
    from {{ ref('stg_prices') }}
),

final as (
    select date,
           ticker,
           open,
           high,
           low,
           close,
           volume
    from base
    where date is not null
    and ticker is not null
)

select *
from final
