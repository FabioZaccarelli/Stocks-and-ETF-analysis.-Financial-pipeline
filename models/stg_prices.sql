with src as (   
    select * 
    from {{ source('raw', 'raw_prices') }}
)

select date
     ,open
     ,high
     ,low
     ,close
     ,volume
     ,ticker
from src