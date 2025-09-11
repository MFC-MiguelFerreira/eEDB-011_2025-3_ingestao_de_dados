{{ 
    config(
        materialized='table'
    )
}}

select *
from {{ source('delivery', 'delivery') }}
