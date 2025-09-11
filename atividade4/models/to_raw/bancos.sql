{{
    config(
        materialized='external',
        location="data/raw/bancos/bancos.parquet",
    )
}}

select *
from {{ source('source_data', 'bancos') }}