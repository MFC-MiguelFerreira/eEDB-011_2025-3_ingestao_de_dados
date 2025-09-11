{{
    config(
        materialized='external',
        location="data/trusted/bancos/bancos.parquet",
    )
}}

select
      trim(cast("CNPJ" as varchar)) as "cnpj"
    , trim(
        regexp_replace(
            trim(lower("Nome")), 
            '\s*-\s*prudencial$|s\.a\.?\s*-\s*prudencial$|s\.a\.?$|s\/a$|ltda\.?$',
            ''
        )
    ) as "name"
    , "Segmento" as "segment"
from 
    {{ source('raw', 'bancos') }}