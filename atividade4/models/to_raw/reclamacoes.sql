{{
    config(
        materialized='external',
        location="data/raw/reclamacoes/reclamacoes.parquet",
    )
}}

select *
from {{ source('source_data', 'reclamacoes') }}