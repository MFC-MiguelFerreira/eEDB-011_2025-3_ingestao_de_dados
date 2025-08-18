{{
    config(
        materialized='external',
        location="data/raw/glassdoor/glassdoor.parquet",
    )
}}

select *
from {{ source('source_data', 'glassdoor') }}