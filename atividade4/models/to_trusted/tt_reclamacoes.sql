{{
    config(
        materialized='external',
        location="data/trusted/reclamacoes/reclamacoes.parquet",
    )
}}

select
      trim(cast("CNPJ IF" as varchar)) as "cnpj"
    , trim(
        regexp_replace(
            trim(lower("Instituição Financeira")), 
            '\s*\(conglomerado\)$|\s*s\.a\.?|\s*s\/a|ltda\.?$',
            ''
        )
    ) as "name"
    , trim(lower("Categoria")) as "category"
    , trim(lower("Tipo")) as "type"
    , trim(
        regexp_replace(
            trim(lower("Trimestre")), 
            'º',
            ''
        )
    ) as "quarter"
    , "Ano" as "year"
    , "Índice" as "complaint_index"
    , "Quantidade de reclamações reguladas procedentes" as "regulated_complaints_upheld"
    , "Quantidade de reclamações reguladas - outras" as "regulated_complaints_other"
    , "Quantidade de reclamações não reguladas" as "unregulated_complaints"
    , "Quantidade total de reclamações" as "total_complaints"
    , "Quantidade total de clientes – CCS e SCR" as "total_clients_ccs_scr"
    , "Quantidade de clientes – CCS" as "clients_ccs"
    , "Quantidade de clientes – SCR" as "clients_scr"
from 
    {{ source('raw', 'reclamacoes') }}