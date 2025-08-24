{{
    config(
        materialized='external',
        location="data/delivery/delivery.parquet",
    )
}}

    select distinct 
     coalesce(g.name, b.name, r.name) as name
     , coalesce(g.cnpj, b.cnpj, r.cnpj) as cnpj
     , coalesce(g.segment, b.segment) as segment
     , g.employer_website
     , g.employer_headquarters
     , g.employer_founded
     , g.employer_industry
     , g.employer_revenue
     , g.general_score
     , g.culture_score
     , g.diversity_score
     , g.quality_of_life_score
     , g.leadership_score
     , g.compensation_score
     , g.career_opportunities_score
     , g.recommendation_percentage
     , g.positive_outlook_percentage
     , g.match_percentage
     , r.category
     , r.type
     , r.quarter
     , r.year
     , r.complaint_index
     , r.regulated_complaints_upheld
     , r.regulated_complaints_other
     , r.unregulated_complaints
     , r.total_complaints
     , r.total_clients_ccs_scr
     , r.clients_ccs
     , r.clients_scr
from 
    {{ source('trusted', 'glassdoor') }} g 
    inner join {{ source('trusted', 'bancos') }} b on g.name = b.name or g.cnpj = b.cnpj
    inner join {{ source('trusted', 'reclamacoes') }} r on r.name = g.name or r.cnpj = g.cnpj or r.name = b.name or r.cnpj = b.cnpj