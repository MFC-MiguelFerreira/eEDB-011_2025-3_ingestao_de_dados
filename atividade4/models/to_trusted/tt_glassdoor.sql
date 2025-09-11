{{
    config(
        materialized='external',
        location="data/trusted/glassdoor/glassdoor.parquet",
    )
}}

with base_glassdoor as (
    select 
        trim(cast("CNPJ" as varchar)) as cnpj
        , trim(
            regexp_replace(
                trim(lower("Nome")), 
                '\s*-\s*prudencial$|s\.a\.?\s*-\s*prudencial$|s\.a\.?$|s\/a$|ltda\.?$',
                ''
            )
        ) as name
        , trim("Segmento") as segment
        , "employer-website" as employer_website
        , "employer-headquarters" as employer_headquarters
        , trim(
            cast(
                cast("employer-founded" as integer) as varchar
            )
        ) as employer_founded
        , "employer-industry" as employer_industry
        , "employer-revenue" as employer_revenue
        , "reviews_count" as reviews_count
        , "culture_count" as culture_count
        , "salaries_count" as salaries_count
        , "benefits_count" as benefits_count
        , "Geral" as general_score
        , "Cultura e valores" as culture_score
        , "Diversidade e inclusão" as diversity_score
        , "Qualidade de vida" as quality_of_life_score
        , "Alta liderança" as leadership_score
        , "Remuneração e benefícios" as compensation_score
        , "Oportunidades de carreira" as career_opportunities_score
        , "Recomendam para outras pessoas(%)" as recommendation_percentage
        , "Perspectiva positiva da empresa(%)" as positive_outlook_percentage
        , "match_percent" as match_percentage
    from 
        {{ source('raw', 'glassdoor') }}
)

select 
      last(cnpj) as cnpj
    , name as name
    , last(segment) as segment
    , last(employer_website) as employer_website
    , last(employer_headquarters) as employer_headquarters
    , last(employer_founded) as employer_founded
    , last(employer_industry) as employer_industry
    , last(employer_revenue) as employer_revenue
    , cast(sum(reviews_count) as bigint) as reviews_count
    , cast(sum(culture_count) as bigint) as culture_count
    , cast(sum(salaries_count) as bigint) as salaries_count
    , cast(sum(benefits_count) as bigint) as benefits_count
    , avg(general_score) as general_score
    , avg(culture_score) as culture_score
    , avg(diversity_score) as diversity_score
    , avg(quality_of_life_score) as quality_of_life_score
    , avg(leadership_score) as leadership_score
    , avg(compensation_score) as compensation_score
    , avg(career_opportunities_score) as career_opportunities_score
    , avg(recommendation_percentage) as recommendation_percentage
    , avg(positive_outlook_percentage) as positive_outlook_percentage
    , avg(match_percentage) as match_percentage
from 
    base_glassdoor
group by 
    name
