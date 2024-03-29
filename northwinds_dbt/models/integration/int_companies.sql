-- with source as (SELECT
	
    
--     hubspot_companies.company_id as hubspot_company_id
--     , rds_companies.company_id as rds_company_id
--     , rds_companies.company_name as name
--     , rds_companies.address as address
--     , rds_companies.city as city
--     , rds_companies.postal_code as postal_code
--     , rds_companies.country as country
-- FROM
-- 	{{ ref('stg_hubspot_companies') }} hubspot_companies
-- inner join
--     {{ ref('stg_rds_companies') }} rds_companies on hubspot_companies.business_name=rds_companies.company_name)


-- select
--     {{ dbt_utils.generate_surrogate_key(['name']) }} as company_pk
--     , *
-- from
--     source



{% set sources = ["stg_hubspot_companies", "stg_rds_companies"] %}

  with merged_companies as (
    {% for source in sources %}
        select name, {{ 'company_id' if 'hubspot' in source else 'null' }} as hubspot_company_id,
        {{ 'company_id' if 'rds' in source else 'null' }} as rds_company_id
         from {{ ref(source) }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
  ), 
    deduped as (
    select max(hubspot_company_id) as hubspot_company_id, 
    max(rds_company_id) as rds_company_id, name from merged_companies group by name
  )
  select {{ dbt_utils.generate_surrogate_key(['deduped.name']) }} as company_pk, hubspot_company_id, rds_company_id, deduped.name, address,
   postal_code, city, country from deduped
  join {{ ref('stg_rds_companies') }} rds_companies on rds_companies.company_id = deduped.rds_company_id

