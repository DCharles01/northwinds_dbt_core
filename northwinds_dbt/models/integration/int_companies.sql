with source as (SELECT
	
    
    hubspot_companies.company_id as hubspot_company_id
    , rds_companies.company_id as rds_company_id
    , rds_companies.company_name as name
    , rds_companies.address as addres
    , rds_companies.city as city
    , rds_companies.postal_code as postal_code
    , rds_companies.country as country
FROM
	dev.stg_hubspot_companies hubspot_companies
inner join
	dev.stg_rds_companies rds_companies on hubspot_companies.business_name=rds_companies.company_name)


select
    {{ dbt_utils.generate_surrogate_key(['name']) }} as company_pk
    , *
from
    source