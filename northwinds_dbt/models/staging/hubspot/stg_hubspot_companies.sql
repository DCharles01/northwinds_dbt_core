with source as (
    select * from {{ source('hubspot', 'northwinds_hubspot') }}
)

select
    max({{ derive_company_id('business_name', 'hubspot-') }}) as company_id
    , business_name as name
from
    source
group by
    business_name