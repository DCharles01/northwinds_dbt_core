with source as (
    select * from {{ source('hubspot', 'northwinds_hubspot') }}
)

select
    max({{ append_prefix_source('business_name', 'hubspot-') }}) as company_id
    , business_name as name
from
    source
group by
    business_name