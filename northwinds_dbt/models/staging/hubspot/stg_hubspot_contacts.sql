with source as (select
    *
from
    {{ source('hubspot', 'northwinds_hubspot') }}

),

cleaned_format_phone_number as (
    select
        'hubspot-' || hubspot_id as contact_id
        , first_name
        , last_name
        , {{ format_phone_number('phone') }} as phone
        , business_name
    from
        source
),

join_hubspot_companies as (

select
	hubspot_contacts.contact_id
	, hubspot_contacts.first_name
	, hubspot_contacts.last_name
	, hubspot_contacts.phone
	, hubspot_companies.company_id
from
	cleaned_format_phone_number as hubspot_contacts
inner join
	{{ ref('stg_hubspot_companies') }} hubspot_companies on hubspot_contacts.business_name=hubspot_companies.business_name

),



final as (
    select * from join_hubspot_companies
)

select * from final
