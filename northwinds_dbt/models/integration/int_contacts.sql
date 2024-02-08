with merged_contacts as
(select
	null as hubspot_contact_id
	, customer_id as rds_contact_id
   	, first_name
    , last_name
    , phone
    , company_id
from
	{{ ref('stg_rds_customers') }}
union all
select
	contact_id as hubspot_contact_id
	, null as rds_contact_id
   	, first_name
    , last_name
    , phone
    , company_id
from
	{{ ref('stg_hubspot_contacts') }}
),


merged_company as 

(
select
	null as hubspot_contact_id
	, customer_id as rds_contact_id
   	, first_name
    , last_name
    , phone
    , null as hubspot_company_id
    , company_id as rds_company_id
from
	{{ ref('stg_rds_customers') }}
union all
select
	contact_id as hubspot_contact_id
	, null as rds_contact_id
   	, first_name
    , last_name
    , phone
    , company_id as hubspot_company_id
    , null as rds_company_id
from
	{{ ref('stg_hubspot_contacts') }}

), 

deduped as (
select
	max(hubspot_contact_id) as hubspot_contact_id
	, max(rds_contact_id) as rds_contact_id
	, first_name
	, last_name
	, max(phone) as phone
	, max(hubspot_company_id) as hubspot_company_id
	, max(rds_company_id) as rds_company_id
from
	merged_company
group by
	first_name
	, last_name
order by
	last_name

)
select 
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'phone']) }} as contact_pk
    , * 
from
    deduped