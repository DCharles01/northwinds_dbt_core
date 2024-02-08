{{ config(materialized='table') }}

with source as (
select
	int_cont.contact_pk
	, int_cont.first_name
	, int_cont.last_name
	, int_cont.phone
	, int_comp.company_pk
from
	{{ ref('int_contacts') }} int_cont
inner join
	{{ ref('int_companies') }} int_comp on int_cont.hubspot_company_id=int_comp.hubspot_company_id or int_cont.rds_company_id=int_comp.rds_company_id
order by
	int_cont.last_name)

select * from source