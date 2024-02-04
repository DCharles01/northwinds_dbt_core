with shippers_model as (select 
    shipper_id
    , company_name
from
    {{ source('postgres', 'shippers') }})

select * from shippers_model