select 
    contact_name
    , address
    , phone 
from 
    {{ source('postgres', 'customers') }}