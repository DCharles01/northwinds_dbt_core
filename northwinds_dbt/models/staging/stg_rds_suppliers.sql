with source as (
    select
        *
    from
        {{ source('postgres', 'suppliers')}}
),
clean_phone as (
    select
        supplier_id
        , company_name
        , split_part(contact_title, ' ', 1) as contact_first_name
        , split_part(contact_title, ' ', 2) as contact_last_name
        , contact_title
        , address
        , city
        , region
        , postal_code
        , country
        , {{ clean_phone_number('phone') }} as phone
        , fax
        , homepage
    from
        source
    
),

format_phone as (
    select
        supplier_id
        , company_name
        , split_part(contact_title, ' ', 1) as contact_first_name
        , split_part(contact_title, ' ', 2) as contact_last_name
        , contact_title
        , address
        , city
        , region
        , postal_code
        , country
        , {{ format_phone_number('phone') }} as phone
        , fax
        , homepage
    from
        clean_phone
)

select * from format_phone