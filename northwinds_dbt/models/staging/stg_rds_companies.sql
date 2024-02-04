with source as (
    select * from {{ source('rds', 'customers')}}
),
renamed as (
    select
        concat('rds-', replace(lower(company_name), ' ', '-')) as company_id
        , company_name
        , max(address) as address
        , max(city) as city
        , max(postal_code) as postal_code
        , max(country) as country
    from
        source
    group by
        company_name
)

select * from renamed